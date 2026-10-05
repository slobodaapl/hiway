use hiway::transport::{
    self, Connection, Contract, Direction, Effect, Error, Frames, Input, IoResult, Lane, OpId,
    Operation, Phase, Protocol, Record, Records, Reservation, StrictReservation, CREDIT_BYTES,
    HEADER_BYTES,
};
use hiway::{DecodeContext, EventReceiver, EventSender, TopicError, WireCodec};
use io_uring::{opcode, types, IoUring};
use std::{
    cell::{Cell, RefCell, RefMut, UnsafeCell},
    future::Future,
    io,
    mem::ManuallyDrop,
    os::fd::{AsRawFd, BorrowedFd, OwnedFd},
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

const CANCEL: u32 = 1 << 31;

trait Admission<G: Reservation> {
    const DEFERRED: bool;
    fn with_access<T>(reservation: &G, submit: impl FnOnce() -> T) -> Result<T, TopicError>;
}

struct Immediate;
impl<G: Reservation> Admission<G> for Immediate {
    const DEFERRED: bool = false;
    fn with_access<T>(reservation: &G, submit: impl FnOnce() -> T) -> Result<T, TopicError> {
        let _access = reservation.enter()?;
        Ok(submit())
    }
}

struct Deferred;
impl<G: StrictReservation> Admission<G> for Deferred {
    const DEFERRED: bool = true;
    fn with_access<T>(reservation: &G, submit: impl FnOnce() -> T) -> Result<T, TopicError> {
        let _access = reservation.enter_deferred()?;
        Ok(submit())
    }
}

/// Per-pass limits. Each field must be nonzero. Byte limits split I/O requests;
/// they do not limit bytes completing from requests accepted in earlier passes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Budget {
    /// Maximum CQEs consumed, including cancellation completions.
    pub completions: usize,
    /// Maximum new SQEs accepted, including cancellation requests.
    pub submissions: usize,
    /// Maximum sum of requested send/receive lengths, including control traffic.
    /// Cancellation consumes no bytes.
    pub bytes: usize,
}

/// I/O scheduling advice. Dispatch pending work and poll ready link futures
/// separately; either can change this snapshot. Use [`Driver::schedule`] to
/// refresh it before parking.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Schedule {
    /// More local I/O work is available. Advance with a replenished budget.
    Continue,
    /// Only admission contention remains runnable. Arrange a host retry (for
    /// example on a later event-loop turn or timer); no link wake is generated.
    Retry,
    /// Await ring, endpoint or authority readiness through the host. This does
    /// not imply that the ring has an outstanding operation to wait for.
    Wait,
}

enum Attempt {
    Submitted(usize),
    Blocked,
    Idle,
}

/// Pending owner work for one slot. These records contain no callbacks or
/// resources; copying or dropping a report does not dispatch or discard work.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Work {
    /// Notify the link future that progress or closure may be observable.
    pub wake: bool,
    /// Release a closed link's resources after its terminal completions.
    pub reclaim: bool,
    /// Notify authority waiters after deferred admission guards have exited.
    pub notify_authority: bool,
}

impl Work {
    #[must_use]
    pub const fn is_empty(self) -> bool {
        !self.wake && !self.reclaim && !self.notify_authority
    }
}

/// Result of an I/O pass. `work` is indexed by slot, not by peer identity.
/// It is an advisory snapshot; [`Driver::dispatch`] consumes the live pending
/// work, including work retained after an I/O error.
#[derive(Clone, Copy, Debug)]
pub struct Progress<const LINKS: usize> {
    /// CQEs consumed, including cancellation completions.
    pub completions: usize,
    /// New SQEs accepted, including cancellation requests.
    pub submissions: usize,
    /// Sum of lengths requested by newly accepted send/receive SQEs.
    pub bytes: usize,
    /// I/O scheduling advice at the end of this pass.
    pub schedule: Schedule,
    /// Pending owner dispatch work; the combined path reports after dispatch.
    pub work: [Work; LINKS],
}

fn token(id: OpId) -> u64 {
    (u64::from(id.generation) << 32) | u64::from(id.slot)
}

struct Buffers<const BYTES: usize> {
    data: UnsafeCell<[u8; BYTES]>,
    send: UnsafeCell<[u8; CREDIT_BYTES]>,
    receive: UnsafeCell<[u8; CREDIT_BYTES]>,
}
impl<const BYTES: usize> Buffers<BYTES> {
    fn new() -> Self {
        Self {
            data: UnsafeCell::new([0; BYTES]),
            send: UnsafeCell::new([0; CREDIT_BYTES]),
            receive: UnsafeCell::new([0; CREDIT_BYTES]),
        }
    }
    fn pointer(&self, lane: Lane) -> *mut u8 {
        match lane {
            Lane::Data => self.data.get().cast(),
            Lane::CreditSend => self.send.get().cast(),
            Lane::CreditReceive => self.receive.get().cast(),
        }
    }
    fn capacity(lane: Lane) -> usize {
        match lane {
            Lane::Data => BYTES,
            _ => CREDIT_BYTES,
        }
    }
}

/// Fixed transport buffers that can be allocated before constructing a driver.
/// Moving the pool preserves buffer addresses. Passing it to [`Driver::with_pool`]
/// transfers ownership until outstanding kernel operations have completed.
pub struct Pool<const LINKS: usize = 8, const BYTES: usize = 4096> {
    buffers: Box<[Buffers<BYTES>]>,
}

impl<const LINKS: usize, const BYTES: usize> Pool<LINKS, BYTES> {
    /// Allocates one data buffer and two credit buffers per link.
    /// `BYTES` includes the 38-byte HWY1 header.
    ///
    /// # Errors
    /// Returns `InvalidInput` for unsupported dimensions and `OutOfMemory`
    /// when the buffer allocation fails.
    pub fn try_new() -> io::Result<Self> {
        if LINKS == 0
            || LINKS > (CANCEL as usize) / 3
            || BYTES < HEADER_BYTES
            || BYTES > i32::MAX as usize
        {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }
        let mut buffers = Vec::new();
        buffers
            .try_reserve_exact(LINKS)
            .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
        for _ in 0..LINKS {
            buffers.push(Buffers::new());
        }
        Ok(Self {
            buffers: buffers.into_boxed_slice(),
        })
    }
}

struct Slot {
    generation: u64,
    protocol: Option<Protocol>,
    sockets: Option<(OwnedFd, OwnedFd)>,
    operations: [Operation; 3],
    receive_hints: [bool; 3],
    alive: bool,
    waker: Option<Waker>,
    work: Work,
    next_lane: usize,
    contended: bool,
}
impl Slot {
    // Pool::try_new bounds operation indices below the cancellation bit.
    #[allow(clippy::cast_possible_truncation)]
    fn new(index: usize) -> Self {
        Self {
            generation: 0,
            protocol: None,
            sockets: None,
            operations: std::array::from_fn(|lane| Operation::new((index * 3 + lane) as u32)),
            receive_hints: [false; 3],
            alive: false,
            waker: None,
            work: Work::default(),
            next_lane: 0,
            contended: false,
        }
    }
    fn idle(&self) -> bool {
        self.operations
            .iter()
            .all(|op| op.phase() == Phase::Available)
    }
    fn pending_lane(&self) -> Option<Lane> {
        let protocol = self.protocol.as_ref()?;
        (0..3)
            .map(|offset| Lane::ALL[(self.next_lane + offset) % 3])
            .find(|&lane| {
                let operation = &self.operations[lane as usize];
                operation.cancellation().is_some()
                    || (matches!(operation.phase(), Phase::Available | Phase::Prepared)
                        && matches!(
                            protocol.effect(lane),
                            Some(Effect::Receive { .. } | Effect::Transmit { .. })
                        ))
            })
    }
}

struct Ready<const LINKS: usize> {
    entries: [usize; LINKS],
    present: [bool; LINKS],
    head: usize,
    len: usize,
}
impl<const LINKS: usize> Ready<LINKS> {
    fn new() -> Self {
        Self {
            entries: [0; LINKS],
            present: [false; LINKS],
            head: 0,
            len: 0,
        }
    }
    fn push(&mut self, index: usize) {
        if !self.present[index] {
            self.entries[(self.head + self.len) % LINKS] = index;
            self.present[index] = true;
            self.len += 1;
        }
    }
    fn at(&self, offset: usize) -> usize {
        self.entries[(self.head + offset) % LINKS]
    }
    fn pop(&mut self) -> usize {
        let index = self.entries[self.head];
        self.head = (self.head + 1) % LINKS;
        self.len -= 1;
        self.present[index] = false;
        index
    }
    fn prioritize(&mut self, index: usize) {
        if self.present[index] {
            while self.entries[self.head] != index {
                let head = self.pop();
                self.push(head);
            }
        }
    }
}

struct Shared<G, const LINKS: usize, const BYTES: usize> {
    archived: RefCell<Records<Record, 64>>,
    retired_lost: Cell<u64>,
    admitting: Cell<bool>,
    detached: Cell<bool>, // kernel access has been proven terminal
    slots: RefCell<[Slot; LINKS]>,
    reservations: [RefCell<Option<G>>; LINKS],
    ready: RefCell<Ready<LINKS>>,
    buffers: Box<[Buffers<BYTES>]>,
}

impl<G: Reservation, const LINKS: usize, const BYTES: usize> Shared<G, LINKS, BYTES> {
    fn pending_work(&self) -> [Work; LINKS] {
        let slots = self.slots.borrow();
        std::array::from_fn(|index| slots[index].work)
    }

    fn dispatch_with_budget(&self, cursor: &mut usize, budget: usize) -> usize {
        if budget == 0 {
            return 0;
        }
        let pending = self.pending_work();
        let mut dispatched = 0;
        for _ in 0..LINKS {
            let index = *cursor;
            *cursor = (index + 1) % LINKS;
            let work = pending[index];
            if work.is_empty() {
                continue;
            }
            self.dispatch_slot(index, work);
            dispatched += 1;
            if dispatched == budget {
                break;
            }
        }
        dispatched
    }

    fn dispatch_slot(&self, index: usize, work: Work) {
        if work.is_empty() {
            return;
        }
        let mut reservation = if work.reclaim || work.notify_authority {
            self.reservations[index].try_borrow_mut().ok()
        } else {
            None
        };
        if (work.reclaim || work.notify_authority) && reservation.is_none() {
            let wake = {
                let mut slots = self.slots.borrow_mut();
                let slot = &mut slots[index];
                slot.work.wake &= !work.wake;
                if work.wake {
                    slot.waker.take()
                } else {
                    None
                }
            };
            if let Some(wake) = wake {
                wake.wake();
            }
            return;
        }

        let (wake, sockets, mut retired, owned_reservation, reclaim) = {
            let mut slots = self.slots.borrow_mut();
            let slot = &mut slots[index];
            slot.work.wake &= !work.wake;
            slot.work.reclaim &= !work.reclaim;
            slot.work.notify_authority &= !work.notify_authority;
            let wake = if work.wake { slot.waker.take() } else { None };
            let reclaim = work.reclaim
                && slot.idle()
                && slot.protocol.as_ref().is_some_and(|p| p.closed().is_some());
            let sockets = if reclaim { slot.sockets.take() } else { None };
            let owned_reservation = if reclaim {
                reservation
                    .as_mut()
                    .and_then(|reservation| reservation.take())
            } else {
                None
            };
            let retired = if reclaim && !slot.alive {
                slot.protocol.take()
            } else {
                None
            };
            (wake, sockets, retired, owned_reservation, reclaim)
        };
        drop(reservation);

        if let Some(protocol) = &mut retired {
            while let Some(record) = protocol.pop_record() {
                self.archived.borrow_mut().push(record);
            }
            self.retired_lost.set(
                self.retired_lost
                    .get()
                    .saturating_add(protocol.lost_records()),
            );
        }
        if work.notify_authority {
            if reclaim {
                if let Some(reservation) = &owned_reservation {
                    reservation.maintain();
                }
            } else {
                let callback_exit = CallbackExit::new(self, index);
                let reservation = self.reservations[index].borrow();
                if let Some(reservation) = reservation.as_ref() {
                    reservation.maintain();
                }
                drop(reservation);
                drop(callback_exit);
            }
        }
        drop(sockets);
        drop(owned_reservation);
        if let Some(wake) = wake {
            wake.wake();
        }
    }

    fn cleanup_detached_slot(&self, index: usize) {
        if !self.detached.get() {
            return;
        }
        let work = {
            let slots = self.slots.borrow();
            let slot = &slots[index];
            if !slot.idle() || slot.protocol.as_ref().is_none_or(|p| p.closed().is_none()) {
                return;
            }
            slot.work
        };
        self.dispatch_slot(index, work);
    }
}

struct CallbackExit<'a, G: Reservation, const LINKS: usize, const BYTES: usize> {
    shared: &'a Shared<G, LINKS, BYTES>,
    index: usize,
}

impl<'a, G: Reservation, const LINKS: usize, const BYTES: usize> CallbackExit<'a, G, LINKS, BYTES> {
    fn new(shared: &'a Shared<G, LINKS, BYTES>, index: usize) -> Self {
        Self { shared, index }
    }
}

impl<G: Reservation, const LINKS: usize, const BYTES: usize> Drop
    for CallbackExit<'_, G, LINKS, BYTES>
{
    fn drop(&mut self) {
        self.shared.cleanup_detached_slot(self.index);
    }
}

/// Single-owner I/O domain. Construction allocates the ring and slot table;
/// buffers come from a supplied pool or are allocated by [`Driver::new`].
/// Attachment, polling, submission, completion and removal never grow storage.
///
/// This driver is thread-local. The host calls [`Self::advance`] for combined
/// I/O and callback dispatch, or separates them with [`Self::advance_io`] and
/// [`Self::dispatch`]. Future polling never drives or waits on the ring.
/// All supplied descriptors must be connected stream sockets.
pub struct Driver<G: Reservation = (), const LINKS: usize = 8, const BYTES: usize = 4096> {
    ring: ManuallyDrop<IoUring>,
    shared: ManuallyDrop<Rc<Shared<G, LINKS, BYTES>>>,
    pending_completions: usize,
    receive_ioprio: u16,
    dispatch_cursor: usize,
}

impl<G: Reservation, const LINKS: usize, const BYTES: usize> Driver<G, LINKS, BYTES> {
    /// `entries` includes one submission slot reserved for cancellation.
    /// Buffer bounds include the 38-byte HWY1 header.
    /// Ring setup consumes kernel memory accounted to the UID. Provision
    /// memlock headroom for this ring and other `io_uring` allocations charged
    /// to that UID through the process or service's `RLIMIT_MEMLOCK`.
    ///
    /// # Errors
    /// Returns invalid capacity, arena allocation or ring setup errors.
    /// Ring allocation can return `ENOMEM` when memlock headroom or available
    /// memory is exhausted. [`crate::setup_diagnostic`] formats setup errors
    /// with current memlock limits and allocation guidance.
    pub fn new(entries: u32) -> io::Result<Self> {
        Self::with_pool(entries, Pool::try_new()?)
    }

    /// Creates a driver using the supplied pool without reallocating its buffers.
    /// `entries` includes one submission slot reserved for cancellation.
    /// The driver retains the buffers until kernel access ends, including after
    /// link futures are dropped. If teardown cannot establish completion, it
    /// retains the storage to preserve live kernel pointers.
    ///
    /// # Errors
    /// Returns `InvalidInput` for unsupported ring dimensions, or the ring's
    /// initialization error. Construction failure drops the supplied pool.
    /// Ring setup uses the process or service's `RLIMIT_MEMLOCK` against
    /// kernel memory accounted to the UID. [`crate::setup_diagnostic`] formats
    /// a setup error with current memlock limits and allocation guidance.
    pub fn with_pool(entries: u32, pool: Pool<LINKS, BYTES>) -> io::Result<Self> {
        if entries < 2 {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }
        let cq_entries = LINKS
            .checked_mul(6)
            .and_then(|count| count.max(entries as usize).checked_next_power_of_two())
            .and_then(|count| u32::try_from(count).ok())
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        // Every original operation and its cancellation may complete together.
        // Optional hints; retain the original setup if the kernel rejects them.
        let ring = IoUring::builder()
            .setup_cqsize(cq_entries)
            .setup_single_issuer()
            .setup_coop_taskrun()
            .setup_taskrun_flag()
            .build(entries)
            .or_else(|error| {
                if error.kind() == io::ErrorKind::InvalidInput {
                    IoUring::builder().setup_cqsize(cq_entries).build(entries)
                } else {
                    Err(error)
                }
            })?;
        Ok(Self {
            ring: ManuallyDrop::new(ring),
            shared: ManuallyDrop::new(Rc::new(Shared {
                archived: RefCell::new(Records::new()),
                retired_lost: Cell::new(0),
                admitting: Cell::new(true),
                detached: Cell::new(false),
                slots: RefCell::new(std::array::from_fn(Slot::new)),
                reservations: std::array::from_fn(|_| RefCell::new(None)),
                ready: RefCell::new(Ready::new()),
                buffers: pool.buffers,
            })),
            pending_completions: 0,
            receive_ioprio: 1, // IORING_RECVSEND_POLL_FIRST
            dispatch_cursor: 0,
        })
    }

    /// Per-link arena bytes to reserve in an accounting adapter.
    #[must_use]
    pub const fn reservation_bytes() -> usize {
        std::mem::size_of::<Slot>()
            + std::mem::size_of::<RefCell<Option<G>>>()
            + std::mem::size_of::<Buffers<BYTES>>()
            + std::mem::size_of::<usize>()
            + std::mem::size_of::<bool>()
    }

    fn attach<E: WireCodec>(
        &self,
        direction: Direction,
        data: OwnedFd,
        control: OwnedFd,
        reservation: G,
    ) -> Result<Handle<G, LINKS, BYTES>, Error> {
        if !self.shared.admitting.get() {
            return Err(Error::Closed);
        }
        reservation
            .check(Contract::of::<E>(), direction, Self::reservation_bytes())
            .map_err(Error::Topic)?;
        drop(reservation.enter().map_err(Error::Topic)?);
        // Reservation callbacks may have stopped admission or cancelled links.
        if !self.shared.admitting.get() {
            return Err(Error::Closed);
        }
        let mut slots = self.shared.slots.borrow_mut();
        let index = slots
            .iter()
            .position(|slot| slot.protocol.is_none() && slot.generation != u64::MAX)
            .ok_or(Error::Capacity)?;
        let slot = &mut slots[index];
        let generation = slot.generation + 1;
        let protocol = Protocol::with_connection(
            Contract::of::<E>(),
            direction,
            BYTES,
            Connection {
                id: u64::try_from(index).map_err(|_| Error::Capacity)?,
                generation,
            },
        )?;
        slot.generation = generation;
        slot.protocol = Some(protocol);
        slot.sockets = Some((data, control));
        *self.shared.reservations[index].borrow_mut() = Some(reservation);
        slot.alive = true;
        slot.next_lane = 0;
        slot.contended = false;
        self.shared.ready.borrow_mut().push(index);
        Ok(Handle {
            shared: Rc::clone(&self.shared),
            index,
            connection: Connection {
                id: u64::try_from(index).map_err(|_| Error::Capacity)?,
                generation,
            },
        })
    }

    /// Attaches an authorized export using the caller's receiver and sockets.
    ///
    /// # Errors
    /// Returns authority, reservation, buffer or link-capacity rejection.
    pub fn export<E: WireCodec, R: EventReceiver<E>>(
        &self,
        receiver: R,
        data: impl Into<OwnedFd>,
        control: impl Into<OwnedFd>,
        reservation: G,
    ) -> Result<impl Future<Output = Result<(), Error>>, Error> {
        self.export_tracked(receiver, data, control, reservation)
            .map(|(_, link)| link)
    }

    /// Attaches an export and returns its host-assigned connection identity.
    /// Use that identity to associate records with the provisioned endpoint.
    ///
    /// # Errors
    /// Returns the same attachment errors as [`Self::export`].
    pub fn export_tracked<E: WireCodec, R: EventReceiver<E>>(
        &self,
        receiver: R,
        data: impl Into<OwnedFd>,
        control: impl Into<OwnedFd>,
        reservation: G,
    ) -> Result<(Connection, impl Future<Output = Result<(), Error>>), Error> {
        let io = self.attach::<E>(Direction::Export, data.into(), control.into(), reservation)?;
        Ok((io.connection, transport::export(receiver, io)))
    }

    /// Attaches an authorized import using the caller's sender and sockets.
    ///
    /// # Errors
    /// Returns authority, reservation, buffer or link-capacity rejection.
    pub fn import<E: WireCodec, S: EventSender<E>>(
        &self,
        sender: S,
        data: impl Into<OwnedFd>,
        control: impl Into<OwnedFd>,
        reservation: G,
    ) -> Result<impl Future<Output = Result<(), Error>>, Error> {
        self.import_tracked(sender, data, control, reservation)
            .map(|(_, link)| link)
    }

    /// Attaches an import and returns its host-assigned connection identity.
    /// Use that identity to associate records with the provisioned endpoint.
    ///
    /// # Errors
    /// Returns the same attachment errors as [`Self::import`].
    pub fn import_tracked<E: WireCodec, S: EventSender<E>>(
        &self,
        sender: S,
        data: impl Into<OwnedFd>,
        control: impl Into<OwnedFd>,
        reservation: G,
    ) -> Result<(Connection, impl Future<Output = Result<(), Error>>), Error> {
        let io = self.attach::<E>(Direction::Import, data.into(), control.into(), reservation)?;
        Ok((io.connection, transport::import(sender, io)))
    }

    /// Permanently rejects new link attachments. Existing links keep running
    /// until closed individually or through [`Self::cancel_all`]. No callbacks,
    /// I/O submissions or resource destruction occur here.
    pub fn stop_admission(&self) {
        self.shared.admitting.set(false);
    }

    /// Drains bounded accountability history without callbacks. IDs are slots
    /// in this driver domain; generations increase on successful attachment and
    /// never wrap. Each connection retains eight records; retired connections share
    /// a 64-record archive. Ordering is per connection, not across connections.
    #[must_use]
    pub fn pop_record(&self) -> Option<Record> {
        self.shared.archived.borrow_mut().pop().or_else(|| {
            self.shared
                .slots
                .borrow_mut()
                .iter_mut()
                .filter_map(|slot| slot.protocol.as_mut())
                .find_map(Protocol::pop_record)
        })
    }

    /// Saturating total of overwritten records, including retired generations.
    #[must_use]
    pub fn lost_records(&self) -> u64 {
        self.shared
            .slots
            .borrow()
            .iter()
            .filter_map(|slot| slot.protocol.as_ref())
            .fold(
                self.shared
                    .retired_lost
                    .get()
                    .saturating_add(self.shared.archived.borrow().lost()),
                |lost, protocol| lost.saturating_add(protocol.lost_records()),
            )
    }

    /// Stops admission and closes every link without callbacks or waiting.
    /// Repeated calls are harmless. Cancellation requests are submitted by later
    /// budgeted I/O passes; buffers and reservations stay owned through terminal
    /// original and cancellation CQEs. Dispatch closure wakes separately.
    pub fn cancel_all(&self) {
        self.stop_admission();
        for index in 0..LINKS {
            {
                let mut slots = self.shared.slots.borrow_mut();
                let slot = &mut slots[index];
                if let Some(protocol) = &mut slot.protocol {
                    slot.work.wake |= protocol.closed().is_none();
                    let _ = protocol.input(Input::Closed(Error::Closed));
                    slot.contended = false;
                    if !slot.idle() || slot.sockets.is_some() {
                        self.shared.ready.borrow_mut().push(index);
                    }
                }
            }
            self.refresh(index);
        }
    }

    /// Admission is stopped, all kernel operations have terminal completions,
    /// and link resources and pending owner work have been dispatched. Dropping
    /// the driver then requires no draining loop. Live link futures still retain
    /// the buffer pool and observe closure when polled; drop them separately.
    #[must_use]
    pub fn shutdown_complete(&self) -> bool {
        !self.shared.admitting.get()
            && self.pending_completions == 0
            && self.shared.slots.borrow().iter().all(|slot| {
                slot.idle()
                    && slot.sockets.is_none()
                    && slot.work.is_empty()
                    && slot.protocol.as_ref().is_none_or(|p| p.closed().is_some())
            })
            && self
                .shared
                .reservations
                .iter()
                .all(|r| r.borrow().is_none())
    }

    fn kernel_drained(&self) -> bool {
        self.pending_completions == 0 && self.shared.slots.borrow().iter().all(Slot::idle)
    }

    /// Advances I/O and dispatches deferred wakes and reclamation. This convenience
    /// path may run reservation callbacks and destructors. Poll link futures
    /// separately to run codecs and endpoint operations. Consult [`Self::schedule`]
    /// before parking: submission backlog does not wake link futures.
    ///
    /// # Errors
    /// Returns submission errors; queued operations retain their storage.
    pub fn advance(&mut self) -> io::Result<usize> {
        let result = self.advance_with::<Immediate>(self.default_budget());
        self.dispatch();
        result.map(|(completions, _, _)| completions)
    }

    /// Combined I/O and callback dispatch with explicit per-pass I/O limits.
    /// The budget does not bound callback execution time.
    ///
    /// # Errors
    /// Returns `InvalidInput` for a zero limit, or the ring submission error.
    /// Pending dispatch work is serviced even if ring submission fails.
    pub fn advance_with_budget(&mut self, budget: Budget) -> io::Result<Progress<LINKS>> {
        let result = self.advance_with::<Immediate>(budget);
        self.dispatch();
        result.map(|(completions, submissions, bytes)| Progress {
            completions,
            submissions,
            bytes,
            schedule: self.schedule(),
            work: self.pending_work(),
        })
    }

    /// One bounded I/O pass without application callbacks, waker operations,
    /// payload decoding, endpoint polling, or resource reclamation.
    /// Authorization still brackets each submission. Notifications and cleanup
    /// remain pending until [`Self::dispatch`]; omitting dispatch retains resources
    /// and may leave waiters parked. Poll link futures outside this step.
    /// Only `()` and Hiway's `GrantReservation` support this path. The pass does
    /// not wait for completions; syscall latency is not bounded by this API.
    ///
    /// # Errors
    /// Returns the ring submission error. Pending work is retained and remains
    /// available through [`Self::pending_work`] even when this call fails.
    pub fn advance_io(&mut self) -> io::Result<Progress<LINKS>>
    where
        G: StrictReservation,
    {
        self.advance_io_with_budget(self.default_budget())
    }

    /// Strict I/O with explicit per-pass limits; see [`Self::advance_io`].
    ///
    /// # Errors
    /// Returns `InvalidInput` for a zero limit, or the ring submission error.
    /// Already pending dispatch work is retained in either case.
    pub fn advance_io_with_budget(&mut self, budget: Budget) -> io::Result<Progress<LINKS>>
    where
        G: StrictReservation,
    {
        self.advance_with::<Deferred>(budget)
            .map(|(completions, submissions, bytes)| Progress {
                completions,
                submissions,
                bytes,
                schedule: self.schedule(),
                work: self.pending_work(),
            })
    }

    fn default_budget(&self) -> Budget {
        Budget {
            completions: self.ring.params().cq_entries() as usize,
            submissions: self.ring.params().sq_entries() as usize,
            bytes: usize::MAX,
        }
    }

    /// Copies the bounded pending-work records without executing or removing them.
    #[must_use]
    pub fn pending_work(&self) -> [Work; LINKS] {
        self.shared.pending_work()
    }

    /// Dispatches one pass of pending authority notifications, reclamation and
    /// wakes. Call outside the strict path, before parking the host. Callbacks
    /// run without the slot-table borrow. Work is coalesced per slot; this method
    /// makes one pass rather than draining work until idle.
    pub fn dispatch(&mut self) {
        self.dispatch_with_budget(LINKS);
    }

    /// Dispatches at most `slots` pending slot records, rotating the starting
    /// slot between calls. Returns the number dispatched; zero performs no work.
    /// A record may include authority notification, resource destruction and a
    /// wake. Callback execution time is not bounded. Newly queued work waits
    /// for a later call. See [`Self::dispatch`] for callback isolation.
    pub fn dispatch_with_budget(&mut self, slots: usize) -> usize {
        self.shared
            .dispatch_with_budget(&mut self.dispatch_cursor, slots)
    }

    fn scan_authority(&self) {
        for index in 0..LINKS {
            let check_authority = {
                let mut slots = self.shared.slots.borrow_mut();
                let slot = &mut slots[index];
                slot.contended = false;
                slot.protocol
                    .as_ref()
                    .is_some_and(|protocol| protocol.closed().is_none())
            };
            let revoked = check_authority
                && self.shared.reservations[index]
                    .borrow()
                    .as_ref()
                    .is_some_and(Reservation::is_revoked);
            if revoked {
                let mut slots = self.shared.slots.borrow_mut();
                let slot = &mut slots[index];
                if let Some(protocol) = &mut slot.protocol {
                    if protocol.closed().is_none() {
                        let _ = protocol.input(Input::Revoked);
                        slot.work.wake = true;
                        self.shared.ready.borrow_mut().push(index);
                    }
                }
            }
        }
    }

    fn advance_with<M: Admission<G>>(
        &mut self,
        budget: Budget,
    ) -> io::Result<(usize, usize, usize)> {
        if budget.completions == 0 || budget.submissions == 0 || budget.bytes == 0 {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }
        let mut completed = 0;
        for _ in 0..budget
            .completions
            .min(self.ring.params().cq_entries() as usize)
        {
            let cqe = self.ring.completion().next();
            let Some(cqe) = cqe else {
                break;
            };
            self.pending_completions -= 1;
            self.complete(cqe.user_data(), cqe.result());
            completed += 1;
        }
        self.scan_authority();
        let mut submissions = 0;
        let mut bytes = 0;
        let mut blocked = None;
        // One request per link per round. Three rounds can service all lanes,
        // without retrying a contended admission in the same pass.
        'rounds: for _ in Lane::ALL {
            let ready = self.shared.ready.borrow().len;
            let before = submissions;
            for _ in 0..ready {
                if submissions == budget.submissions {
                    break 'rounds;
                }
                let index = self.shared.ready.borrow_mut().pop();
                self.refresh(index);
                let lane = {
                    let slots = self.shared.slots.borrow();
                    let slot = &slots[index];
                    if slot.contended {
                        None
                    } else {
                        slot.pending_lane()
                    }
                };
                if let Some(lane) = lane {
                    let cancelling = self.shared.slots.borrow()[index].operations[lane as usize]
                        .cancellation()
                        .is_some();
                    let attempt = if cancelling {
                        self.cancel(index, lane)
                    } else {
                        self.submit::<M>(index, lane, budget.bytes - bytes)
                    };
                    match attempt {
                        Attempt::Submitted(length) => {
                            submissions += 1;
                            bytes += length;
                            self.shared.slots.borrow_mut()[index].next_lane =
                                (lane as usize + 1) % 3;
                        }
                        Attempt::Blocked => {
                            blocked.get_or_insert(index);
                        }
                        Attempt::Idle => {}
                    }
                }
                self.refresh(index);
                if self.shared.slots.borrow()[index].pending_lane().is_some() {
                    self.shared.ready.borrow_mut().push(index);
                }
            }
            if submissions == before {
                break;
            }
        }
        // Scan past a byte/SQ-blocked link so cancellations can still use the
        // reserved SQ entry, but preserve the first denied link's next turn.
        if let Some(index) = blocked {
            self.shared.ready.borrow_mut().prioritize(index);
        }
        let submit = {
            let queue = self.ring.submission();
            !queue.is_empty() || queue.cq_overflow() || queue.taskrun()
        };
        if submit {
            self.ring.submit()?;
        }
        Ok((completed, submissions, bytes))
    }

    /// Refreshes I/O scheduling advice without callbacks or consuming work.
    /// Host readiness must include local endpoint and authority changes, not
    /// only the ring's eventfd. Dispatch pending work before parking.
    pub fn schedule(&mut self) -> Schedule {
        if !self.ring.completion().is_empty() || !self.ring.submission().is_empty() {
            return Schedule::Continue;
        }
        let ready = self.shared.ready.borrow();
        let slots = self.shared.slots.borrow();
        if (0..ready.len).any(|offset| !slots[ready.at(offset)].contended) {
            Schedule::Continue
        } else if ready.len != 0 {
            Schedule::Retry
        } else {
            Schedule::Wait
        }
    }

    /// Explicit host wait. Do not call this from a future's `poll` method.
    /// Local endpoint readiness still belongs to the host's executor/waker.
    /// Returns immediately if I/O, contention retry or dispatch work is pending.
    ///
    /// # Errors
    /// Returns the kernel's submission or wait error.
    pub fn wait(&mut self) -> io::Result<usize> {
        if self.pending_completions == 0
            || self.schedule() != Schedule::Wait
            || self.pending_work().iter().any(|work| !work.is_empty())
        {
            return Ok(0);
        }
        self.ring.submit_and_wait(1)
    }

    /// Connects CQ notification to a host-owned eventfd/reactor. Register before
    /// entering the host wait; local endpoint wakes remain a separate source.
    ///
    /// # Errors
    /// Returns the kernel's eventfd registration error.
    pub fn register_eventfd(&self, eventfd: BorrowedFd<'_>) -> io::Result<()> {
        self.ring.submitter().register_eventfd(eventfd.as_raw_fd())
    }

    #[must_use]
    pub fn active_links(&self) -> usize {
        self.shared
            .slots
            .borrow()
            .iter()
            .filter(|slot| slot.protocol.is_some())
            .count()
    }

    // Tokens deliberately unpack their two u32 words. The negative-result
    // branch handles errors before a completion length is converted to usize.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn complete(&mut self, user_data: u64, result: i32) {
        let index = (user_data as u32 & !CANCEL) as usize;
        if index / 3 >= LINKS {
            return;
        }
        let id = OpId {
            slot: index as u32,
            generation: (user_data >> 32) as u32,
        };
        let lane = Lane::ALL[index % 3];
        let mut slots = self.shared.slots.borrow_mut();
        let slot = &mut slots[index / 3];
        let operation = &mut slot.operations[lane as usize];
        if user_data & u64::from(CANCEL) != 0 {
            operation.cancel_completed(id);
        } else if operation.completed(id) {
            if let Some(protocol) = &mut slot.protocol {
                // SAFETY: this single-shot operation has its terminal CQE. No
                // other operation uses this lane's buffer, and Shared is !Send.
                let buffer = unsafe {
                    std::slice::from_raw_parts(
                        self.shared.buffers[index / 3].pointer(lane),
                        Buffers::<BYTES>::capacity(lane),
                    )
                };
                let result = match result {
                    -22 if std::mem::take(&mut slot.receive_hints[lane as usize]) => {
                        self.receive_ioprio = 0;
                        IoResult::Retry
                    }
                    -4 | -11 => IoResult::Retry, // EINTR / EAGAIN
                    value if value < 0 => IoResult::Failed(-value),
                    value => IoResult::Bytes(value as usize),
                };
                let before = (protocol.closed(), protocol.effect(Lane::Data));
                let _ = protocol.input(Input::Completed {
                    op: id,
                    result,
                    buffer,
                });
                let effect = protocol.effect(Lane::Data);
                slot.work.wake |= protocol.closed() != before.0
                    || (matches!(effect, Some(Effect::Provide | Effect::Admit { .. }))
                        && effect != before.1);
            }
        }
        operation.reclaim();
        self.shared.ready.borrow_mut().push(index / 3);
    }

    fn cancel(&mut self, index: usize, lane: Lane) -> Attempt {
        let mut slots = self.shared.slots.borrow_mut();
        let operation = &mut slots[index].operations[lane as usize];
        let Some(id) = operation.cancellation() else {
            return Attempt::Idle;
        };
        let entry = opcode::AsyncCancel::new(token(id))
            .build()
            .user_data(token(id) | u64::from(CANCEL));
        // SAFETY: cancellation owns no borrowed memory. Its CQE has a separate
        // tag, and the original buffer stays retained through both completions.
        if unsafe { self.ring.submission().push(&entry) }.is_ok() {
            self.pending_completions += 1;
            operation.cancel_accepted(id);
            Attempt::Submitted(0)
        } else {
            Attempt::Blocked
        }
    }

    fn submit<M: Admission<G>>(&mut self, index: usize, lane: Lane, bytes: usize) -> Attempt {
        {
            let slots = self.shared.slots.borrow();
            let slot = &slots[index];
            if !matches!(
                slot.operations[lane as usize].phase(),
                Phase::Available | Phase::Prepared
            ) || !slot.protocol.as_ref().is_some_and(|protocol| {
                matches!(
                    protocol.effect(lane),
                    Some(Effect::Receive { .. } | Effect::Transmit { .. })
                )
            }) {
                return Attempt::Idle;
            }
            let sq = self.ring.submission();
            if bytes == 0 || sq.len() >= sq.capacity() - 1 {
                return Attempt::Blocked;
            }
        }
        let reservation = self.shared.reservations[index].borrow();
        let Some(reservation) = reservation.as_ref() else {
            return Attempt::Idle;
        };
        if M::DEFERRED {
            self.shared.slots.borrow_mut()[index].work.notify_authority = true;
        }
        let result = M::with_access(reservation, || {
            Self::submit_entered(
                &mut self.ring,
                &self.shared,
                &mut self.pending_completions,
                index,
                lane,
                bytes,
                self.receive_ioprio,
            )
        });
        match result {
            Ok(attempt) => attempt,
            Err(TopicError::Contended) => {
                self.shared.slots.borrow_mut()[index].contended = true;
                Attempt::Idle
            }
            Err(error) => {
                let input = if error == TopicError::Revoked {
                    Input::Revoked
                } else {
                    Input::Closed(Error::Topic(error))
                };
                let mut slots = self.shared.slots.borrow_mut();
                let slot = &mut slots[index];
                if let Some(protocol) = &mut slot.protocol {
                    let _ = protocol.input(input);
                    slot.work.wake = true;
                }
                Attempt::Idle
            }
        }
    }

    fn submit_entered(
        ring: &mut IoUring,
        shared: &Shared<G, LINKS, BYTES>,
        pending_completions: &mut usize,
        index: usize,
        lane: Lane,
        bytes: usize,
        receive_ioprio: u16,
    ) -> Attempt {
        let mut slots = shared.slots.borrow_mut();
        let slot = &mut slots[index];
        let Some(protocol) = &mut slot.protocol else {
            return Attempt::Idle;
        };
        let Some(effect) = protocol.effect(lane) else {
            return Attempt::Idle;
        };
        let (offset, length, receive) = match effect {
            Effect::Receive { offset, length, .. } => (offset, length, true),
            Effect::Transmit { offset, length, .. } => (offset, length, false),
            _ => return Attempt::Idle,
        };
        let length = length.min(bytes);
        let operation = &mut slot.operations[lane as usize];
        if !matches!(operation.phase(), Phase::Available | Phase::Prepared) {
            return Attempt::Idle;
        }
        let mut sq = ring.submission();
        if sq.len() >= sq.capacity() - 1 {
            return Attempt::Blocked;
        }
        let id = if operation.phase() == Phase::Prepared {
            operation.id()
        } else {
            let Some(id) = operation.prepare() else {
                let _ = protocol.input(Input::Closed(Error::Capacity));
                slot.work.wake = true;
                return Attempt::Idle;
            };
            id
        };
        let buffers = &shared.buffers[index];
        if lane == Lane::CreditSend && offset == 0 {
            // SAFETY: this lane is not submitted and has no other buffer reader.
            unsafe {
                *buffers.send.get() = protocol.credit();
            }
        }
        let sockets = slot.sockets.as_ref().unwrap();
        let fd = types::Fd(match lane {
            Lane::Data => sockets.0.as_raw_fd(),
            _ => sockets.1.as_raw_fd(),
        });
        // SAFETY: protocol effects are within the fixed buffer's bounds. No
        // access or recycling is permitted until this operation's terminal CQE.
        let pointer = unsafe { buffers.pointer(lane).add(offset) };
        let receive_ioprio = if receive && offset == 0 {
            receive_ioprio
        } else {
            0
        };
        // Every buffer length is bounded to u32 by Pool::try_new and Protocol::new.
        #[allow(clippy::cast_possible_truncation)]
        let entry = if receive {
            opcode::Recv::new(fd, pointer, length as u32)
                .ioprio(receive_ioprio)
                .build()
        } else {
            opcode::Send::new(fd, pointer.cast_const(), length as u32)
                .flags(0x4000)
                .build() // MSG_NOSIGNAL
        }
        .user_data(token(id));
        // SAFETY: Shared owns stable buffers and descriptors; Driver::drop
        // drains every accepted operation before releasing that ownership.
        if unsafe { sq.push(&entry) }.is_ok() {
            *pending_completions += 1;
            slot.receive_hints[lane as usize] = receive_ioprio != 0;
            operation.accepted(id);
            let _ = protocol.input(Input::Accepted { lane, op: id });
            Attempt::Submitted(length)
        } else {
            Attempt::Blocked
        }
    }

    fn refresh(&self, index: usize) {
        let mut slots = self.shared.slots.borrow_mut();
        let slot = &mut slots[index];
        if slot.protocol.as_ref().is_none_or(|p| p.closed().is_none()) {
            return;
        }
        for operation in &mut slot.operations {
            operation.cancel();
            operation.reclaim();
        }
        slot.work.reclaim |= slot.idle() && (slot.sockets.is_some() || !slot.alive);
    }
}

impl<G: Reservation, const LINKS: usize, const BYTES: usize> Drop for Driver<G, LINKS, BYTES> {
    fn drop(&mut self) {
        if !self.shutdown_complete() {
            self.cancel_all();
        }
        loop {
            let result = self.advance();
            if self.kernel_drained() {
                break;
            }
            if let Err(error) = result {
                if matches!(
                    error.kind(),
                    io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
                ) {
                    continue;
                }
                // A broken ring cannot prove that kernel access has ended.
                // Retain the whole domain rather than free live kernel pointers.
                return;
            }
            if self.pending_completions != 0 {
                if let Err(error) = self.ring.submit_and_wait(1) {
                    if matches!(
                        error.kind(),
                        io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
                    ) {
                        continue;
                    }
                    // A broken ring cannot prove that kernel access has ended.
                    // Retain the whole domain rather than free live kernel pointers.
                    return;
                }
            }
        }
        self.shared.detached.set(true);
        for index in 0..LINKS {
            self.refresh(index);
        }
        self.shared
            .dispatch_with_budget(&mut self.dispatch_cursor, LINKS);
        // SAFETY: every accepted original and cancellation operation has a
        // terminal CQE. ManuallyDrop also retains storage if a host callback
        // panics during teardown before reaching this point.
        unsafe {
            ManuallyDrop::drop(&mut self.ring);
            ManuallyDrop::drop(&mut self.shared);
        }
    }
}

struct Handle<G: Reservation, const LINKS: usize, const BYTES: usize> {
    shared: Rc<Shared<G, LINKS, BYTES>>,
    index: usize,
    connection: Connection,
}

impl<G: Reservation, const LINKS: usize, const BYTES: usize> Handle<G, LINKS, BYTES> {
    fn register(&self, cx: &Context<'_>) -> bool {
        if self.shared.slots.borrow()[self.index]
            .waker
            .as_ref()
            .is_some_and(|w| w.will_wake(cx.waker()))
        {
            return false;
        }
        let waker = cx.waker().clone();
        let old = self.shared.slots.borrow_mut()[self.index]
            .waker
            .replace(waker);
        drop(old);
        true
    }
    // Inlining avoids the profiled stack return and wide tag check.
    #[allow(clippy::inline_always)]
    #[inline(always)]
    fn protocol(&self) -> Result<RefMut<'_, Protocol>, Error> {
        let closed = {
            let slots = self.shared.slots.borrow();
            match slots[self.index].protocol.as_ref() {
                Some(protocol) => protocol.closed(),
                None => Some(Error::Closed),
            }
        };
        if let Some(error) = closed {
            return Err(error);
        }
        let callback_exit = CallbackExit::new(&self.shared, self.index);
        let revoked = {
            let reservation = self.shared.reservations[self.index].borrow();
            reservation.as_ref().is_some_and(Reservation::is_revoked)
        };
        drop(callback_exit);
        let mut protocol = RefMut::filter_map(self.shared.slots.borrow_mut(), |slots| {
            slots[self.index].protocol.as_mut()
        })
        .map_err(|_| Error::Closed)?;
        if revoked && protocol.closed().is_none() {
            let _ = protocol.input(Input::Revoked);
        }
        if let Some(error) = protocol.closed() {
            return Err(error);
        }
        Ok(protocol)
    }
}

impl<G: Reservation, const LINKS: usize, const BYTES: usize> Frames for Handle<G, LINKS, BYTES> {
    fn poll_source(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        if self.protocol()?.effect(Lane::Data) == Some(Effect::Provide) {
            return Poll::Ready(Ok(()));
        }
        // Waker clone/drop may drive progress while no table borrow is held.
        if self.register(cx) && self.protocol()?.effect(Lane::Data) == Some(Effect::Provide) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn provide<E: WireCodec>(&mut self, sequence: u64, payload: &E::Payload) -> Result<(), Error> {
        {
            let mut protocol = self.protocol()?;
            if protocol.contract() != Contract::of::<E>()
                || protocol.effect(Lane::Data) != Some(Effect::Provide)
            {
                return Err(Error::Protocol);
            }
            protocol.observe_sequence(sequence);
        }
        // SAFETY: Provide has no active data operation. Until Provided is
        // committed, reentrant progress cannot submit this buffer. This handle
        // retains Shared, and its live slot cannot be recycled during the codec.
        let output = unsafe { &mut *self.shared.buffers[self.index].data.get() };
        let length = transport::encode_frame::<E>(sequence, payload, output)?;
        self.protocol()?
            .input(Input::Provided { sequence, length })?;
        self.shared.ready.borrow_mut().push(self.index);
        Ok(())
    }

    fn poll_frame<E: WireCodec>(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<E::Payload, Error>> {
        for registered in [false, true] {
            let effect = {
                let protocol = self.protocol()?;
                if protocol.contract() != Contract::of::<E>() {
                    return Poll::Ready(Err(Error::Protocol));
                }
                protocol.effect(Lane::Data)
            };
            if let Some(Effect::Admit { length, revision }) = effect {
                // SAFETY: the receive has its terminal CQE. Until Admitted,
                // this live slot cannot submit another data receive or recycle
                // the buffer; this handle retains Shared across the callback.
                let buffer = unsafe { &*self.shared.buffers[self.index].data.get() };
                let decoder = {
                    let callback_exit = CallbackExit::new(&self.shared, self.index);
                    let decoder = {
                        let reservation = self.shared.reservations[self.index].borrow();
                        reservation.as_ref().ok_or(Error::Closed)?.decode_context()
                    };
                    drop(callback_exit);
                    decoder
                };
                let payload = decoder
                    .decode::<E>(&buffer[HEADER_BYTES..HEADER_BYTES + length], revision)
                    .map_err(Error::from)?;
                // A codec may revoke authority or close the driver. Reject its
                // result before endpoint admission, dropping it outside borrows.
                drop(self.protocol()?);
                return Poll::Ready(Ok(payload));
            }
            if registered || !self.register(cx) {
                break;
            }
        }
        Poll::Pending
    }

    fn admitted(&mut self) -> Result<(), Error> {
        self.protocol()?.input(Input::Admitted)?;
        self.shared.ready.borrow_mut().push(self.index);
        Ok(())
    }

    fn poll_endpoint<F: Future>(
        &mut self,
        cx: &mut Context<'_>,
        future: Pin<&mut F>,
    ) -> Poll<Result<F::Output, Error>> {
        self.register(cx);
        drop(self.protocol()?);
        let callback_exit = CallbackExit::new(&self.shared, self.index);
        let result = {
            let reservation = self.shared.reservations[self.index].borrow();
            if let Some(reservation) = reservation.as_ref() {
                match reservation.enter() {
                    Ok(access) => {
                        let result = future.poll(cx).map(Ok);
                        drop(access);
                        result
                    }
                    Err(TopicError::Revoked) => Poll::Ready(Err(Error::Revoked)),
                    Err(error) => Poll::Ready(Err(Error::Topic(error))),
                }
            } else {
                Poll::Ready(Err(Error::Closed))
            }
        };
        drop(callback_exit);
        if let Some(error) = self.protocol().err() {
            Poll::Ready(Err(error))
        } else {
            result
        }
    }

    fn close(&mut self, error: Error) {
        let mut slots = self.shared.slots.borrow_mut();
        let slot = &mut slots[self.index];
        if let Some(protocol) = &mut slot.protocol {
            slot.work.wake |= protocol.closed().is_none();
            let _ = protocol.input(Input::Closed(error));
            slot.contended = false;
            self.shared.ready.borrow_mut().push(self.index);
        }
    }
}

impl<G: Reservation, const LINKS: usize, const BYTES: usize> Drop for Handle<G, LINKS, BYTES> {
    fn drop(&mut self) {
        self.close(Error::Closed);
        let mut slots = self.shared.slots.borrow_mut();
        let slot = &mut slots[self.index];
        slot.alive = false;
        let eligible = slot.idle()
            && slot
                .protocol
                .as_ref()
                .is_some_and(|protocol| protocol.closed().is_some());
        if self.shared.detached.get() && eligible {
            slot.work.reclaim = true;
        }
        let wake = slot.waker.take();
        drop(slots);
        drop(wake);
        self.shared.cleanup_detached_slot(self.index);
    }
}

#[cfg(test)]
#[path = "tests_receive.rs"]
mod tests;
