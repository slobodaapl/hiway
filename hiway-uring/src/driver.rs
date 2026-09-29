use hiway::transport::{
    self, Contract, Direction, Effect, Error, Frames, Input, IoResult, Lane, OpId, Operation,
    Phase, Protocol, Reservation, CREDIT_BYTES, HEADER_BYTES,
};
use hiway::{EventReceiver, EventSender, WireCodec};
use io_uring::{opcode, types, IoUring};
use std::{
    cell::{RefCell, UnsafeCell},
    future::Future,
    io,
    mem::ManuallyDrop,
    os::fd::{AsRawFd, BorrowedFd, OwnedFd},
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

const CANCEL: u64 = 1 << 31;

fn token(id: OpId) -> u64 {
    ((id.generation as u64) << 32) | id.slot as u64
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

struct Slot<G> {
    protocol: Option<Protocol>,
    sockets: Option<(OwnedFd, OwnedFd)>,
    reservation: Option<G>,
    operations: [Operation; 3],
    alive: bool,
    waker: Option<Waker>,
}
impl<G> Slot<G> {
    fn new(index: usize) -> Self {
        Self {
            protocol: None,
            sockets: None,
            reservation: None,
            operations: std::array::from_fn(|lane| Operation::new((index * 3 + lane) as u32)),
            alive: false,
            waker: None,
        }
    }
    fn idle(&self) -> bool {
        self.operations
            .iter()
            .all(|op| op.phase() == Phase::Available)
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
}

struct Shared<G, const LINKS: usize, const BYTES: usize> {
    slots: RefCell<[Slot<G>; LINKS]>,
    ready: RefCell<Ready<LINKS>>,
    buffers: Box<[Buffers<BYTES>]>,
}

/// Single-owner I/O domain. Construction allocates the ring and the entire
/// arena; attachment, polling, submission, completion and removal never grow it.
///
/// This driver is thread-local. The host calls `advance` for bounded progress
/// and may call `wait` separately. Future polling never drives or waits on the
/// ring. All supplied descriptors must be connected stream sockets.
pub struct Driver<G: Reservation = (), const LINKS: usize = 8, const BYTES: usize = 4096> {
    ring: ManuallyDrop<IoUring>,
    shared: ManuallyDrop<Rc<Shared<G, LINKS, BYTES>>>,
    pending_completions: usize,
}

impl<G: Reservation, const LINKS: usize, const BYTES: usize> Driver<G, LINKS, BYTES> {
    /// `entries` includes one submission slot reserved for cancellation.
    /// Buffer bounds include the 38-byte HWY1 header.
    pub fn new(entries: u32) -> io::Result<Self> {
        if LINKS == 0
            || LINKS > (CANCEL as usize) / 3
            || entries < 2
            || BYTES < HEADER_BYTES
            || BYTES > i32::MAX as usize
        {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }
        let cq_entries = LINKS
            .checked_mul(6)
            .and_then(|count| count.max(entries as usize).checked_next_power_of_two())
            .and_then(|count| u32::try_from(count).ok())
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        // Every original operation and its cancellation may complete together.
        let ring = IoUring::builder().setup_cqsize(cq_entries).build(entries)?;
        let mut buffers = Vec::new();
        buffers
            .try_reserve_exact(LINKS)
            .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
        for _ in 0..LINKS {
            buffers.push(Buffers::new());
        }
        Ok(Self {
            ring: ManuallyDrop::new(ring),
            shared: ManuallyDrop::new(Rc::new(Shared {
                slots: RefCell::new(std::array::from_fn(Slot::new)),
                ready: RefCell::new(Ready::new()),
                buffers: buffers.into_boxed_slice(),
            })),
            pending_completions: 0,
        })
    }

    /// Per-link arena bytes to reserve in an accounting adapter.
    pub const fn reservation_bytes() -> usize {
        std::mem::size_of::<Slot<G>>()
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
        reservation
            .check(Contract::of::<E>(), direction, Self::reservation_bytes())
            .map_err(Error::Topic)?;
        drop(reservation.enter().map_err(Error::Topic)?);
        let protocol = Protocol::new(Contract::of::<E>(), direction, BYTES)?;
        let mut slots = self.shared.slots.borrow_mut();
        let index = slots
            .iter()
            .position(|slot| slot.protocol.is_none())
            .ok_or(Error::Capacity)?;
        let slot = &mut slots[index];
        slot.protocol = Some(protocol);
        slot.sockets = Some((data, control));
        slot.reservation = Some(reservation);
        slot.alive = true;
        self.shared.ready.borrow_mut().push(index);
        Ok(Handle {
            shared: Rc::clone(&self.shared),
            index,
        })
    }

    pub fn export<E: WireCodec, R: EventReceiver<E>>(
        &self,
        receiver: R,
        data: impl Into<OwnedFd>,
        control: impl Into<OwnedFd>,
        reservation: G,
    ) -> Result<impl Future<Output = Result<(), Error>>, Error> {
        let io = self.attach::<E>(Direction::Export, data.into(), control.into(), reservation)?;
        Ok(transport::export(receiver, io))
    }

    pub fn import<E: WireCodec, S: EventSender<E>>(
        &self,
        sender: S,
        data: impl Into<OwnedFd>,
        control: impl Into<OwnedFd>,
        reservation: G,
    ) -> Result<impl Future<Output = Result<(), Error>>, Error> {
        let io = self.attach::<E>(Direction::Import, data.into(), control.into(), reservation)?;
        Ok(transport::import(sender, io))
    }

    /// One bounded pass over completions, authority and ready links, followed by nonwaiting
    /// submission. Independent links batch into the same ring.
    pub fn advance(&mut self) -> io::Result<usize> {
        let budget = self.ring.params().cq_entries();
        let mut completed = 0;
        for _ in 0..budget {
            let cqe = self.ring.completion().next();
            let Some(cqe) = cqe else {
                break;
            };
            self.pending_completions -= 1;
            self.complete(cqe.user_data(), cqe.result());
            completed += 1;
        }
        for index in 0..LINKS {
            let mut slots = self.shared.slots.borrow_mut();
            let slot = &mut slots[index];
            if slot
                .reservation
                .as_ref()
                .is_some_and(Reservation::is_revoked)
            {
                if let Some(protocol) = &mut slot.protocol {
                    if protocol.closed().is_none() {
                        let _ = protocol.input(Input::Revoked);
                        self.shared.ready.borrow_mut().push(index);
                    }
                }
            }
        }
        let ready = self.shared.ready.borrow().len;
        // Cleanup gets first use of the reserved SQ space, even under saturation.
        for offset in 0..ready {
            let index = self.shared.ready.borrow().at(offset);
            let mut slots = self.shared.slots.borrow_mut();
            let slot = &mut slots[index];
            if slot.protocol.as_ref().is_some_and(|p| p.closed().is_some()) {
                for operation in &mut slot.operations {
                    operation.cancel();
                    operation.reclaim();
                }
            }
            drop(slots);
            for lane in Lane::ALL {
                self.cancel(index, lane);
            }
        }
        for _ in 0..ready {
            let index = self.shared.ready.borrow_mut().pop();
            for lane in Lane::ALL {
                self.submit(index, lane);
            }
            self.reclaim(index);
            self.wake(index);
        }
        self.ring.submit()?;
        Ok(completed)
    }

    /// Explicit host wait. Do not call this from a future's `poll` method.
    /// Local endpoint readiness still belongs to the host's executor/waker.
    pub fn wait(&mut self) -> io::Result<usize> {
        if self.pending_completions == 0 {
            return Ok(0);
        }
        self.ring.submit_and_wait(1)
    }

    /// Connects CQ notification to a host-owned eventfd/reactor. Register before
    /// entering the host wait; local endpoint wakes remain a separate source.
    pub fn register_eventfd(&self, eventfd: BorrowedFd<'_>) -> io::Result<()> {
        self.ring.submitter().register_eventfd(eventfd.as_raw_fd())
    }

    pub fn active_links(&self) -> usize {
        self.shared
            .slots
            .borrow()
            .iter()
            .filter(|slot| slot.protocol.is_some())
            .count()
    }

    fn complete(&mut self, user_data: u64, result: i32) {
        let index = (user_data as u32 & !(CANCEL as u32)) as usize;
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
        if user_data & CANCEL != 0 {
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
                    -4 | -11 => IoResult::Retry, // EINTR / EAGAIN
                    value if value < 0 => IoResult::Failed(-value),
                    value => IoResult::Bytes(value as usize),
                };
                let _ = protocol.input(Input::Completed {
                    op: id,
                    result,
                    buffer,
                });
            }
        }
        operation.reclaim();
        self.shared.ready.borrow_mut().push(index / 3);
        let wake = slot.waker.take();
        drop(slots);
        if let Some(wake) = wake {
            wake.wake();
        }
    }

    fn cancel(&mut self, index: usize, lane: Lane) {
        let mut slots = self.shared.slots.borrow_mut();
        let operation = &mut slots[index].operations[lane as usize];
        let Some(id) = operation.cancellation() else {
            return;
        };
        let entry = opcode::AsyncCancel::new(token(id))
            .build()
            .user_data(token(id) | CANCEL);
        // SAFETY: cancellation owns no borrowed memory. Its CQE has a separate
        // tag, and the original buffer stays retained through both completions.
        if unsafe { self.ring.submission().push(&entry) }.is_ok() {
            self.pending_completions += 1;
            operation.cancel_accepted(id);
        }
    }

    fn submit(&mut self, index: usize, lane: Lane) {
        let mut slots = self.shared.slots.borrow_mut();
        let slot = &mut slots[index];
        let Some(protocol) = &mut slot.protocol else {
            return;
        };
        let Some(effect) = protocol.effect(lane) else {
            return;
        };
        let (offset, length, receive) = match effect {
            Effect::Receive { offset, length, .. } => (offset, length, true),
            Effect::Transmit { offset, length, .. } => (offset, length, false),
            _ => return,
        };
        let operation = &mut slot.operations[lane as usize];
        if !matches!(operation.phase(), Phase::Available | Phase::Prepared) {
            return;
        }
        let mut sq = self.ring.submission();
        if sq.len() >= sq.capacity() - 1 {
            return;
        }
        let reservation = slot.reservation.as_ref().unwrap();
        let access = match reservation.enter() {
            Ok(access) => access,
            Err(error) => {
                let input = if error == hiway::TopicError::Revoked {
                    Input::Revoked
                } else {
                    Input::Closed(Error::Topic(error))
                };
                let _ = protocol.input(input);
                self.shared.ready.borrow_mut().push(index);
                return;
            }
        };
        let id = if operation.phase() == Phase::Prepared {
            operation.id()
        } else {
            match operation.prepare() {
                Some(id) => id,
                None => {
                    let _ = protocol.input(Input::Closed(Error::Capacity));
                    self.shared.ready.borrow_mut().push(index);
                    return;
                }
            }
        };
        let buffers = &self.shared.buffers[index];
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
        let entry = if receive {
            opcode::Recv::new(fd, pointer, length as u32).build()
        } else {
            opcode::Send::new(fd, pointer.cast_const(), length as u32)
                .flags(0x4000)
                .build() // MSG_NOSIGNAL
        }
        .user_data(token(id));
        // SAFETY: Shared owns stable buffers and descriptors; Driver::drop
        // drains every accepted operation before releasing that ownership.
        if unsafe { sq.push(&entry) }.is_ok() {
            self.pending_completions += 1;
            operation.accepted(id);
            let _ = protocol.input(Input::Accepted { lane, op: id });
        }
        drop(access);
    }

    fn reclaim(&self, index: usize) {
        let mut slots = self.shared.slots.borrow_mut();
        let slot = &mut slots[index];
        if !slot.idle() || slot.protocol.as_ref().is_none_or(|p| p.closed().is_none()) {
            return;
        }
        let reservation = slot.reservation.take();
        let sockets = slot.sockets.take();
        if !slot.alive {
            slot.protocol = None;
        }
        drop(slots);
        drop(sockets);
        drop(reservation);
    }

    fn wake(&self, index: usize) {
        let mut slots = self.shared.slots.borrow_mut();
        let slot = &mut slots[index];
        let work = slot.operations.iter().any(|op| op.cancellation().is_some())
            || slot.protocol.as_ref().is_some_and(|p| {
                Lane::ALL.into_iter().any(|lane| {
                    matches!(
                        slot.operations[lane as usize].phase(),
                        Phase::Available | Phase::Prepared
                    ) && matches!(
                        p.effect(lane),
                        Some(Effect::Receive { .. } | Effect::Transmit { .. })
                    )
                })
            });
        if work {
            self.shared.ready.borrow_mut().push(index);
        }
        let ready = work || slot.protocol.as_ref().is_some_and(|p| p.closed().is_some());
        let wake = if ready { slot.waker.take() } else { None };
        drop(slots);
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}

impl<G: Reservation, const LINKS: usize, const BYTES: usize> Drop for Driver<G, LINKS, BYTES> {
    fn drop(&mut self) {
        for (index, slot) in self.shared.slots.borrow_mut().iter_mut().enumerate() {
            if let Some(protocol) = &mut slot.protocol {
                let _ = protocol.input(Input::Closed(Error::Closed));
                self.shared.ready.borrow_mut().push(index);
            }
        }
        loop {
            let result = self.advance();
            if self.shared.slots.borrow().iter().all(Slot::idle) {
                break;
            }
            if let Err(error) = result.and_then(|_| self.wait()) {
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
}

impl<G: Reservation, const LINKS: usize, const BYTES: usize> Handle<G, LINKS, BYTES> {
    fn register(slot: &mut Slot<G>, cx: &Context<'_>) {
        if !slot.waker.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
            slot.waker = Some(cx.waker().clone());
        }
    }
    fn status(slot: &mut Slot<G>) -> Result<&mut Protocol, Error> {
        let protocol = slot.protocol.as_mut().ok_or(Error::Closed)?;
        if slot
            .reservation
            .as_ref()
            .is_some_and(Reservation::is_revoked)
        {
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
        let mut slots = self.shared.slots.borrow_mut();
        let slot = &mut slots[self.index];
        let protocol = Self::status(slot)?;
        if protocol.effect(Lane::Data) == Some(Effect::Provide) {
            return Poll::Ready(Ok(()));
        }
        Self::register(slot, cx);
        Poll::Pending
    }

    fn provide<E: WireCodec>(&mut self, sequence: u64, payload: &E::Payload) -> Result<(), Error> {
        let mut slots = self.shared.slots.borrow_mut();
        let slot = &mut slots[self.index];
        let protocol = Self::status(slot)?;
        if protocol.contract() != Contract::of::<E>()
            || protocol.effect(Lane::Data) != Some(Effect::Provide)
        {
            return Err(Error::Protocol);
        }
        // SAFETY: Provide has no active data operation; no other handle exists.
        let output = unsafe { &mut *self.shared.buffers[self.index].data.get() };
        let length = transport::encode_frame::<E>(sequence, payload, output)?;
        protocol.input(Input::Provided { sequence, length })?;
        self.shared.ready.borrow_mut().push(self.index);
        Ok(())
    }

    fn poll_frame<E: WireCodec>(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<E::Payload, Error>> {
        let mut slots = self.shared.slots.borrow_mut();
        let slot = &mut slots[self.index];
        let protocol = Self::status(slot)?;
        if protocol.contract() != Contract::of::<E>() {
            return Poll::Ready(Err(Error::Protocol));
        }
        if let Some(Effect::Admit { length, revision }) = protocol.effect(Lane::Data) {
            // SAFETY: Admission follows the terminal data receive completion.
            let buffer = unsafe { &*self.shared.buffers[self.index].data.get() };
            return Poll::Ready(
                E::decode(&buffer[HEADER_BYTES..HEADER_BYTES + length], revision)
                    .map_err(Error::Wire),
            );
        }
        Self::register(slot, cx);
        Poll::Pending
    }

    fn admitted(&mut self) -> Result<(), Error> {
        let mut slots = self.shared.slots.borrow_mut();
        Self::status(&mut slots[self.index])?.input(Input::Admitted)?;
        self.shared.ready.borrow_mut().push(self.index);
        Ok(())
    }

    fn poll_endpoint<F: Future>(
        &mut self,
        cx: &mut Context<'_>,
        future: Pin<&mut F>,
    ) -> Poll<Result<F::Output, Error>> {
        {
            let mut slots = self.shared.slots.borrow_mut();
            let slot = &mut slots[self.index];
            Self::status(slot)?;
            Self::register(slot, cx);
        }
        let slots = self.shared.slots.borrow();
        let reservation = slots[self.index]
            .reservation
            .as_ref()
            .ok_or(Error::Closed)?;
        let _access = reservation.enter().map_err(|error| {
            if error == hiway::TopicError::Revoked {
                Error::Revoked
            } else {
                Error::Topic(error)
            }
        })?;
        future.poll(cx).map(Ok)
    }

    fn close(&mut self, error: Error) {
        let mut slots = self.shared.slots.borrow_mut();
        if let Some(protocol) = &mut slots[self.index].protocol {
            let _ = protocol.input(Input::Closed(error));
            self.shared.ready.borrow_mut().push(self.index);
        }
    }
}

impl<G: Reservation, const LINKS: usize, const BYTES: usize> Drop for Handle<G, LINKS, BYTES> {
    fn drop(&mut self) {
        self.close(Error::Closed);
        let mut slots = self.shared.slots.borrow_mut();
        slots[self.index].alive = false;
        let wake = slots[self.index].waker.take();
        drop(slots);
        drop(wake);
    }
}
