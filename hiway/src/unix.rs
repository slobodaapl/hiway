use std::{
    fmt,
    future::Future,
    io,
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::UnixStream,
    sync::Notify,
};

use crate::{
    DynamicReceiver, DynamicSender, EventId, Grant, Limits, ReceiveError, Rights, SchemaRevision,
    SendError, StreamItem, SubscriptionRole, TopicError, WireCodec, WireError,
};

const HEADER_BYTES: usize = 38;
const CREDIT_BYTES: usize = 9;
const MAGIC: [u8; 4] = *b"HWY1";

use crate::transport::{Accountability, Connection, Contract, Direction, Error, Record};

/// Caller-drained, eight-record history for one host-provisioned Unix link.
/// Clones share the FIFO. The handle survives link completion or cancellation.
/// Access uses a mutex; this Tokio backend is not the strict I/O path.
/// I/O rejection codes use the raw OS error, or zero when none is available.
#[derive(Clone)]
pub struct UnixAccountability(Arc<Mutex<Accountability>>);

impl UnixAccountability {
    fn new<E: WireCodec>(connection: Connection, direction: Direction) -> Self {
        Self(Arc::new(Mutex::new(Accountability::new(
            connection,
            Contract::of::<E>(),
            direction,
        ))))
    }

    /// Removes the oldest retained record.
    ///
    /// # Panics
    /// Panics if the accountability mutex is poisoned.
    #[must_use]
    pub fn pop_record(&self) -> Option<Record> {
        self.0.lock().unwrap().pop()
    }

    /// Returns the number of overwritten records.
    ///
    /// # Panics
    /// Panics if the accountability mutex is poisoned.
    #[must_use]
    pub fn lost_records(&self) -> u64 {
        self.0.lock().unwrap().lost()
    }
}

struct Trace(Option<UnixAccountability>);

impl Trace {
    fn observe(&self, sequence: u64) {
        if let Some(trace) = &self.0 {
            trace.0.lock().unwrap().observe(sequence);
        }
    }
    fn admitted(&self) {
        if let Some(trace) = &self.0 {
            trace.0.lock().unwrap().admitted();
        }
    }
    fn completed(&self, sequence: u64) {
        if let Some(trace) = &self.0 {
            trace.0.lock().unwrap().completed(sequence);
        }
    }
    fn finish(&self, result: &Result<(), IpcError>) {
        if let (Some(trace), Err(error)) = (&self.0, result) {
            let mut trace = trace.0.lock().unwrap();
            match error {
                IpcError::Revoked => trace.cancel(),
                IpcError::Io(error) => trace.finish(Error::Io(error.raw_os_error().unwrap_or(0))),
                IpcError::Topic(error) => trace.finish(Error::Topic(*error)),
                IpcError::Wire(error) => trace.finish(Error::Wire(*error)),
                IpcError::Receive(error) => trace.finish(Error::Receive(*error)),
                IpcError::Send(error) => trace.finish(Error::Send(*error)),
                IpcError::Protocol => trace.finish(Error::Protocol),
                IpcError::Oversized { .. } => trace.finish(Error::Capacity),
                IpcError::Gap { from, to } => trace.finish(Error::Gap {
                    from: *from,
                    to: *to,
                }),
            }
        }
    }
}

impl Drop for Trace {
    fn drop(&mut self) {
        if let Some(trace) = &self.0 {
            trace.0.lock().unwrap().cancel();
        }
    }
}

/// Failure of one pre-authorized, unidirectional IPC link.
#[derive(Debug)]
#[non_exhaustive]
pub enum IpcError {
    /// The socket closed or failed, including during a partial frame.
    Io(io::Error),
    /// The grant lacks authority or an available resource reservation.
    Topic(TopicError),
    /// The event codec rejected a payload or schema revision.
    Wire(WireError),
    /// The local receiving endpoint terminated or exhausted its waiters.
    Receive(ReceiveError),
    /// The local destination rejected admission.
    Send(SendError),
    /// Local authority was revoked. Peer acknowledgement is unnecessary.
    Revoked,
    /// The peer sent an invalid header, sequence, or unsolicited credit.
    Protocol,
    /// An encoded payload exceeds this link's explicit byte bound.
    Oversized {
        /// Declared or encoded payload size.
        length: usize,
        /// Configured payload bound.
        limit: usize,
    },
    /// An observer lost source data. This link terminates without hiding the gap.
    Gap {
        /// First unavailable sequence, inclusive.
        from: u64,
        /// First available sequence, exclusive end of the gap.
        to: u64,
    },
}

impl fmt::Display for IpcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "IPC link I/O error: {error}"),
            Self::Topic(error) => write!(formatter, "IPC topic error: {error}"),
            Self::Wire(error) => write!(formatter, "IPC wire error: {error}"),
            Self::Receive(error) => write!(formatter, "IPC receive error: {error}"),
            Self::Send(error) => write!(formatter, "IPC send error: {error}"),
            Self::Revoked => formatter.write_str("IPC link authority has been revoked"),
            Self::Protocol => formatter.write_str("IPC protocol error"),
            Self::Oversized { length, limit } => write!(
                formatter,
                "IPC payload is too large: {length} bytes exceeds {limit}"
            ),
            Self::Gap { from, to } => {
                write!(formatter, "IPC stream gap from sequence {from} to {to}")
            }
        }
    }
}

impl std::error::Error for IpcError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Topic(error) => Some(error),
            Self::Wire(error) => Some(error),
            Self::Receive(error) => Some(error),
            Self::Send(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for IpcError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<crate::DecodeError> for IpcError {
    fn from(error: crate::DecodeError) -> Self {
        match error {
            crate::DecodeError::Wire(error) => Self::Wire(error),
            crate::DecodeError::Resources(error) => Self::Topic(error),
        }
    }
}

type Driver = Pin<Box<dyn Future<Output = Result<(), IpcError>> + Send>>;

/// Caller-driven link between explicitly authorized endpoints.
///
/// The application provides two already-connected sockets through a protected
/// bootstrap channel. Socket paths, event IDs, and peer-supplied metadata grant
/// no authority. Each link carries exactly one event in one direction.
///
/// One frame may await remote admission. Data and credits use separate sockets;
/// local revocation remains observable even when either socket stops progressing.
/// The configured byte bound covers Hiway buffers, not kernel socket buffers or
/// heap allocations performed by an application codec.
#[must_use = "a link only progresses while its future is polled"]
pub struct UnixLink {
    future: Option<Driver>,
}

impl UnixLink {
    /// Relays one pre-authorized source through its local scoped stream.
    ///
    /// Both legs charge `source`; forwarding cannot switch to another budget.
    /// The host assigns one source-specific scoped stream during bootstrap.
    /// Each process accounts its own resources; no capability travels on the wire.
    /// Incoming credit still means local acceptance, not downstream acceptance.
    /// Failure of either leg closes both. Accepted local data keeps its existing
    /// retention/lifecycle contract. No task is spawned.
    ///
    /// # Errors
    /// Requires publish and selected subscription rights plus both links' resource
    /// reservations. Construction failure releases all partial reservations.
    pub fn relay<E>(
        source: Grant,
        incoming: (UnixStream, UnixStream),
        outgoing: (UnixStream, UnixStream),
        role: SubscriptionRole,
        max_payload: usize,
    ) -> Result<Self, IpcError>
    where
        E: WireCodec + 'static,
        E::Payload: Send + Sync + 'static,
    {
        let export = Self::export::<E>(source.clone(), outgoing.0, outgoing.1, role, max_payload)?;
        let import = Self::import::<E>(source, incoming.0, incoming.1, max_payload)?;
        Ok(Self {
            future: Some(Box::pin(async move {
                tokio::try_join!(import, export)?;
                Ok(())
            })),
        })
    }

    /// Exports a local stream through sockets supplied by the composition root.
    ///
    /// `Observer` cannot backpressure the source and terminates on a visible gap.
    /// Latest-state events replay and coalesce complete snapshots without gaps.
    /// `Required` needs the grant's required-membership right. Local publication
    /// success never implies remote delivery, even when this link is required.
    ///
    /// # Errors
    ///
    /// Returns [`IpcError::Topic`] when the grant is revoked, lacks the selected
    /// receive rights or resource allowance, or cannot bind the event's stream.
    /// Payload bounds exceeding the wire's `u32` length range are rejected.
    /// Failure to reserve the payload buffer returns [`TopicError::Capacity`].
    pub fn export<E>(
        grant: Grant,
        data: UnixStream,
        control: UnixStream,
        role: SubscriptionRole,
        max_payload: usize,
    ) -> Result<Self, IpcError>
    where
        E: WireCodec + 'static,
        E::Payload: Send + Sync + 'static,
    {
        Self::export_io::<E>(grant, data, control, role, max_payload)
    }

    /// Exports with bounded accountability attributed to the supplied host ID
    /// and generation. The host must not reuse that pair within its domain.
    ///
    /// # Errors
    /// Returns the same construction errors as [`Self::export`].
    pub fn export_tracked<E>(
        grant: Grant,
        data: UnixStream,
        control: UnixStream,
        role: SubscriptionRole,
        max_payload: usize,
        connection: Connection,
    ) -> Result<(Self, UnixAccountability), IpcError>
    where
        E: WireCodec + 'static,
        E::Payload: Send + Sync + 'static,
    {
        let trace = UnixAccountability::new::<E>(connection, Direction::Export);
        let link = Self::export_with_trace::<E>(
            grant,
            data,
            control,
            role,
            max_payload,
            Some(trace.clone()),
        )?;
        Ok((link, trace))
    }

    fn export_io<E>(
        grant: Grant,
        data: impl AsyncWrite + Unpin + Send + 'static,
        control: impl AsyncRead + Unpin + Send + 'static,
        role: SubscriptionRole,
        max_payload: usize,
    ) -> Result<Self, IpcError>
    where
        E: WireCodec + 'static,
        E::Payload: Send + Sync + 'static,
    {
        Self::export_with_trace::<E>(grant, data, control, role, max_payload, None)
    }

    fn export_with_trace<E>(
        grant: Grant,
        data: impl AsyncWrite + Unpin + Send + 'static,
        control: impl AsyncRead + Unpin + Send + 'static,
        role: SubscriptionRole,
        max_payload: usize,
        trace: Option<UnixAccountability>,
    ) -> Result<Self, IpcError>
    where
        E: WireCodec + 'static,
        E::Payload: Send + Sync + 'static,
    {
        let rights = match role {
            SubscriptionRole::Observer => Rights::OBSERVE,
            SubscriptionRole::Required => Rights::OBSERVE | Rights::REQUIRED,
        };
        let _operation = grant.enter(E::ID, rights).map_err(IpcError::Topic)?;
        let lease = grant
            .try_charge(link_limits(max_payload)?)
            .map_err(IpcError::Topic)?;
        let receiver = grant.subscribe::<E>(role).map_err(IpcError::Topic)?;
        let buffer = payload_buffer(max_payload)?;
        let trace = Trace(trace);
        let future = async move {
            let _lease = lease;
            let credits = Credits::default();
            let result = tokio::select! {
                biased;
                () = revoked(&grant) => Err(IpcError::Revoked),
                result = receive_credits(control, &credits, &trace) => result,
                result = export_data::<E>(data, receiver, buffer, &credits, &trace) => result,
            };
            trace.finish(&result);
            result
        };
        Ok(Self {
            future: Some(Box::pin(future)),
        })
    }

    /// Imports one wire event into the exact scoped stream named by `grant`.
    ///
    /// Credit returns only after local admission. A blocked destination cannot
    /// prevent local revocation or control-socket closure from ending the link.
    ///
    /// # Errors
    ///
    /// Returns [`IpcError::Topic`] when the grant is revoked, lacks publication
    /// rights or resource allowance, or cannot bind the event's stream.
    /// Payload bounds exceeding the wire's `u32` length range are rejected.
    /// Failure to reserve the payload buffer returns [`TopicError::Capacity`].
    pub fn import<E>(
        grant: Grant,
        data: UnixStream,
        control: UnixStream,
        max_payload: usize,
    ) -> Result<Self, IpcError>
    where
        E: WireCodec + 'static,
        E::Payload: Send + Sync + 'static,
    {
        Self::import_io::<E, _, _>(grant, data, move || control.into_split(), max_payload)
    }

    /// Imports with bounded accountability attributed to the supplied host ID
    /// and generation. The host must not reuse that pair within its domain.
    ///
    /// # Errors
    /// Returns the same construction errors as [`Self::import`].
    pub fn import_tracked<E>(
        grant: Grant,
        data: UnixStream,
        control: UnixStream,
        max_payload: usize,
        connection: Connection,
    ) -> Result<(Self, UnixAccountability), IpcError>
    where
        E: WireCodec + 'static,
        E::Payload: Send + Sync + 'static,
    {
        let trace = UnixAccountability::new::<E>(connection, Direction::Import);
        let link = Self::import_with_trace::<E, _, _>(
            grant,
            data,
            move || control.into_split(),
            max_payload,
            Some(trace.clone()),
        )?;
        Ok((link, trace))
    }

    fn import_io<E, R, W>(
        grant: Grant,
        data: impl AsyncRead + Unpin + Send + 'static,
        split_control: impl FnOnce() -> (R, W) + Send + 'static,
        max_payload: usize,
    ) -> Result<Self, IpcError>
    where
        E: WireCodec + 'static,
        E::Payload: Send + Sync + 'static,
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        Self::import_with_trace::<E, R, W>(grant, data, split_control, max_payload, None)
    }

    fn import_with_trace<E, R, W>(
        grant: Grant,
        data: impl AsyncRead + Unpin + Send + 'static,
        split_control: impl FnOnce() -> (R, W) + Send + 'static,
        max_payload: usize,
        trace: Option<UnixAccountability>,
    ) -> Result<Self, IpcError>
    where
        E: WireCodec + 'static,
        E::Payload: Send + Sync + 'static,
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let _operation = grant
            .enter(E::ID, Rights::PUBLISH)
            .map_err(IpcError::Topic)?;
        let lease = grant
            .try_charge(link_limits(max_payload)?)
            .map_err(IpcError::Topic)?;
        let sender = grant.sender::<E>().map_err(IpcError::Topic)?;
        let buffer = payload_buffer(max_payload)?;
        let trace = Trace(trace);
        let future = async move {
            let _lease = lease;
            let (mut control_read, control_write) = split_control();
            let result = tokio::select! {
                biased;
                () = revoked(&grant) => Err(IpcError::Revoked),
                result = reject_control(&mut control_read) => result,
                result = import_data::<E>(data, control_write, sender, buffer, &grant, &trace) => result,
            };
            trace.finish(&result);
            result
        };
        Ok(Self {
            future: Some(Box::pin(future)),
        })
    }
}

impl Future for UnixLink {
    type Output = Result<(), IpcError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(future) = self.future.as_mut() else {
            return Poll::Ready(Ok(()));
        };
        let result = future.as_mut().poll(context);
        if result.is_ready() {
            self.future = None;
        }
        result
    }
}

impl crate::Link for UnixLink {
    type Error = IpcError;

    fn poll(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Future::poll(Pin::new(self), context)
    }
}

fn link_limits(max_payload: usize) -> Result<Limits, IpcError> {
    if max_payload > u32::MAX as usize {
        return Err(IpcError::Topic(TopicError::InvalidConfig));
    }
    let bytes = max_payload
        .checked_add(HEADER_BYTES + 2 * CREDIT_BYTES)
        .ok_or(IpcError::Topic(TopicError::InvalidConfig))?;
    Ok(Limits {
        connections: 2,
        retained_items: 1,
        waiters: 2,
        bytes,
        ..Limits::ZERO
    })
}

fn payload_buffer(max_payload: usize) -> Result<Vec<u8>, IpcError> {
    let mut buffer = Vec::new();
    buffer
        .try_reserve_exact(max_payload)
        .map_err(|_| IpcError::Topic(TopicError::Capacity))?;
    buffer.resize(max_payload, 0);
    Ok(buffer)
}

async fn revoked(grant: &Grant) {
    loop {
        let changed = grant.revocation_changed().notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if grant.is_revoked() {
            return;
        }
        changed.await;
    }
}

#[derive(Default)]
struct Credits {
    outstanding: AtomicBool,
    sent: AtomicBool,
    sequence: AtomicU64,
    changed: Notify,
}

impl Credits {
    async fn wait(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if !self.outstanding.load(Ordering::Acquire) {
                return;
            }
            changed.await;
        }
    }
}

async fn receive_credits(
    mut control: impl AsyncRead + Unpin,
    credits: &Credits,
    trace: &Trace,
) -> Result<(), IpcError> {
    loop {
        let mut frame = [0; CREDIT_BYTES];
        control.read_exact(&mut frame).await?;
        let sequence = u64::from_le_bytes(frame[1..].try_into().expect("fixed credit field"));
        if frame[0] != 1
            || !credits.outstanding.load(Ordering::Acquire)
            || sequence != credits.sequence.load(Ordering::Acquire)
            || !credits.outstanding.swap(false, Ordering::AcqRel)
        {
            return Err(IpcError::Protocol);
        }
        if credits.sent.load(Ordering::Acquire) {
            trace.completed(sequence);
        }
        credits.changed.notify_one();
        tokio::task::yield_now().await;
    }
}

async fn reject_control(control: &mut (impl AsyncRead + Unpin)) -> Result<(), IpcError> {
    let mut byte = [0];
    match control.read(&mut byte).await? {
        0 => Err(IpcError::Io(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "IPC control socket closed",
        ))),
        _ => Err(IpcError::Protocol),
    }
}

async fn export_data<E>(
    mut data: impl AsyncWrite + Unpin,
    receiver: DynamicReceiver<E>,
    mut buffer: Vec<u8>,
    credits: &Credits,
    trace: &Trace,
) -> Result<(), IpcError>
where
    E: WireCodec + 'static,
    E::Payload: Send + Sync + 'static,
{
    loop {
        credits.wait().await;
        let (sequence, payload) = match receiver.recv().await.map_err(IpcError::Receive)? {
            StreamItem::Data { sequence, value } => (sequence, value),
            StreamItem::Gap { from, to } => return Err(IpcError::Gap { from, to }),
        };
        trace.observe(sequence);
        let length = E::encoded_len(&payload);
        if length > buffer.len() {
            return Err(IpcError::Oversized {
                length,
                limit: buffer.len(),
            });
        }
        let encoded = E::encode(&payload, &mut buffer[..length]).map_err(IpcError::Wire)?;
        if encoded != length {
            return Err(IpcError::Wire(WireError::InvalidPayload));
        }
        let header = encode_header::<E>(sequence, length).map_err(IpcError::Wire)?;
        drop(payload);
        credits.sequence.store(sequence, Ordering::Release);
        credits.sent.store(false, Ordering::Release);
        credits.outstanding.store(true, Ordering::Release);
        trace.admitted();
        data.write_all(&header).await?;
        data.write_all(&buffer[..length]).await?;
        credits.sent.store(true, Ordering::Release);
        if !credits.outstanding.load(Ordering::Acquire) {
            trace.completed(sequence);
        }
        tokio::task::yield_now().await;
    }
}

async fn import_data<E>(
    mut data: impl AsyncRead + Unpin,
    mut control: impl AsyncWrite + Unpin,
    sender: DynamicSender<E>,
    mut buffer: Vec<u8>,
    grant: &Grant,
    trace: &Trace,
) -> Result<(), IpcError>
where
    E: WireCodec + 'static,
    E::Payload: Send + Sync + 'static,
{
    let mut previous: Option<u64> = None;
    loop {
        let mut header = [0; HEADER_BYTES];
        data.read_exact(&mut header).await?;
        trace.observe(u64::from_le_bytes(
            header[26..34].try_into().expect("fixed sequence field"),
        ));
        let (sequence, revision, length) = decode_header::<E>(&header, buffer.len())?;
        if previous.is_some_and(|last| !E::DELIVERY.follows(last, sequence)) {
            return Err(IpcError::Protocol);
        }
        data.read_exact(&mut buffer[..length]).await?;
        let payload =
            E::decode_with_resources(&buffer[..length], revision, grant).map_err(IpcError::from)?;
        sender
            .send(payload)
            .await
            .map_err(|error| IpcError::Send(error.map(|_| ())))?;
        previous = Some(sequence);
        trace.admitted();
        let mut credit = [1; CREDIT_BYTES];
        credit[1..].copy_from_slice(&sequence.to_le_bytes());
        control.write_all(&credit).await?;
        trace.completed(sequence);
        tokio::task::yield_now().await;
    }
}

fn encode_header<E: WireCodec>(
    sequence: u64,
    length: usize,
) -> Result<[u8; HEADER_BYTES], WireError> {
    let length = u32::try_from(length).map_err(|_| WireError::InvalidHeader)?;
    let mut header = [0; HEADER_BYTES];
    header[..4].copy_from_slice(&MAGIC);
    header[4..20].copy_from_slice(&E::ID.as_u128().to_le_bytes());
    header[20..22].copy_from_slice(&E::WIRE_MAJOR.0.to_le_bytes());
    header[22..26].copy_from_slice(&E::SCHEMA_REVISION.0.to_le_bytes());
    header[26..34].copy_from_slice(&sequence.to_le_bytes());
    header[34..].copy_from_slice(&length.to_le_bytes());
    Ok(header)
}

fn decode_header<E: WireCodec>(
    header: &[u8; HEADER_BYTES],
    max_payload: usize,
) -> Result<(u64, SchemaRevision, usize), IpcError> {
    let event = EventId::from_u128(u128::from_le_bytes(
        header[4..20].try_into().expect("fixed event field"),
    ));
    let major = u16::from_le_bytes(header[20..22].try_into().expect("fixed major field"));
    if header[..4] != MAGIC || event != E::ID || major != E::WIRE_MAJOR.0 {
        return Err(IpcError::Protocol);
    }
    let revision = SchemaRevision(u32::from_le_bytes(
        header[22..26].try_into().expect("fixed revision field"),
    ));
    let sequence = u64::from_le_bytes(header[26..34].try_into().expect("fixed sequence field"));
    let length = u32::from_le_bytes(header[34..].try_into().expect("fixed length field")) as usize;
    if length > max_payload {
        return Err(IpcError::Oversized {
            length,
            limit: max_payload,
        });
    }
    Ok((sequence, revision, length))
}

#[cfg(test)]
#[path = "tests_unix.rs"]
mod tests;
