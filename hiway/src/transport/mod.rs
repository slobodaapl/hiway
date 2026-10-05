//! Heapless stream protocol and concrete future adapters.
//!
//! The host owns storage, interprets effects, and reports accepted operations
//! and terminal completions. Polling an endpoint never waits for socket I/O.

use crate::{
    EventId, EventReceiver, EventSender, EventSpec, ReceiveError, SchemaRevision, SendError,
    StreamItem, TopicError, WireCodec, WireError, WireMajor,
};
use core::{
    future::{poll_fn, Future},
    pin::{pin, Pin},
    task::{Context, Poll},
};

mod operation;
pub use operation::{OpId, Operation, Phase};
mod protocol;
pub use protocol::{Direction, Effect, Input, IoResult, Lane, Protocol};
#[cfg(feature = "std")]
mod grant;
#[cfg(feature = "std")]
pub use grant::{GrantAccess, GrantReservation};

pub const HEADER_BYTES: usize = 38;
pub const CREDIT_BYTES: usize = 9;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Closed,
    Revoked,
    Protocol,
    Capacity,
    Io(i32),
    Wire(WireError),
    Topic(TopicError),
    Receive(ReceiveError),
    Send(SendError),
    Gap { from: u64, to: u64 },
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "transport: {self:?}")
    }
}
impl core::error::Error for Error {}

/// Event contract fixed by the composition root, never by peer bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Contract {
    pub event: EventId,
    pub major: WireMajor,
    pub revision: SchemaRevision,
}
impl Contract {
    #[must_use]
    pub const fn of<E: EventSpec>() -> Self {
        Self {
            event: E::ID,
            major: E::WIRE_MAJOR,
            revision: E::SCHEMA_REVISION,
        }
    }
}

/// Opaque resource reservation retained by a backend until reclamation.
/// `enter` brackets authorization of new work; its guard establishes the
/// cutoff against concurrent revocation. Guard callbacks belong to the host.
pub trait Reservation {
    type Access<'a>
    where
        Self: 'a;
    /// Checks the contract, direction and reserved storage size.
    ///
    /// # Errors
    /// Returns the adapter's authority or resource rejection.
    fn check(
        &self,
        contract: Contract,
        direction: Direction,
        bytes: usize,
    ) -> Result<(), TopicError>;
    /// Enters authorized work until the returned guard is dropped.
    ///
    /// # Errors
    /// Returns the adapter's authority rejection, including revocation.
    fn enter(&self) -> Result<Self::Access<'_>, TopicError>;
    fn is_revoked(&self) -> bool;
}

/// Caller-owned static endpoints need no dynamic grant or accounting pool.
impl Reservation for () {
    type Access<'a> = ();
    fn check(&self, _: Contract, _: Direction, _: usize) -> Result<(), TopicError> {
        Ok(())
    }
    fn enter(&self) -> Result<(), TopicError> {
        Ok(())
    }
    fn is_revoked(&self) -> bool {
        false
    }
}

/// Backend access used by the endpoint futures. Implementations retain a
/// closing record on drop; dropping a future does not reclaim submitted I/O.
pub trait Frames {
    fn poll_source(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>>;
    /// Provides the next export frame.
    ///
    /// # Errors
    /// Returns encoding, capacity or lifecycle rejection.
    fn provide<E: WireCodec>(&mut self, sequence: u64, payload: &E::Payload) -> Result<(), Error>;
    fn poll_frame<E: WireCodec>(&mut self, cx: &mut Context<'_>)
        -> Poll<Result<E::Payload, Error>>;
    /// Records local admission before credit can be sent.
    ///
    /// # Errors
    /// Returns protocol or lifecycle rejection.
    fn admitted(&mut self) -> Result<(), Error>;
    /// Polls one endpoint operation under authority, releasing access before
    /// returning, including when the operation parks.
    fn poll_endpoint<F: Future>(
        &mut self,
        cx: &mut Context<'_>,
        future: Pin<&mut F>,
    ) -> Poll<Result<F::Output, Error>>;
    fn close(&mut self, error: Error);
}

/// Exports through any typed receiver, without boxing its receive future.
///
/// # Errors
/// Returns receiver, framing, I/O, gap or lifecycle failures and closes the link.
pub async fn export<E: WireCodec, R: EventReceiver<E>, T: Frames>(
    receiver: R,
    mut io: T,
) -> Result<(), Error> {
    let result = async {
        loop {
            poll_fn(|cx| io.poll_source(cx)).await?;
            let item = {
                let mut receive = pin!(async { receiver.event_recv().await });
                poll_fn(|cx| io.poll_endpoint(cx, receive.as_mut())).await?
            }
            .map_err(Error::Receive)?;
            match item {
                StreamItem::Data { sequence, value } => io.provide::<E>(sequence, &value)?,
                StreamItem::Gap { from, to } => return Err(Error::Gap { from, to }),
            }
        }
    }
    .await;
    if let Err(error) = result {
        io.close(error);
    }
    result
}

/// Credit follows successful local admission, including when admission parks.
///
/// # Errors
/// Returns admission, framing, I/O or lifecycle failures and closes the link.
pub async fn import<E: WireCodec, S: EventSender<E>, T: Frames>(
    sender: S,
    mut io: T,
) -> Result<(), Error> {
    let result = async {
        loop {
            let payload = poll_fn(|cx| io.poll_frame::<E>(cx)).await?;
            {
                let mut send = pin!(async { sender.send(payload).await });
                poll_fn(|cx| io.poll_endpoint(cx, send.as_mut())).await?
            }
            .map_err(|error| Error::Send(error.map(|_| ())))?;
            io.admitted()?;
        }
    }
    .await;
    if let Err(error) = result {
        io.close(error);
    }
    result
}

/// Encodes a payload and its HWY1 header into caller-owned storage.
///
/// # Errors
/// Returns `Capacity` if the frame does not fit, or `Wire` if payload encoding fails.
pub fn encode_frame<E: WireCodec>(
    sequence: u64,
    payload: &E::Payload,
    output: &mut [u8],
) -> Result<usize, Error> {
    let length = E::encoded_len(payload);
    let total = HEADER_BYTES.checked_add(length).ok_or(Error::Capacity)?;
    if total > output.len() {
        return Err(Error::Capacity);
    }
    let wire_length = u32::try_from(length).map_err(|_| Error::Capacity)?;
    if E::encode(payload, &mut output[HEADER_BYTES..total]).map_err(Error::Wire)? != length {
        return Err(Error::Wire(WireError::InvalidPayload));
    }
    output[..4].copy_from_slice(b"HWY1");
    output[4..20].copy_from_slice(&E::ID.as_u128().to_le_bytes());
    output[20..22].copy_from_slice(&E::WIRE_MAJOR.0.to_le_bytes());
    output[22..26].copy_from_slice(&E::SCHEMA_REVISION.0.to_le_bytes());
    output[26..34].copy_from_slice(&sequence.to_le_bytes());
    output[34..38].copy_from_slice(&wire_length.to_le_bytes());
    Ok(total)
}
