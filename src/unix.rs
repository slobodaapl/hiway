use std::{
    collections::VecDeque,
    io::{self, ErrorKind, Read, Write},
    os::unix::net::UnixStream,
    path::Path,
    task::{Context, Poll},
};

use crate::{
    Broker, BrokerError, ClientId, EnvelopeHeader, EventId, RouteKey, WireEnvelope, WireError,
};

const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// Unix-link control or publication frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UnixFrame {
    /// Add one remote subscription.
    Subscribe(RouteKey),
    /// Remove one remote subscription.
    Unsubscribe(RouteKey),
    /// Publish one opaque encoded payload.
    Publish {
        /// Encoded envelope header.
        header: EnvelopeHeader,
        /// Encoded payload bytes.
        payload: std::vec::Vec<u8>,
    },
}

impl UnixFrame {
    /// Encodes one frame body. The stream wrapper adds a big-endian length.
    pub fn encode(&self) -> Result<std::vec::Vec<u8>, WireError> {
        let mut output = std::vec::Vec::new();
        match self {
            Self::Subscribe(route) => {
                output.push(1);
                put_route(&mut output, *route);
            }
            Self::Unsubscribe(route) => {
                output.push(2);
                put_route(&mut output, *route);
            }
            Self::Publish { header, payload } => {
                let payload_len =
                    u32::try_from(payload.len()).map_err(|_| WireError::InvalidHeader)?;
                if header.payload_len != payload_len {
                    return Err(WireError::InvalidHeader);
                }
                output.push(3);
                let mut encoded = [0; crate::ENVELOPE_HEADER_BYTES];
                header.encode(&mut encoded)?;
                output.extend_from_slice(&encoded);
                output.extend_from_slice(payload);
            }
        }
        Ok(output)
    }

    /// Decodes one frame body without allocating a payload for control frames.
    pub fn decode(input: &[u8]) -> Result<Self, WireError> {
        if input.len() > MAX_FRAME_BYTES {
            return Err(WireError::InvalidHeader);
        }
        let Some(kind) = input.first().copied() else {
            return Err(WireError::Truncated);
        };
        match kind {
            1 => Ok(Self::Subscribe(read_route(&input[1..])?)),
            2 => Ok(Self::Unsubscribe(read_route(&input[1..])?)),
            3 => {
                if input.len() < 1 + crate::ENVELOPE_HEADER_BYTES {
                    return Err(WireError::Truncated);
                }
                let header = EnvelopeHeader::decode(&input[1..1 + crate::ENVELOPE_HEADER_BYTES])?;
                let payload = &input[1 + crate::ENVELOPE_HEADER_BYTES..];
                if payload.len() != header.payload_len as usize {
                    return Err(WireError::InvalidHeader);
                }
                Ok(Self::Publish {
                    header,
                    payload: payload.to_vec(),
                })
            }
            _ => Err(WireError::InvalidHeader),
        }
    }
}

fn put_route(output: &mut std::vec::Vec<u8>, route: RouteKey) {
    output.extend_from_slice(&route.event.as_u128().to_le_bytes());
    output.extend_from_slice(&route.wire_major.0.to_le_bytes());
}

fn read_route(input: &[u8]) -> Result<RouteKey, WireError> {
    if input.len() != 18 {
        return Err(WireError::InvalidHeader);
    }
    Ok(RouteKey {
        event: EventId::from_u128(u128::from_le_bytes(
            input[..16].try_into().expect("route ID length"),
        )),
        wire_major: crate::WireMajor(u16::from_le_bytes(
            input[16..].try_into().expect("route major length"),
        )),
    })
}

/// Pollable Unix stream link. The link only transports opaque frames; the
/// daemon may route IDs it has never seen in Rust code.
pub struct UnixLink {
    stream: UnixStream,
    incoming: std::vec::Vec<u8>,
    outgoing: VecDeque<std::vec::Vec<u8>>,
    received: VecDeque<UnixFrame>,
    closed: bool,
}

impl UnixLink {
    /// Connects to a Unix-domain socket and switches it to nonblocking mode.
    pub fn connect(path: impl AsRef<Path>) -> io::Result<Self> {
        let stream = UnixStream::connect(path)?;
        Self::from_stream(stream)
    }

    /// Wraps an accepted Unix stream.
    pub fn from_stream(stream: UnixStream) -> io::Result<Self> {
        stream.set_nonblocking(true)?;
        Ok(Self {
            stream,
            incoming: std::vec::Vec::new(),
            outgoing: VecDeque::new(),
            received: VecDeque::new(),
            closed: false,
        })
    }

    /// Queues one frame for transmission.
    pub fn queue(&mut self, frame: &UnixFrame) -> Result<(), WireError> {
        let body = frame.encode()?;
        if body.len() > MAX_FRAME_BYTES {
            return Err(WireError::InvalidHeader);
        }
        let length = u32::try_from(body.len()).map_err(|_| WireError::InvalidHeader)?;
        let mut packet = std::vec::Vec::with_capacity(body.len() + 4);
        packet.extend_from_slice(&length.to_be_bytes());
        packet.extend_from_slice(&body);
        self.outgoing.push_back(packet);
        Ok(())
    }

    /// Reads one complete frame when the stream is currently readable.
    pub fn try_recv(&mut self) -> io::Result<Option<UnixFrame>> {
        if self.closed && self.incoming.is_empty() {
            return Err(io::Error::new(ErrorKind::UnexpectedEof, "Unix link closed"));
        }
        let mut buffer = [0; 8192];
        loop {
            match self.stream.read(&mut buffer) {
                Ok(0) => {
                    self.closed = true;
                    break;
                }
                Ok(read) => self.incoming.extend_from_slice(&buffer[..read]),
                Err(error) if error.kind() == ErrorKind::WouldBlock => break,
                Err(error) => return Err(error),
            }
        }
        if self.incoming.len() < 4 {
            if self.closed {
                return Err(io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "truncated Unix frame",
                ));
            }
            return Ok(None);
        }
        let length = u32::from_be_bytes(
            self.incoming[..4]
                .try_into()
                .expect("length prefix has four bytes"),
        ) as usize;
        if length > MAX_FRAME_BYTES {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "Unix frame is too large",
            ));
        }
        if self.incoming.len() < length + 4 {
            if self.closed {
                return Err(io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "truncated Unix frame",
                ));
            }
            return Ok(None);
        }
        let body = self.incoming[4..length + 4].to_vec();
        self.incoming.drain(..length + 4);
        UnixFrame::decode(&body)
            .map(Some)
            .map_err(|_| io::Error::new(ErrorKind::InvalidData, "invalid Unix frame"))
    }

    /// Returns a frame observed by [`crate::Link::poll`].
    pub fn take_received(&mut self) -> Option<UnixFrame> {
        self.received.pop_front()
    }

    fn next_frame(&mut self) -> io::Result<Option<UnixFrame>> {
        if let Some(frame) = self.received.pop_front() {
            return Ok(Some(frame));
        }
        self.try_recv()
    }

    fn flush(&mut self) -> io::Result<()> {
        while let Some(packet) = self.outgoing.front_mut() {
            while !packet.is_empty() {
                match self.stream.write(packet) {
                    Ok(0) => return Err(io::Error::new(ErrorKind::WriteZero, "Unix link closed")),
                    Ok(written) => {
                        packet.drain(..written);
                    }
                    Err(error) if error.kind() == ErrorKind::WouldBlock => return Ok(()),
                    Err(error) => return Err(error),
                }
            }
            let _ = self.outgoing.pop_front();
        }
        Ok(())
    }

    /// Applies one incoming frame to an opaque broker.
    pub fn apply_to_broker(
        &mut self,
        broker: &mut Broker,
        client: ClientId,
        deliver: impl FnMut(ClientId, WireEnvelope<'_>),
    ) -> io::Result<()> {
        let Some(frame) = self.next_frame()? else {
            return Ok(());
        };
        match frame {
            UnixFrame::Subscribe(route) => broker.subscribe(route, client),
            UnixFrame::Unsubscribe(route) => broker.unsubscribe(route, client),
            UnixFrame::Publish { header, payload } => {
                let envelope = WireEnvelope {
                    event: header.event,
                    wire_major: header.wire_major,
                    schema_revision: header.schema_revision,
                    metadata: header.metadata,
                    payload: &payload,
                };
                broker
                    .route(envelope, deliver)
                    .map_err(|error| match error {
                        BrokerError::Expired => {
                            io::Error::new(ErrorKind::InvalidData, "expired envelope")
                        }
                        BrokerError::OriginCapacity => {
                            io::Error::new(ErrorKind::ResourceBusy, "broker origin table is full")
                        }
                    })?;
            }
        }
        Ok(())
    }
}

impl crate::Link for UnixLink {
    type Error = io::Error;

    fn poll(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if !self.received.is_empty() {
            return Poll::Ready(Ok(()));
        }
        if let Err(error) = self.flush() {
            return Poll::Ready(Err(error));
        }
        match self.try_recv() {
            Ok(Some(frame)) => {
                self.received.push_back(frame);
                Poll::Ready(Ok(()))
            }
            Ok(None) => Poll::Pending,
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}
