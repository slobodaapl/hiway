#![forbid(unsafe_code)]

#[cfg(unix)]
use std::{
    collections::HashMap,
    env,
    io::{self, ErrorKind, Read, Write},
    os::unix::net::{UnixListener, UnixStream},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, MutexGuard, PoisonError,
    },
    thread,
};

#[cfg(unix)]
use hiway::{Broker, BrokerError, ClientId, EnvelopeHeader, UnixFrame, WireEnvelope};

#[cfg(unix)]
const DEFAULT_SOCKET: &str = "/tmp/hiwayd.sock";
#[cfg(unix)]
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

#[cfg(unix)]
struct DaemonState {
    broker: Broker,
    writers: HashMap<ClientId, Arc<Mutex<UnixStream>>>,
}

#[cfg(unix)]
fn main() -> io::Result<()> {
    let socket = env::args()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_SOCKET.to_owned());
    let listener = UnixListener::bind(&socket)?;
    let state = Arc::new(Mutex::new(DaemonState {
        broker: Broker::new(),
        writers: HashMap::new(),
    }));
    let next_client = AtomicU64::new(1);

    for connection in listener.incoming() {
        let stream = connection?;
        let writer = Arc::new(Mutex::new(stream.try_clone()?));
        let client = ClientId(next_client.fetch_add(1, Ordering::Relaxed));
        lock(&state).writers.insert(client, writer);

        let state_for_client = Arc::clone(&state);
        thread::spawn(move || {
            if let Err(error) = serve_client(client, stream, &state_for_client) {
                eprintln!("hiwayd client {client:?}: {error}");
            }
            let mut state = lock(&state_for_client);
            state.broker.remove_client(client);
            state.writers.remove(&client);
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn main() {
    eprintln!("hiwayd requires a Unix-domain socket platform");
}

#[cfg(unix)]
fn serve_client(
    client: ClientId,
    mut stream: UnixStream,
    state: &Arc<Mutex<DaemonState>>,
) -> io::Result<()> {
    while let Some(body) = read_packet(&mut stream)? {
        let frame = UnixFrame::decode(&body)
            .map_err(|_| io::Error::new(ErrorKind::InvalidData, "invalid Unix frame"))?;
        match frame {
            UnixFrame::Subscribe(route) => lock(state).broker.subscribe(route, client),
            UnixFrame::Unsubscribe(route) => lock(state).broker.unsubscribe(route, client),
            UnixFrame::Publish { header, payload } => publish(state, header, payload)?,
        }
    }
    Ok(())
}

#[cfg(unix)]
fn publish(
    state: &Arc<Mutex<DaemonState>>,
    header: EnvelopeHeader,
    payload: Vec<u8>,
) -> io::Result<()> {
    if header.payload_len as usize != payload.len() {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "payload length does not match envelope header",
        ));
    }
    let envelope = WireEnvelope {
        event: header.event,
        wire_major: header.wire_major,
        schema_revision: header.schema_revision,
        metadata: header.metadata,
        payload: &payload,
    };
    let routed_result = {
        let mut state = lock(state);
        state.broker.route_snapshot(envelope)
    };
    let (routed, clients) = match routed_result {
        Ok(result) => result,
        Err(BrokerError::Expired) => return Ok(()),
        Err(BrokerError::OriginCapacity) => {
            return Err(io::Error::new(
                ErrorKind::ResourceBusy,
                "broker origin table is full",
            ));
        }
    };
    let Some(clients) = clients else {
        return Ok(());
    };

    let header = EnvelopeHeader::from_envelope(&routed)
        .map_err(|_| io::Error::new(ErrorKind::InvalidData, "invalid envelope header"))?;
    let packet = encode_packet(&UnixFrame::Publish {
        header,
        payload: payload.clone(),
    })?;

    let writers = {
        let state = lock(state);
        clients
            .iter()
            .filter_map(|client| state.writers.get(client).cloned())
            .collect::<Vec<_>>()
    };
    for writer in writers {
        let mut writer = lock(&writer);
        writer.write_all(&packet)?;
    }
    Ok(())
}

#[cfg(unix)]
fn encode_packet(frame: &UnixFrame) -> io::Result<Vec<u8>> {
    let body = frame
        .encode()
        .map_err(|_| io::Error::new(ErrorKind::InvalidData, "invalid Unix frame"))?;
    if body.len() > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "Unix frame is too large",
        ));
    }
    let length = u32::try_from(body.len())
        .map_err(|_| io::Error::new(ErrorKind::InvalidData, "Unix frame is too large"))?;
    let mut packet = Vec::with_capacity(body.len() + 4);
    packet.extend_from_slice(&length.to_be_bytes());
    packet.extend_from_slice(&body);
    Ok(packet)
}

#[cfg(unix)]
fn read_packet(stream: &mut UnixStream) -> io::Result<Option<Vec<u8>>> {
    let mut length = [0; 4];
    match stream.read_exact(&mut length) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "Unix frame is too large",
        ));
    }
    let mut body = vec![0; length];
    stream.read_exact(&mut body)?;
    Ok(Some(body))
}

#[cfg(unix)]
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
