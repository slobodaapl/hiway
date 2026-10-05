//! Separate-process Unix admission and unrelated local-flow measurements.
//! The raw peer receives only connected data/control sockets through stdio.
//! Optional `--sandbox-peer` uses installed bubblewrap; no one-way latency claim.

use std::{
    alloc::System,
    future::Future,
    io::Read,
    net::{TcpListener, TcpStream},
    os::fd::{AsFd, OwnedFd},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use hiway::{
    events, DynamicFabric, EventSpec, Grant, IpcError, Limits, Permission, Rights, SchemaRevision,
    StreamConfig, StreamItem, StreamLimits, SubscriptionRole, UnixLink, WireCodec, WireError,
};
use stats_alloc::{Region, Stats, StatsAlloc, INSTRUMENTED_SYSTEM};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

const REJECTED_PHASES: [&str; 3] = ["oversized", "forged_event", "unsolicited_control"];

#[events]
enum Events {
    Healthy(u64),
    Noise(u64),
}

impl WireCodec for events::Noise {
    fn encoded_len(_: &u64) -> usize {
        8
    }

    fn encode(value: &u64, output: &mut [u8]) -> std::result::Result<usize, WireError> {
        output
            .get_mut(..8)
            .ok_or(WireError::BufferTooSmall)?
            .copy_from_slice(&value.to_le_bytes());
        Ok(8)
    }

    fn decode(bytes: &[u8], revision: SchemaRevision) -> std::result::Result<u64, WireError> {
        if revision != SchemaRevision(1) {
            return Err(WireError::UnsupportedRevision);
        }
        Ok(u64::from_le_bytes(
            bytes.try_into().map_err(|_| WireError::InvalidPayload)?,
        ))
    }
}

struct Peer {
    child: Child,
    _probe: Option<TcpListener>,
}

impl Peer {
    fn spawn(phase: &str, data: UnixStream, control: UnixStream, sandbox: bool) -> Result<Self> {
        let executable = std::env::current_exe()?;
        let probe = sandbox
            .then(|| TcpListener::bind("127.0.0.1:0"))
            .transpose()?;
        let mut command = if sandbox {
            let mut command = Command::new("bwrap");
            command
                .args([
                    "--unshare-all",
                    "--die-with-parent",
                    "--new-session",
                    "--cap-drop",
                    "ALL",
                    "--clearenv",
                    "--setenv",
                    "PATH",
                    "/usr/bin",
                    "--ro-bind",
                    "/usr",
                    "/usr",
                    "--symlink",
                    "usr/lib",
                    "/lib",
                    "--symlink",
                    "usr/lib",
                    "/lib64",
                    "--proc",
                    "/proc",
                    "--dev",
                    "/dev",
                    "--ro-bind",
                ])
                .arg(&executable)
                .args(["/app", "--chdir", "/", "--", "/app"]);
            command
        } else {
            Command::new(&executable)
        };
        command.args(["--peer", phase]);
        if let Some(listener) = &probe {
            command
                .arg("--sandbox-check")
                .arg(&executable)
                .arg(listener.local_addr()?.to_string());
        }
        Ok(Self {
            child: command
                .stdin(Stdio::from(OwnedFd::from(data.into_std()?)))
                .stdout(Stdio::from(OwnedFd::from(control.into_std()?)))
                .stderr(Stdio::piped())
                .spawn()?,
            _probe: probe,
        })
    }

    async fn finish(&mut self) -> Result {
        loop {
            if let Some(status) = self.child.try_wait()? {
                let mut report = String::new();
                self.child
                    .stderr
                    .take()
                    .ok_or("missing peer report pipe")?
                    .read_to_string(&mut report)?;
                print!("{report}");
                if !status.success() {
                    return Err("peer failed".into());
                }
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        if !matches!(self.child.try_wait(), Ok(Some(_))) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn grant<E: EventSpec>(fabric: &DynamicFabric, event: StreamLimits, links: usize) -> Result<Grant> {
    Ok(fabric.grant(
        &[
            Permission::new::<E>(Rights::PUBLISH | Rights::OBSERVE | Rights::REQUIRED)
                .with_limits(event),
        ],
        Limits {
            streams: 1,
            retained_items: event.retained_items + links,
            subscriptions: event.subscriptions,
            waiters: event.waiters + 2 * links,
            connections: 2 * links,
            bytes: 64 * links,
            ..Limits::ZERO
        },
    )?)
}

fn cpu_ticks() -> Result<u64> {
    let stat = std::fs::read_to_string("/proc/self/stat")?;
    let fields = stat
        .rsplit_once(')')
        .ok_or("invalid process stat")?
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    let user = fields
        .get(11)
        .ok_or("missing user CPU time")?
        .parse::<u64>()?;
    let system = fields
        .get(12)
        .ok_or("missing system CPU time")?
        .parse::<u64>()?;
    Ok(user + system)
}

fn ticks_per_second() -> Result<u32> {
    let output = Command::new("getconf").arg("CLK_TCK").output()?;
    if !output.status.success() {
        return Err("getconf CLK_TCK failed".into());
    }
    let ticks = std::str::from_utf8(&output.stdout)?.trim().parse()?;
    if ticks == 0 {
        return Err("zero CPU tick frequency".into());
    }
    Ok(ticks)
}

struct Measurement {
    elapsed: Duration,
    ticks: u64,
    allocations: Stats,
    completed: u64,
    samples: Vec<u128>,
}

impl Measurement {
    fn report(&mut self, side: &str, phase: &str, hz: u32) -> Result {
        self.samples.sort_unstable();
        let percentile = |percent: usize| {
            if self.samples.is_empty() {
                0
            } else {
                self.samples[(self.samples.len() * percent).div_ceil(100) - 1]
            }
        };
        let operations = u32::try_from(self.completed)?;
        let row = format!(
            "{side},{phase},{operations},{},{:.2},{},{hz},{},{},{},{},{},{},{}",
            self.elapsed.as_nanos(),
            f64::from(operations) / self.elapsed.as_secs_f64(),
            self.ticks,
            self.samples.len(),
            percentile(50),
            percentile(95),
            percentile(99),
            self.allocations.allocations,
            self.allocations.bytes_allocated,
            self.allocations.reallocations
        );
        if side == "peer_acceptance_rtt" {
            eprintln!("{row}");
        } else {
            println!("{row}");
        }
        Ok(())
    }
}

async fn with_link<T>(link: &mut UnixLink, action: impl Future<Output = Result<T>>) -> Result<T> {
    tokio::select! {
        result = action => result,
        result = link => { result?; Err("link ended during measurement".into()) },
    }
}

async fn measure_local(healthy: &Grant, phase: &str, operations: usize) -> Result<Measurement> {
    let sender = healthy.sender::<events::Healthy>()?;
    let receiver = healthy.subscribe::<events::Healthy>(SubscriptionRole::Required)?;
    let mut samples = Vec::with_capacity(operations);
    for index in 0..1000 {
        sender.send(index).await?;
        match receiver.recv().await? {
            StreamItem::Data { value, .. } if *value == index => {}
            _ => return Err("local warmup lost data".into()),
        }
        tokio::task::yield_now().await;
    }
    let ticks = cpu_ticks()?;
    let region = Region::new(GLOBAL);
    let start = Instant::now();
    if phase == "idle" {
        tokio::time::sleep(Duration::from_secs(1)).await;
    } else {
        for index in 0..operations {
            let sent = 1000 + index as u64;
            let operation = Instant::now();
            sender.send(sent).await?;
            tokio::task::yield_now().await;
            match receiver.recv().await? {
                StreamItem::Data { sequence, value } if sequence == sent && *value == sent => {}
                _ => return Err("local probe lost, reordered, or changed data".into()),
            }
            samples.push(operation.elapsed().as_nanos());
        }
    }
    let elapsed = start.elapsed();
    let allocations = region.change();
    Ok(Measurement {
        elapsed,
        allocations,
        completed: samples.len() as u64,
        samples,
        ticks: cpu_ticks()? - ticks,
    })
}

async fn run(phase: &str, operations: usize, hz: u32, sandbox: bool) -> Result {
    let fabric = DynamicFabric::new();
    let config = StreamConfig {
        capacity: 8,
        subscribers: 1,
        waiters: 4,
    };
    fabric.create_stream::<events::Healthy>(config)?;
    fabric.create_stream::<events::Noise>(config)?;
    let event = StreamLimits {
        retained_items: 8,
        subscriptions: 1,
        waiters: 2,
    };
    let healthy = grant::<events::Healthy>(&fabric, event, 0)?;
    let noise = grant::<events::Noise>(&fabric, event, 1)?;
    let role = if phase == "stopped_required" {
        SubscriptionRole::Required
    } else {
        SubscriptionRole::Observer
    };
    let stopped = noise.subscribe::<events::Noise>(role)?;
    let (data, peer_data) = UnixStream::pair()?;
    let (control, peer_control) = UnixStream::pair()?;
    let mut peer = Peer::spawn(phase, peer_data, peer_control, sandbox)?;
    let mut link = UnixLink::import::<events::Noise>(noise.clone(), data, control, 8)?;
    if REJECTED_PHASES.contains(&phase) {
        let rejected = match (phase, (&mut link).await) {
            ("oversized", Err(IpcError::Oversized { length, limit: 8 })) => {
                length == u32::MAX as usize
            }
            ("forged_event" | "unsolicited_control", Err(IpcError::Protocol)) => true,
            _ => false,
        };
        peer.finish().await?;
        if !rejected
            || stopped.try_recv()?.is_some()
            || noise.usage()
                != (Limits {
                    subscriptions: 1,
                    ..Limits::ZERO
                })
        {
            return Err("malformed peer was admitted or leaked transport resources".into());
        }
        eprintln!("{phase}: rejected before admission; transport reservations returned");
        return Ok(());
    }
    if phase == "stopped_required" {
        with_link(&mut link, async {
            while noise.stream_usage::<events::Noise>()?.waiters == 0 {
                tokio::task::yield_now().await;
            }
            Ok(())
        })
        .await?;
    } else if matches!(phase, "normal" | "flooding") {
        with_link(&mut link, async {
            let _ = stopped.recv().await?;
            Ok(())
        })
        .await?;
    }
    let mut measurement = with_link(&mut link, measure_local(&healthy, phase, operations)).await?;
    let mut admitted = 0;
    let mut gaps = 0;
    while let Some(item) = stopped.recv_now()? {
        match item {
            StreamItem::Data { sequence, value } if sequence == *value => admitted = sequence + 1,
            StreamItem::Gap { from, to } => gaps += to - from,
            StreamItem::Data { .. } => return Err("IPC import changed a value".into()),
        }
    }
    if (phase == "stopped_required" && admitted != 8) || (phase == "flooding" && admitted <= 1) {
        return Err("peer did not establish the requested interference phase".into());
    }
    noise.revoke().await?;
    if !matches!(link.await, Err(hiway::IpcError::Revoked)) {
        return Err("link ignored revocation".into());
    }
    measurement.report("host_local", phase, hz)?;
    peer.finish().await?;
    let usage = noise.usage();
    if usage.waiters != 0 || usage.connections != 0 || usage.bytes != 0 {
        return Err("link resources survived revocation".into());
    }
    eprintln!("{phase}: admitted_tail={admitted}, observed_gaps={gaps}");
    Ok(())
}

fn peer_frame(sequence: u64) -> [u8; 46] {
    let mut frame = [0; 46];
    frame[..4].copy_from_slice(b"HWY1");
    frame[4..20].copy_from_slice(&events::Noise::ID.as_u128().to_le_bytes());
    frame[20..22].copy_from_slice(&1_u16.to_le_bytes());
    frame[22..26].copy_from_slice(&1_u32.to_le_bytes());
    frame[26..34].copy_from_slice(&sequence.to_le_bytes());
    frame[34..38].copy_from_slice(&8_u32.to_le_bytes());
    frame[38..].copy_from_slice(&sequence.to_le_bytes());
    frame
}

async fn peer(phase: &str, hz: u32) -> Result {
    let data = std::os::unix::net::UnixStream::from(std::io::stdin().as_fd().try_clone_to_owned()?);
    let control =
        std::os::unix::net::UnixStream::from(std::io::stdout().as_fd().try_clone_to_owned()?);
    data.set_nonblocking(true)?;
    control.set_nonblocking(true)?;
    let mut data = UnixStream::from_std(data)?;
    let mut control = UnixStream::from_std(control)?;
    if REJECTED_PHASES.contains(&phase) {
        if phase == "unsolicited_control" {
            control.write_all(&[1]).await?;
        } else {
            let mut frame = peer_frame(0);
            if phase == "oversized" {
                frame[34..38].copy_from_slice(&u32::MAX.to_le_bytes());
            } else {
                frame[4] ^= 1;
            }
            data.write_all(&frame[..38]).await?;
        }
        if control.read(&mut [0]).await? != 0 {
            return Err("malformed frame received admission credit".into());
        }
        return Ok(());
    }
    let mut samples = Vec::with_capacity(100_000);
    let ticks = cpu_ticks()?;
    let region = Region::new(GLOBAL);
    let start = Instant::now();
    let mut completed = 0;
    if matches!(phase, "idle" | "baseline") {
        let mut byte = [0];
        if control.read(&mut byte).await? != 0 {
            return Err("idle peer received unsolicited credit".into());
        }
    } else {
        for sequence in 0..u64::MAX {
            let frame = peer_frame(sequence);
            let operation = Instant::now();
            if let Err(error) = data.write_all(&frame).await {
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
                ) {
                    break;
                }
                return Err(error.into());
            }
            let mut credit = [0; 9];
            if let Err(error) = control.read_exact(&mut credit).await {
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
                ) {
                    break;
                }
                return Err(error.into());
            }
            if credit[0] != 1 || u64::from_le_bytes(credit[1..].try_into()?) != sequence {
                return Err("credit did not match publication".into());
            }
            completed += 1;
            if samples.len() < 100_000 {
                samples.push(operation.elapsed().as_nanos());
            }
            if phase == "normal" {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    }
    let elapsed = start.elapsed();
    let allocations = region.change();
    Measurement {
        elapsed,
        allocations,
        completed,
        samples,
        ticks: cpu_ticks()? - ticks,
    }
    .report("peer_acceptance_rtt", phase, hz)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    let hz = ticks_per_second()?;
    if arguments
        .first()
        .is_some_and(|argument| argument == "--peer")
    {
        if arguments
            .get(2)
            .is_some_and(|argument| argument == "--sandbox-check")
        {
            sandbox_checks(
                arguments.get(3).ok_or("missing host executable")?,
                arguments.get(4).ok_or("missing host listener")?,
            )?;
        }
        return peer(arguments.get(1).ok_or("missing peer phase")?, hz).await;
    }
    let sandbox = arguments
        .get(1)
        .is_some_and(|argument| argument == "--sandbox-peer");
    if arguments.len() > 2 || (arguments.len() == 2 && !sandbox) {
        return Err("usage: ipc_process [operations] [--sandbox-peer]".into());
    }
    let operations = arguments
        .first()
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(10_000);
    if !(1..=1_000_000).contains(&operations) {
        return Err("choose 1..=1000000 operations".into());
    }
    eprintln!("Separate processes, inherited connected sockets, sandbox_peer={sandbox}. Host rows measure local send/yield/receive. Peer rows measure raw Unix publication-to-import-credit RTT, not handler completion, retaining the first 100000 samples. Normal traffic sleeps 1 ms between publications; flooding respects credits without pacing. Peer rows include warmup/idle/stall; host rows exclude 1000 warmup operations. CPU uses coarse Linux process ticks at {hz} Hz; zero ticks does not prove zero CPU use. Allocations cover each process. Zero-sample latency fields are 0. No affinity or latency guarantee.");
    println!("side,phase,operations,elapsed_ns,operations_per_second,cpu_ticks,ticks_per_second,latency_samples,p50_ns,p95_ns,p99_ns,allocations,allocated_bytes,reallocations");
    for phase in ["idle", "baseline", "normal", "stopped_required", "flooding"]
        .into_iter()
        .chain(REJECTED_PHASES)
    {
        tokio::time::timeout(Duration::from_secs(30), run(phase, operations, hz, sandbox))
            .await??;
    }
    Ok(())
}

fn sandbox_checks(host_executable: &str, host_listener: &str) -> Result {
    if std::fs::metadata(host_executable).is_ok() {
        return Err("sandbox exposes the host executable path".into());
    }
    if !std::fs::metadata("/app")?.is_file()
        || std::fs::OpenOptions::new().write(true).open("/app").is_ok()
    {
        return Err("sandbox executable mount is missing or writable".into());
    }
    if TcpStream::connect_timeout(&host_listener.parse()?, Duration::from_millis(100)).is_ok() {
        return Err("sandbox can connect to the host-only listener".into());
    }
    eprintln!("sandbox probes: host path hidden; executable read-only; host listener unreachable");
    Ok(())
}
