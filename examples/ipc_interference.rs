//! A local probe shares one executor with caller-driven Unix-socket traffic.
//! Both peers run in this process; this does not measure sandbox isolation.

use std::{
    alloc::System,
    cell::Cell,
    hint::black_box,
    time::{Duration, Instant},
};

use hiway::{
    events, DynamicFabric, Limits, Permission, Rights, SchemaRevision, StreamConfig, StreamItem,
    SubscriptionRole, UnixLink, WireCodec, WireError,
};
use stats_alloc::{Region, StatsAlloc, INSTRUMENTED_SYSTEM};
use tokio::{net::UnixStream, task::yield_now};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

type BenchResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[events]
enum Events {
    Healthy(u64),
    Noise(u64),
}

impl WireCodec for events::Noise {
    fn encoded_len(_: &u64) -> usize {
        8
    }

    fn encode(value: &u64, output: &mut [u8]) -> Result<usize, WireError> {
        output
            .get_mut(..8)
            .ok_or(WireError::BufferTooSmall)?
            .copy_from_slice(&value.to_le_bytes());
        Ok(8)
    }

    fn decode(bytes: &[u8], revision: SchemaRevision) -> Result<u64, WireError> {
        if revision != SchemaRevision(1) {
            return Err(WireError::UnsupportedRevision);
        }
        Ok(u64::from_le_bytes(
            bytes.try_into().map_err(|_| WireError::InvalidPayload)?,
        ))
    }
}

#[derive(Clone, Copy)]
enum Phase {
    Baseline,
    StoppedRequired,
    FloodingObserver,
}

impl Phase {
    fn name(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::StoppedRequired => "ipc_stopped_required",
            Self::FloodingObserver => "ipc_flooding_stopped_observer",
        }
    }
}

struct BenchmarkGrants {
    healthy: hiway::Grant,
    producer_grant: hiway::Grant,
    export_grant: hiway::Grant,
    import_grant: hiway::Grant,
    observer_grant: hiway::Grant,
}

fn benchmark_grants() -> BenchResult<BenchmarkGrants> {
    let source = DynamicFabric::new();
    let destination = DynamicFabric::new();
    let config = StreamConfig {
        capacity: 8,
        subscribers: 1,
        waiters: 4,
    };
    source.create_stream::<events::Healthy>(config)?;
    source.create_stream::<events::Noise>(config)?;
    destination.create_stream::<events::Noise>(config)?;
    let limits = Limits {
        streams: 1,
        subscriptions: 1,
        retained_items: 16,
        waiters: 4,
        ..Limits::ZERO
    };
    let link_limits = Limits {
        connections: 2,
        bytes: 128,
        ..limits
    };
    let healthy =
        source.grant(
            &[Permission::new::<events::Healthy>(
                Rights::PUBLISH | Rights::OBSERVE | Rights::REQUIRED,
            )
            .with_limits(hiway::StreamLimits {
                retained_items: 16,
                subscriptions: 1,
                waiters: 4,
            })],
            limits,
        )?;
    let producer_grant = source.grant(
        &[
            Permission::new::<events::Noise>(Rights::PUBLISH).with_limits(hiway::StreamLimits {
                retained_items: 16,
                subscriptions: 0,
                waiters: 4,
            }),
        ],
        limits,
    )?;
    let export_grant = source.grant(
        &[
            Permission::new::<events::Noise>(Rights::OBSERVE | Rights::REQUIRED).with_limits(
                hiway::StreamLimits {
                    retained_items: 0,
                    subscriptions: 1,
                    waiters: 2,
                },
            ),
        ],
        link_limits,
    )?;
    let import_grant = destination.grant(
        &[
            Permission::new::<events::Noise>(Rights::PUBLISH).with_limits(hiway::StreamLimits {
                retained_items: 15,
                subscriptions: 0,
                waiters: 2,
            }),
        ],
        link_limits,
    )?;
    let observer_grant = destination.grant(
        &[
            Permission::new::<events::Noise>(Rights::OBSERVE | Rights::REQUIRED).with_limits(
                hiway::StreamLimits {
                    retained_items: 0,
                    subscriptions: 1,
                    waiters: 0,
                },
            ),
        ],
        Limits {
            retained_items: 0,
            ..limits
        },
    )?;
    Ok(BenchmarkGrants {
        healthy,
        producer_grant,
        export_grant,
        import_grant,
        observer_grant,
    })
}

async fn probe(
    grants: &BenchmarkGrants,
    progress: &Cell<u64>,
    phase: Phase,
    operations: usize,
    samples: &mut Vec<u128>,
) -> BenchResult<(Duration, stats_alloc::Stats, u64)> {
    let sender = grants.healthy.sender::<events::Healthy>()?;
    let receiver = grants
        .healthy
        .subscribe::<events::Healthy>(SubscriptionRole::Required)?;
    let mut region = None;
    let mut start = Instant::now();
    let mut noise_before = 0;
    for index in 0..1_000 + operations {
        if index == 1_000 {
            if matches!(phase, Phase::StoppedRequired)
                && (grants.producer_grant.usage().waiters == 0
                    || grants.import_grant.usage().waiters <= 2)
            {
                return Err("IPC warmup did not reach required-receiver backpressure".into());
            }
            noise_before = progress.get();
            region = Some(Region::new(GLOBAL));
            start = Instant::now();
        }
        let operation_start = Instant::now();
        sender.send(black_box(index as u64)).await?;
        yield_now().await;
        match receiver.recv().await? {
            StreamItem::Data { sequence, value }
                if sequence == index as u64 && *value == index as u64 =>
            {
                black_box(value);
            }
            _ => return Err("healthy probe lost, reordered, or changed a value".into()),
        }
        if index >= 1_000 {
            samples.push(operation_start.elapsed().as_nanos());
        }
    }
    let elapsed = start.elapsed();
    let allocations = region.ok_or("measurement did not start")?.change();
    Ok::<_, Box<dyn std::error::Error>>((elapsed, allocations, progress.get() - noise_before))
}

async fn run(phase: Phase, operations: usize) -> BenchResult {
    let grants = benchmark_grants()?;
    let BenchmarkGrants {
        producer_grant,
        export_grant,
        import_grant,
        observer_grant,
        ..
    } = &grants;
    let noise_sender = producer_grant.sender::<events::Noise>()?;
    let role = if matches!(phase, Phase::StoppedRequired) {
        SubscriptionRole::Required
    } else {
        SubscriptionRole::Observer
    };
    let stopped = observer_grant.subscribe::<events::Noise>(role)?;
    let (source_data, destination_data) = UnixStream::pair()?;
    let (source_control, destination_control) = UnixStream::pair()?;
    let mut export = UnixLink::export::<events::Noise>(
        export_grant.clone(),
        source_data,
        source_control,
        SubscriptionRole::Required,
        8,
    )?;
    let mut import = UnixLink::import::<events::Noise>(
        import_grant.clone(),
        destination_data,
        destination_control,
        8,
    )?;
    let progress = Cell::new(0_u64);
    let producer = async {
        loop {
            if let Err(error) = noise_sender.send(progress.get()).await {
                break Err::<(), _>(error);
            }
            progress.set(progress.get() + 1);
            yield_now().await;
        }
    };
    tokio::pin!(producer);

    let mut samples = Vec::with_capacity(operations);
    let probe = probe(&grants, &progress, phase, operations, &mut samples);
    let active = !matches!(phase, Phase::Baseline);
    let (elapsed, allocations, noise_during) = tokio::select! {
        result = probe => result?,
        result = &mut export, if active => {
            result?;
            return Err("IPC export ended before the probe".into());
        }
        result = &mut import, if active => {
            result?;
            return Err("IPC import ended before the probe".into());
        }
        result = &mut producer, if active => {
            result?;
            return Err("IPC producer ended before the probe".into());
        }
        () = tokio::time::sleep(Duration::from_secs(30)) => {
            return Err("IPC interference phase exceeded 30 seconds".into());
        }
    };
    let source_usage = producer_grant.usage();
    let export_usage = export_grant.usage();
    let import_usage = import_grant.usage();
    let mut admitted = 0;
    let mut gaps = 0;
    while let Some(item) = stopped.recv_now()? {
        match item {
            StreamItem::Data { sequence, .. } => admitted = sequence + 1,
            StreamItem::Gap { from, to } => gaps += to - from,
        }
    }
    samples.sort_unstable();
    let percentile = |percent: usize| samples[(samples.len() * percent).div_ceil(100) - 1];
    println!(
        "{},{operations},{},{:.0},{},{},{},{},{},{},{},{noise_during},{},{admitted},{gaps},{},{},{},{},{}",
        phase.name(), elapsed.as_nanos(), f64::from(u32::try_from(operations)?) / elapsed.as_secs_f64(),
        percentile(50), percentile(95), percentile(99), samples[samples.len() - 1],
        allocations.allocations, allocations.bytes_allocated, allocations.reallocations,
        progress.get(), source_usage.retained_items, import_usage.retained_items,
        source_usage.waiters, export_usage.waiters, import_usage.waiters,
    );
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> BenchResult {
    let operations = std::env::args()
        .nth(1)
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(10_000);
    if !(1..=1_000_000).contains(&operations) {
        return Err("choose between 1 and 1000000 measured roundtrips".into());
    }
    eprintln!(
        "Same-process Unix peers, one Tokio current-thread executor, caller-owned futures. \
         Healthy probe: local send, yield, local required receive; 1000 warmup roundtrips per phase. \
         Allocation counts cover all active futures; setup, sorting and shutdown excluded. \
         Resource figures are end snapshots, not peaks; import figures include link reservations. \
         No CPU affinity, process isolation, real-time bound, or IPC delivery-latency claim."
    );
    println!(
        "phase,operations,elapsed_ns,roundtrips_per_second,p50_ns,p95_ns,p99_ns,max_ns,allocations,allocated_bytes,reallocations,noise_published_during_measurement,noise_published_total,noise_admitted_total,observer_gaps,source_retained_snapshot,import_retained_snapshot,producer_waiters_snapshot,export_waiters_snapshot,import_waiters_snapshot"
    );
    for phase in [
        Phase::Baseline,
        Phase::StoppedRequired,
        Phase::FloodingObserver,
    ] {
        run(phase, operations).await?;
    }
    Ok(())
}
