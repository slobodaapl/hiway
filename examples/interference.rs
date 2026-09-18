//! Local admission and receipt under unrelated stalled or flooding streams.
//! Allocation counters cover the whole process and add shared atomic overhead.

use std::{
    alloc::System,
    hint::black_box,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc, Arc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use hiway::{
    events, DynamicFabric, DynamicReceiver, DynamicSender, Limits, Permission, Rights,
    StreamConfig, StreamItem, SubscriptionRole,
};
use stats_alloc::{Region, StatsAlloc, INSTRUMENTED_SYSTEM};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

type BenchResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[events]
enum Events {
    Healthy(u64),
    Noise(u64),
}

#[derive(Clone, Copy)]
enum Phase {
    Baseline,
    StoppedObserver,
    FullRequired,
    Flooding,
}

impl Phase {
    fn name(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::StoppedObserver => "unrelated_stopped_observer",
            Self::FullRequired => "unrelated_full_required",
            Self::Flooding => "unrelated_flooding",
        }
    }
}

struct Flood {
    stop: Arc<AtomicBool>,
    progress: Arc<AtomicU64>,
    worker: Option<JoinHandle<Result<(), &'static str>>>,
}

impl Flood {
    fn start(sender: DynamicSender<events::Noise>) -> BenchResult<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let progress = Arc::new(AtomicU64::new(0));
        let worker_stop = stop.clone();
        let worker_progress = progress.clone();
        let (ready, started) = mpsc::sync_channel(1);
        let worker = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(30);
            let mut ready = Some(ready);
            let mut sent = 0;
            while !worker_stop.load(Ordering::Acquire) {
                sender
                    .send_now(sent)
                    .map_err(|_| "unrelated flood publication was rejected")?;
                sent += 1;
                worker_progress.store(sent, Ordering::Release);
                if let Some(ready) = ready.take() {
                    let _ = ready.send(());
                }
                if sent % 256 == 0 {
                    if Instant::now() >= deadline {
                        return Err("flood exceeded its 30-second deadline");
                    }
                    thread::yield_now();
                }
            }
            Ok(())
        });
        let mut flood = Self {
            stop,
            progress,
            worker: Some(worker),
        };
        if started.recv_timeout(Duration::from_secs(2)).is_err() {
            flood.finish()?;
            return Err("flood did not start within two seconds".into());
        }
        Ok(flood)
    }

    fn finish(&mut self) -> BenchResult {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let deadline = Instant::now() + Duration::from_secs(2);
            while !worker.is_finished() {
                if Instant::now() >= deadline {
                    return Err("flood did not stop within two seconds".into());
                }
                thread::sleep(Duration::from_millis(1));
            }
            worker.join().map_err(|_| "flood worker panicked")??;
        }
        Ok(())
    }
}

impl Drop for Flood {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
    }
}

fn roundtrip(
    sender: &DynamicSender<events::Healthy>,
    receiver: &DynamicReceiver<events::Healthy>,
    sent: u64,
) -> BenchResult {
    sender.send_now(black_box(sent))?;
    match receiver.recv_now()? {
        Some(StreamItem::Data { sequence, value }) if sequence == sent && *value == sent => {
            black_box(value);
            Ok(())
        }
        _ => Err("healthy roundtrip lost, reordered, or changed a value".into()),
    }
}

fn stream_grant<E: hiway::EventSpec>(fabric: &DynamicFabric) -> BenchResult<hiway::Grant> {
    Ok(fabric.grant(
        &[
            Permission::new::<E>(Rights::PUBLISH | Rights::OBSERVE | Rights::REQUIRED).with_limits(
                hiway::StreamLimits {
                    retained_items: 16,
                    subscriptions: 1,
                    waiters: 2,
                },
            ),
        ],
        Limits {
            streams: 1,
            subscriptions: 1,
            retained_items: 16,
            waiters: 2,
            ..Limits::ZERO
        },
    )?)
}

fn run(phase: Phase, operations: usize) -> BenchResult {
    let fabric = DynamicFabric::new();
    let config = StreamConfig {
        capacity: 8,
        subscribers: 1,
        waiters: 2,
    };
    fabric.create_stream::<events::Healthy>(config)?;
    fabric.create_stream::<events::Noise>(config)?;
    let healthy = stream_grant::<events::Healthy>(&fabric)?;
    let noise = stream_grant::<events::Noise>(&fabric)?;
    let sender = healthy.sender::<events::Healthy>()?;
    let receiver = healthy.subscribe::<events::Healthy>(SubscriptionRole::Required)?;
    let noise_sender = noise.sender::<events::Noise>()?;
    let _stopped = match phase {
        Phase::Baseline => None,
        Phase::FullRequired => Some(noise.subscribe::<events::Noise>(SubscriptionRole::Required)?),
        Phase::StoppedObserver | Phase::Flooding => {
            Some(noise.subscribe::<events::Noise>(SubscriptionRole::Observer)?)
        }
    };
    let preload = match phase {
        Phase::StoppedObserver => config.capacity * 2,
        Phase::FullRequired => config.capacity,
        Phase::Baseline | Phase::Flooding => 0,
    };
    for value in 0..preload {
        noise_sender.send_now(value as u64)?;
    }
    let mut flood = if matches!(phase, Phase::Flooding) {
        Some(Flood::start(noise_sender)?)
    } else {
        None
    };

    let mut samples = Vec::with_capacity(operations);
    let measurement = (|| -> BenchResult<_> {
        for value in 0..1_000 {
            roundtrip(&sender, &receiver, value)?;
        }
        let flood_before = flood
            .as_ref()
            .map_or(0, |flood| flood.progress.load(Ordering::Acquire));
        let region = Region::new(GLOBAL);
        let start = Instant::now();
        for index in 0..operations {
            let operation_start = Instant::now();
            roundtrip(&sender, &receiver, 1_000 + index as u64)?;
            samples.push(operation_start.elapsed().as_nanos());
        }
        let elapsed = start.elapsed();
        let flood_during = flood
            .as_ref()
            .map_or(0, |flood| flood.progress.load(Ordering::Acquire))
            - flood_before;
        Ok((elapsed, region, flood_during))
    })();
    if let Some(flood) = &mut flood {
        flood.finish()?;
    }
    let (elapsed, region, flood_during) = measurement?;
    let allocations = region.change();
    let healthy_usage = healthy.usage();
    let noise_usage = noise.usage();
    let scope_usage = fabric.usage();
    samples.sort_unstable();
    let percentile = |percent: usize| samples[(samples.len() * percent).div_ceil(100) - 1];
    println!(
        "{},{operations},{},{:.0},{},{},{},{},{},{},{},{},{},{},{},{flood_during}",
        phase.name(),
        elapsed.as_nanos(),
        f64::from(u32::try_from(operations)?) / elapsed.as_secs_f64(),
        percentile(50),
        percentile(95),
        percentile(99),
        samples[samples.len() - 1],
        allocations.allocations,
        allocations.bytes_allocated,
        allocations.reallocations,
        healthy_usage.retained_items + noise_usage.retained_items,
        healthy_usage.subscriptions + noise_usage.subscriptions,
        healthy_usage.waiters + noise_usage.waiters,
        scope_usage.retained_items,
    );
    Ok(())
}

fn main() -> BenchResult {
    let operations = std::env::args()
        .nth(1)
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(10_000);
    if !(1..=1_000_000).contains(&operations) {
        return Err("choose between 1 and 1000000 measured roundtrips".into());
    }
    eprintln!(
        "Local u64 roundtrips; 1000 warmup operations per phase; per-operation timing overhead remains. \
         Allocation counts cover the whole process, including flood shutdown, but exclude setup and sorting. \
         Global allocation counters add shared atomic contention. Resource figures are end snapshots, not peaks. \
         No CPU affinity, latency guarantee, idle-CPU measurement, or IPC measurement."
    );
    println!(
        "phase,operations,elapsed_ns,roundtrips_per_second,p50_ns,p95_ns,p99_ns,max_ns,allocations,allocated_bytes,reallocations,retained_items_snapshot,subscriptions_snapshot,waiters_snapshot,scope_reserved_items,flood_publishes_during_measurement"
    );
    for phase in [
        Phase::Baseline,
        Phase::StoppedObserver,
        Phase::FullRequired,
        Phase::Flooding,
    ] {
        run(phase, operations)?;
    }
    Ok(())
}
