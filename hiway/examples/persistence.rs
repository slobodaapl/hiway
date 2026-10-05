use std::error::Error;

use hiway::{
    events, port, DynamicFabric, Grant, Limits, Permission, PortExt, Rights, StreamConfig,
    StreamItem, StreamLimits,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
struct WorkerState {
    completed: u32,
    last_job: Option<u32>,
}

#[events]
enum Events {
    Job(u32),
}

#[port(factory = Grant, required(events::Job))]
struct WorkerPort;

struct Worker {
    state: WorkerState,
    port: WorkerPort,
}

impl Worker {
    async fn complete_next(&mut self) -> Result<(), Box<dyn Error>> {
        match PortExt::recv::<events::Job>(&self.port).await? {
            StreamItem::Data { value, .. } => {
                self.state.completed = self
                    .state
                    .completed
                    .checked_add(1)
                    .ok_or_else(|| std::io::Error::other("worker completion count overflow"))?;
                self.state.last_job = Some(*value);
            }
            StreamItem::Gap { from, to } => {
                return Err(format!("worker missed jobs {from}..{to}").into());
            }
        }
        Ok(())
    }
}

async fn run_once(state: WorkerState, job: u32) -> Result<WorkerState, Box<dyn Error>> {
    let fabric = DynamicFabric::new();
    fabric.create_stream::<events::Job>(StreamConfig::default())?;
    let worker_grant = fabric.grant(
        &[
            Permission::new::<events::Job>(Rights::OBSERVE | Rights::REQUIRED).with_limits(
                StreamLimits {
                    retained_items: 0,
                    subscriptions: 1,
                    waiters: 1,
                },
            ),
        ],
        Limits {
            streams: 1,
            subscriptions: 1,
            waiters: 1,
            ..Limits::ZERO
        },
    )?;
    let source = fabric.grant(
        &[
            Permission::new::<events::Job>(Rights::PUBLISH).with_limits(StreamLimits {
                retained_items: 1,
                subscriptions: 0,
                waiters: 1,
            }),
        ],
        Limits {
            streams: 1,
            retained_items: 1,
            waiters: 1,
            ..Limits::ZERO
        },
    )?;
    let mut worker = Worker {
        state,
        port: WorkerPort::bind(&worker_grant)?,
    };

    source.sender::<events::Job>()?.send(job).await?;
    worker.complete_next().await?;
    Ok(worker.state)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let state = run_once(WorkerState::default(), 7).await?;
    let snapshot = serde_json::to_string(&state)?;
    println!("snapshot: {snapshot}");

    let restored = serde_json::from_str::<WorkerState>(&snapshot)?;
    let state = run_once(restored, 11).await?;
    println!(
        "restored: completed={}, last_job={:?}",
        state.completed, state.last_job
    );
    Ok(())
}

#[cfg(test)]
#[path = "persistence/tests.rs"]
mod tests;
