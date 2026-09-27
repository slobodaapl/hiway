use std::error::Error;

use hiway::{
    events, graph, DynamicFabric, EventReceiver, EventSender, Limits, Permission, Rights,
    StaticStream, StreamConfig, StreamItem, StreamLimits, SubscriptionRole,
};

#[events]
enum Events {
    Job(u16),
    Completed(u32),
}

struct Worker<R, S> {
    jobs: R,
    completed: S,
}

impl<R, S> Worker<R, S>
where
    R: EventReceiver<events::Job>,
    S: EventSender<events::Completed>,
{
    async fn complete_next(&self) -> Result<(), Box<dyn Error>> {
        match self.jobs.event_recv().await? {
            StreamItem::Data { value, .. } => {
                self.completed.send(2 * u32::from(*value)).await?;
            }
            StreamItem::Gap { from, to } => {
                return Err(format!("worker missed jobs {from}..{to}").into());
            }
        }
        Ok(())
    }
}

#[graph(
    jobs = (events::Job, 2, 1, 2),
    completed = (events::Completed, 2, 1, 2)
)]
struct Graph;

async fn run_static() -> Result<(), Box<dyn Error>> {
    let jobs = StaticStream::<events::Job, 2, 1, 2>::new();
    let completed = StaticStream::<events::Completed, 2, 1, 2>::new();
    let graph = Graph::new(&jobs, &completed);
    let worker = Worker {
        jobs: graph.subscribe::<events::Job>(SubscriptionRole::Required)?,
        completed: graph.sender::<events::Completed>()?,
    };
    let completion = graph.subscribe::<events::Completed>(SubscriptionRole::Required)?;

    graph.sender::<events::Job>()?.send(21).await?;
    worker.complete_next().await?;
    match completion.event_recv().await? {
        StreamItem::Data { value, .. } => println!("static: completed {}", *value),
        StreamItem::Gap { from, to } => {
            return Err(format!("static completion receiver missed {from}..{to}").into());
        }
    }
    Ok(())
}

async fn run_dynamic() -> Result<(), Box<dyn Error>> {
    let fabric = DynamicFabric::new();
    fabric.create_stream::<events::Job>(StreamConfig::default())?;
    fabric.create_stream::<events::Completed>(StreamConfig::default())?;
    let limits = Limits {
        streams: 2,
        subscriptions: 2,
        retained_items: 2,
        waiters: 2,
        ..Limits::ZERO
    };
    let grant = fabric.grant(
        &[
            Permission::new::<events::Job>(Rights::PUBLISH | Rights::OBSERVE | Rights::REQUIRED)
                .with_limits(StreamLimits {
                    retained_items: 1,
                    subscriptions: 1,
                    waiters: 1,
                }),
            Permission::new::<events::Completed>(
                Rights::PUBLISH | Rights::OBSERVE | Rights::REQUIRED,
            )
            .with_limits(StreamLimits {
                retained_items: 1,
                subscriptions: 1,
                waiters: 1,
            }),
        ],
        limits,
    )?;
    let worker = Worker {
        jobs: grant.subscribe::<events::Job>(SubscriptionRole::Required)?,
        completed: grant.sender::<events::Completed>()?,
    };
    let completion = grant.subscribe::<events::Completed>(SubscriptionRole::Required)?;

    grant.sender::<events::Job>()?.send(21).await?;
    worker.complete_next().await?;
    match completion.event_recv().await? {
        StreamItem::Data { value, .. } => println!("dynamic: completed {}", *value),
        StreamItem::Gap { from, to } => {
            return Err(format!("dynamic completion receiver missed {from}..{to}").into());
        }
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    run_static().await?;
    run_dynamic().await?;
    Ok(())
}
