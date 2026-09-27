use std::error::Error;

use hiway::{
    events, graph, port, DynamicFabric, EventPort, EventReceiver, Limits, Permission, PortExt,
    Rights, StaticStream, StreamConfig, StreamItem, StreamLimits, SubscriptionRole,
};

#[events]
enum Events {
    Job(u16),
    Completed(u32),
}

#[port(send(events::Completed), required(events::Job))]
struct WorkerPort;

struct Worker<P> {
    port: P,
}

impl<P> Worker<P>
where
    P: EventReceiver<events::Job> + EventPort<events::Completed>,
{
    async fn complete_next(&self) -> Result<(), Box<dyn Error>> {
        match self.port.event_recv().await? {
            StreamItem::Data { value, .. } => {
                self.port
                    .publish(events::Completed(2 * u32::from(*value)))
                    .await?;
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
    let jobs = StaticStream::new();
    let completed = StaticStream::new();
    let graph = Graph::new(&jobs, &completed);
    let worker = Worker {
        port: WorkerPort::bind(&graph)?,
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
        port: WorkerPort::bind(&grant)?,
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
