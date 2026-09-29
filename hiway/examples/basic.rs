use hiway::{
    events, port, DynamicFabric, EventPort, EventReceiver, Grant, Limits, Permission, PortExt,
    Rights, StreamConfig, StreamItem,
};

#[derive(Debug)]
pub struct Job(u32);

#[events]
enum Events {
    Started,
    Job(Job),
}

#[port(factory = Grant, send(events::Started), required(events::Job))]
struct WorkerPort;

struct Worker<P> {
    port: P,
    completed: u32,
}

impl<P> Worker<P> {
    fn new(port: P) -> Self {
        Self { port, completed: 0 }
    }
}

impl<P> Worker<P>
where
    P: EventPort<events::Started> + EventReceiver<events::Job>,
{
    async fn complete_next(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        match self.port.recv::<events::Job>().await? {
            StreamItem::Data { value, .. } => {
                self.completed += 1;
                println!("completed job {}", value.0);
                self.port.publish(events::Started).await?;
            }
            StreamItem::Gap { from, to } => {
                eprintln!("worker missed jobs {from}..{to}");
            }
        }
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let fabric = DynamicFabric::new();
    fabric.create_stream::<events::Started>(StreamConfig::default())?;
    fabric.create_stream::<events::Job>(StreamConfig::default())?;
    let worker_grant = fabric.grant(
        &[
            Permission::new::<events::Started>(Rights::PUBLISH).with_limits(hiway::StreamLimits {
                retained_items: 8,
                subscriptions: 0,
                waiters: 1,
            }),
            Permission::new::<events::Job>(Rights::OBSERVE | Rights::REQUIRED).with_limits(
                hiway::StreamLimits {
                    retained_items: 0,
                    subscriptions: 1,
                    waiters: 1,
                },
            ),
        ],
        Limits {
            streams: 2,
            subscriptions: 1,
            retained_items: 8,
            waiters: 2,
            ..Limits::ZERO
        },
    )?;
    let source = fabric.grant(
        &[
            Permission::new::<events::Job>(Rights::PUBLISH).with_limits(hiway::StreamLimits {
                retained_items: 8,
                subscriptions: 0,
                waiters: 1,
            }),
        ],
        Limits {
            streams: 1,
            retained_items: 8,
            waiters: 1,
            ..Limits::ZERO
        },
    )?;
    let mut worker = Worker::new(WorkerPort::bind(&worker_grant)?);

    source.sender::<events::Job>()?.send(Job(7)).await?;

    worker.complete_next().await?;
    Ok(())
}
