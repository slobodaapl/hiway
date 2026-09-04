use hiway::{events, port, EventPort, EventReceiver, Hiway, PortExt, SendError};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Job(u32);

#[events]
enum Events {
    Started,
    Job(Job),
}

type GameBus = hiway::DynamicFabric;

#[port(factory = GameBus, send(events::Started), recv(events::Job))]
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
    async fn complete_next(&mut self) -> Result<(), SendError> {
        if let Some(Job(id)) = self.port.recv::<events::Job>().await {
            self.completed += 1;
            println!("completed job {id}");
        }
        self.port.publish(events::Started).await
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hiway = Hiway::new();
    let mut worker = Worker::new(WorkerPort::bind(hiway.fabric())?);

    hiway.alloc_sender::<events::Job>()?.send(Job(7)).await?;

    worker.complete_next().await?;
    Ok(())
}
