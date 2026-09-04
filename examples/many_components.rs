//! Dynamic hosts can instantiate the same injected port any number of times.

use hiway::{events, port, EventReceiver, Hiway, PortExt};

#[events]
enum Events {
    Broadcast(u32),
}

type GameBus = hiway::DynamicFabric;

#[port(factory = GameBus, capacity = 1, recv(events::Broadcast))]
struct MonsterPort;

struct Monster<P> {
    port: P,
    health: u32,
}

impl<P> Monster<P>
where
    P: EventReceiver<events::Broadcast>,
{
    fn received_broadcast(&self) -> bool {
        self.health > 0 && self.port.try_recv::<events::Broadcast>() == Some(7)
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let count = std::env::args()
        .nth(1)
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1_024);
    let hiway = Hiway::new();

    let monsters: Vec<_> = (0..count)
        .map(|_| {
            Ok::<_, hiway::PortError>(Monster {
                port: MonsterPort::bind(hiway.fabric())?,
                health: 100,
            })
        })
        .collect::<Result<_, _>>()?;

    hiway.alloc_sender::<events::Broadcast>()?.send(7).await?;

    let delivered = monsters
        .iter()
        .filter(|monster| monster.received_broadcast())
        .count();
    println!("delivered one broadcast to {delivered} dynamically-created monsters");
    Ok(())
}
