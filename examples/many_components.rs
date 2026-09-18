//! Dynamic hosts can instantiate the same injected port any number of times.

use hiway::{
    events, port, DynamicFabric, EventReceiver, Grant, Limits, Permission, PortExt, ReceiveError,
    Rights, StreamConfig, StreamItem,
};

#[events]
enum Events {
    Broadcast(u32),
}

#[port(factory = Grant, recv(events::Broadcast))]
struct MonsterPort;

struct Monster<P> {
    port: P,
    health: u32,
}

impl<P> Monster<P>
where
    P: EventReceiver<events::Broadcast>,
{
    fn receive_broadcast(&mut self) -> Result<bool, ReceiveError> {
        match self.port.recv_now::<events::Broadcast>()? {
            Some(StreamItem::Data { value, .. }) => {
                self.health = self.health.saturating_sub(*value);
                Ok(true)
            }
            Some(StreamItem::Gap { from, to }) => {
                eprintln!("monster missed broadcasts {from}..{to}");
                Ok(false)
            }
            None => Ok(false),
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let count = std::env::args()
        .nth(1)
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(1_024);
    if !(1..=65_536).contains(&count) {
        return Err("choose between 1 and 65536 monsters".into());
    }
    let fabric = DynamicFabric::with_limits(Limits {
        streams: 2,
        grants: 1,
        subscriptions: count.checked_mul(2).ok_or("too many subscriptions")?,
        retained_items: 2,
        waiters: 2,
        ..Limits::ZERO
    })?;
    fabric.create_stream::<events::Broadcast>(StreamConfig {
        capacity: 1,
        subscribers: count,
        waiters: 1,
    })?;
    let grant = fabric.grant(
        &[
            Permission::new::<events::Broadcast>(Rights::PUBLISH | Rights::OBSERVE).with_limits(
                hiway::StreamLimits {
                    retained_items: 1,
                    subscriptions: count,
                    waiters: 1,
                },
            ),
        ],
        Limits {
            streams: 1,
            subscriptions: count,
            retained_items: 1,
            waiters: 1,
            ..Limits::ZERO
        },
    )?;

    let mut monsters: Vec<_> = (0..count)
        .map(|_| {
            Ok::<_, hiway::PortError>(Monster {
                port: MonsterPort::bind(&grant)?,
                health: 100,
            })
        })
        .collect::<Result<_, _>>()?;

    grant.sender::<events::Broadcast>()?.send(7).await?;

    let mut delivered = 0;
    for monster in &mut monsters {
        delivered += usize::from(monster.receive_broadcast()?);
    }
    println!("applied 7 broadcast damage to {delivered} dynamically-created monsters");
    Ok(())
}
