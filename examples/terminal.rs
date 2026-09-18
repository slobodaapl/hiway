//! A terminal view receives lines through an observer grant.

use hiway::{
    events, port, DynamicFabric, EventReceiver, Grant, Limits, Permission, PortExt, ReceiveError,
    Rights, StreamConfig, StreamItem,
};

#[derive(Debug)]
pub struct Line(pub &'static str);

#[events]
enum Events {
    Line(Line),
}

#[port(factory = Grant, recv(events::Line))]
struct TerminalPort;

struct Terminal<P> {
    port: P,
    lines_seen: usize,
}

impl<P> Terminal<P> {
    fn new(port: P) -> Self {
        Self {
            port,
            lines_seen: 0,
        }
    }
}

impl<P> Terminal<P>
where
    P: EventReceiver<events::Line>,
{
    async fn print_next(&mut self) -> Result<(), ReceiveError> {
        match self.port.recv::<events::Line>().await? {
            StreamItem::Data { value, .. } => {
                self.lines_seen += 1;
                println!("{}", value.0);
            }
            StreamItem::Gap { from, to } => {
                eprintln!("terminal missed {} lines", to - from);
            }
        }
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let fabric = DynamicFabric::new();
    fabric.create_stream::<events::Line>(StreamConfig::default())?;
    let terminal_grant = fabric.grant(
        &[
            Permission::new::<events::Line>(Rights::OBSERVE).with_limits(hiway::StreamLimits {
                retained_items: 0,
                subscriptions: 1,
                waiters: 1,
            }),
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
            Permission::new::<events::Line>(Rights::PUBLISH).with_limits(hiway::StreamLimits {
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
    let mut terminal = Terminal::new(TerminalPort::bind(&terminal_grant)?);

    source.sender::<events::Line>()?.send(Line("hello")).await?;

    terminal.print_next().await?;
    Ok(())
}
