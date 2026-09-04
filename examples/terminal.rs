//! Minimal terminal-free local-router example.

use hiway::{events, port, EventReceiver, Hiway, PortExt};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Line(pub &'static str);

#[events]
enum Events {
    Line(Line),
    Stop,
}

type TerminalBus = hiway::DynamicFabric;

#[port(factory = TerminalBus, recv(events::Line, events::Stop))]
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
    async fn print_next(&mut self) {
        if let Some(Line(line)) = self.port.recv::<events::Line>().await {
            self.lines_seen += 1;
            println!("{line}");
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hiway = Hiway::new();
    let mut terminal = Terminal::new(TerminalPort::bind(hiway.fabric())?);

    hiway
        .alloc_sender::<events::Line>()?
        .send(Line("hello"))
        .await?;

    terminal.print_next().await;
    Ok(())
}
