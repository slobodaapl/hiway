//! Animated terminal demonstration of routing and a custom terminal pipeline.

use std::{
    collections::VecDeque,
    io::{self, IsTerminal, Write},
    time::{Duration, Instant},
};

use hiway::{Batch, Bus, OutputFull, Pipeline, RecvError, TypedSubscription};
use tokio::time::{interval, sleep};

const fn ensure_send<F: std::future::Future + Send>(future: F) -> F {
    future
}

const MESSAGE: &str = "HIWAY EVENTS FLOW THROUGH TRANSFORMS   ";
const BILLBOARD_WIDTH: usize = 41;
const PIPE_WIDTH: usize = 105;
const PRODUCER_TAP: usize = 17;
const PITSTOP_POSITION: usize = 36;
const COUNT_KEEPER_TAP: usize = 54;
const TERMINAL_UI_TAP: usize = 79;
const FRAMES_PER_LEG: usize = 6;
const FRAME_TIME: Duration = Duration::from_millis(30);
const RESET: &str = "\x1b[0m";
const PRODUCER_BOX_INNER_WIDTH: usize = 32;
const TERMINAL_BOX_INNER_WIDTH: usize = 49;
const TERMINAL_BOX_X: usize = 54;
const COUNT_BOX_INNER_WIDTH: usize = 39;
const COUNT_BOX_X: usize = 34;
const TICK_INTERVAL: Duration = Duration::from_millis(6);
const PRODUCER_INTERVAL: Duration = Duration::from_millis(450);

type DemoResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Debug)]
struct CharacterEvent {
    text: String,
}

#[derive(Clone, Debug, hiway::HiwayEvent)]
enum Events {
    Character(CharacterEvent),
    RgbaCount(RgbaCountEvent),
    UiCountUpdate(UiCountUpdateEvent),
}

struct ColorPipeline {
    epoch: Instant,
}

impl Pipeline<Events, 4> for ColorPipeline {
    fn apply<'a>(
        &'a self,
        event: Events,
    ) -> impl std::future::Future<Output = Result<Batch<Events, 4>, OutputFull>> + Send + 'a
    where
        Events: 'a,
    {
        ensure_send(async move {
            match event {
                Events::Character(mut character) => {
                    let color = Colorizer::at(self.epoch.elapsed());
                    character.text = color.apply(&character.text);
                    Batch::try_from_iter([
                        character.into(),
                        RgbaCountEvent::from_colorizer(color).into(),
                    ])
                }
                other => Batch::try_from_iter([other]),
            }
        })
    }
}

type DemoBus = Bus<Events, 4, 8, 4, 4, ColorPipeline>;
type DemoSubscription<'a, V> = TypedSubscription<'a, Events, V, 4, 8, 4, 4>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Colorizer {
    Pass,
    Red,
    Green,
    Blue,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct RgbaCountEvent {
    none: u64,
    red: u64,
    green: u64,
    blue: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct UiCountUpdateEvent {
    none: u64,
    red: u64,
    green: u64,
    blue: u64,
}

impl UiCountUpdateEvent {
    fn from_totals(totals: RgbaCountEvent) -> Self {
        Self {
            none: totals.none,
            red: totals.red,
            green: totals.green,
            blue: totals.blue,
        }
    }
}

impl RgbaCountEvent {
    fn from_colorizer(colorizer: Colorizer) -> Self {
        match colorizer {
            Colorizer::Pass => Self {
                none: 1,
                ..Self::default()
            },
            Colorizer::Red => Self {
                red: 1,
                ..Self::default()
            },
            Colorizer::Green => Self {
                green: 1,
                ..Self::default()
            },
            Colorizer::Blue => Self {
                blue: 1,
                ..Self::default()
            },
        }
    }

    fn accumulate(&mut self, delta: Self) {
        self.none += delta.none;
        self.red += delta.red;
        self.green += delta.green;
        self.blue += delta.blue;
    }
}

impl Colorizer {
    fn at(elapsed: Duration) -> Self {
        match (elapsed.as_secs() / 2) % 4 {
            0 => Self::Pass,
            1 => Self::Red,
            2 => Self::Green,
            _ => Self::Blue,
        }
    }

    fn apply(self, text: &str) -> String {
        match self {
            Self::Pass => text.to_owned(),
            Self::Red => format!("r!{text}"),
            Self::Green => format!("g!{text}"),
            Self::Blue => format!("b!{text}"),
        }
    }

    fn from_payload(payload: &str) -> Self {
        if payload.starts_with("r!") {
            Self::Red
        } else if payload.starts_with("g!") {
            Self::Green
        } else if payload.starts_with("b!") {
            Self::Blue
        } else {
            Self::Pass
        }
    }

    fn ansi(self) -> &'static str {
        match self {
            Self::Pass => "\x1b[97m",
            Self::Red => "\x1b[91m",
            Self::Green => "\x1b[92m",
            Self::Blue => "\x1b[94m",
        }
    }

    fn paint(self, text: &str) -> String {
        format!("{}{text}{RESET}", self.ansi())
    }
}

#[derive(Clone, Copy)]
enum Stage {
    Moving(usize),
    Delivered,
}

struct CharacterProducer<'a> {
    bus: &'a DemoBus,
    message: std::iter::Cycle<std::str::Chars<'static>>,
}

impl<'a> CharacterProducer<'a> {
    fn new(bus: &'a DemoBus) -> Self {
        Self {
            bus,
            message: MESSAGE.chars().cycle(),
        }
    }

    async fn publish_next(&mut self) -> DemoResult {
        let character = self
            .message
            .next()
            .expect("the repeated demo message is not empty");
        self.bus
            .publish(CharacterEvent {
                text: character.to_string(),
            })
            .await?;
        Ok(())
    }

    async fn run(mut self) -> DemoResult {
        loop {
            self.publish_next().await?;
            sleep(PRODUCER_INTERVAL).await;
        }
    }
}

struct CountKeeper<'a> {
    bus: &'a DemoBus,
    rgba_events: DemoSubscription<'a, RgbaCountEvent>,
    totals: RgbaCountEvent,
}

impl<'a> CountKeeper<'a> {
    fn new(bus: &'a DemoBus) -> Result<Self, hiway::SubscribersFull> {
        Ok(Self {
            bus,
            rgba_events: bus.consume::<RgbaCountEvent>()?,
            totals: RgbaCountEvent::default(),
        })
    }

    async fn update(&mut self) -> DemoResult {
        let delta = self.rgba_events.recv().await?;
        self.totals.accumulate(delta);
        self.bus
            .publish(UiCountUpdateEvent::from_totals(self.totals))
            .await?;
        Ok(())
    }

    async fn run(mut self) -> DemoResult {
        loop {
            self.update().await?;
        }
    }
}

struct TerminalUi<'a> {
    character_events: DemoSubscription<'a, CharacterEvent>,
    count_updates: DemoSubscription<'a, UiCountUpdateEvent>,
    billboard: VecDeque<(Colorizer, char)>,
    delivered: u64,
    counts: UiCountUpdateEvent,
    pending_character: Option<(char, Colorizer, CharacterEvent)>,
}

impl<'a> TerminalUi<'a> {
    fn new(bus: &'a DemoBus) -> Result<Self, hiway::SubscribersFull> {
        Ok(Self {
            character_events: bus.consume::<CharacterEvent>()?,
            count_updates: bus.consume::<UiCountUpdateEvent>()?,
            billboard: VecDeque::with_capacity(BILLBOARD_WIDTH),
            delivered: 0,
            counts: UiCountUpdateEvent::default(),
            pending_character: None,
        })
    }

    async fn receive_character(&mut self) -> Result<CharacterEvent, RecvError> {
        self.character_events.recv().await
    }

    async fn receive_count_update(&mut self) -> Result<UiCountUpdateEvent, RecvError> {
        let update = self.count_updates.recv().await?;
        self.counts = update;
        Ok(update)
    }

    async fn run(mut self) -> DemoResult {
        loop {
            let transformed = self.receive_character().await?;
            self.animate_character(transformed).await?;
            self.deliver().await?;
        }
    }
}

struct Terminal;

impl Terminal {
    fn enter() -> io::Result<Self> {
        let terminal = Self;
        let mut stdout = io::stdout().lock();
        write!(stdout, "\x1b[?1049h\x1b[?25l\x1b[2J\x1b[H")?;
        stdout.flush()?;
        Ok(terminal)
    }
}

impl Drop for Terminal {
    fn drop(&mut self) {
        let mut stdout = io::stdout().lock();
        let _ = write!(stdout, "{RESET}\x1b[?25h\x1b[?1049l");
        let _ = stdout.flush();
    }
}

fn track(marker: Option<(usize, Colorizer)>, active: Colorizer) -> String {
    let mut track = String::new();
    for position in 0..PIPE_WIDTH {
        match marker {
            Some((marker_position, color)) if marker_position == position => {
                let color = if position == PITSTOP_POSITION {
                    active
                } else {
                    color
                };
                track.push_str(color.ansi());
                track.push(if position == PITSTOP_POSITION {
                    '◆'
                } else {
                    '●'
                });
                track.push_str(RESET);
            }
            _ if position == PITSTOP_POSITION => {
                track.push_str(active.ansi());
                track.push('◆');
                track.push_str(RESET);
            }
            _ if position == PRODUCER_TAP || position == TERMINAL_UI_TAP => track.push('┴'),
            _ if position == COUNT_KEEPER_TAP => track.push('┬'),
            _ => track.push('─'),
        }
    }
    track
}

fn marker_position(frame: usize, start: usize, end: usize) -> usize {
    start + frame * (end - start) / (FRAMES_PER_LEG - 1)
}

fn visible(text: &str) -> String {
    text.replace(' ', "␠")
}

fn box_top(title: &str, inner_width: usize) -> String {
    let label = format!("─ {title} ");
    format!(
        "┌{label}{}┐",
        "─".repeat(inner_width.saturating_sub(label.chars().count()))
    )
}

fn box_row(text: &str, inner_width: usize) -> String {
    format!(
        "│{text}{}│",
        " ".repeat(inner_width.saturating_sub(text.chars().count()))
    )
}

fn box_bottom(inner_width: usize) -> String {
    format!("└{}┘", "─".repeat(inner_width))
}

fn placed_line(parts: &[(usize, &str)]) -> String {
    let mut line = vec![' '; PIPE_WIDTH];
    for &(start, text) in parts {
        for (offset, character) in text.chars().enumerate() {
            if let Some(slot) = line.get_mut(start + offset) {
                *slot = character;
            }
        }
    }
    line.into_iter().collect()
}

fn side_by_side(left: &str, right_x: usize, right: &str) -> String {
    format!(
        "{left}{}{right}",
        " ".repeat(right_x.saturating_sub(left.chars().count()))
    )
}

fn frame_line(frame: &mut String, line: &str) {
    frame.push_str(line);
    frame.push_str("\x1b[K\n");
}

impl TerminalUi<'_> {
    async fn animate_character(&mut self, transformed: CharacterEvent) -> DemoResult {
        let raw = transformed
            .text
            .chars()
            .last()
            .expect("a character event payload is not empty");
        let color = Colorizer::from_payload(&transformed.text);

        for frame in 0..FRAMES_PER_LEG {
            self.render(
                raw,
                None,
                color,
                Stage::Moving(marker_position(frame, PRODUCER_TAP, PITSTOP_POSITION)),
            )?;
            sleep(FRAME_TIME).await;
        }

        for frame in 0..FRAMES_PER_LEG {
            self.render(
                raw,
                Some(&transformed.text),
                color,
                Stage::Moving(marker_position(frame, PITSTOP_POSITION, TERMINAL_UI_TAP)),
            )?;
            sleep(FRAME_TIME).await;
        }

        self.pending_character = Some((raw, color, transformed));
        Ok(())
    }

    async fn deliver(&mut self) -> DemoResult {
        self.receive_count_update().await?;
        let (raw, color, transformed) = self
            .pending_character
            .take()
            .expect("a count update follows an animated character");

        if self.billboard.len() == BILLBOARD_WIDTH {
            self.billboard.pop_front();
        }
        self.billboard.push_back((color, raw));
        self.delivered += 1;
        self.render(raw, Some(&transformed.text), color, Stage::Delivered)?;
        sleep(Duration::from_millis(70)).await;
        Ok(())
    }

    fn render_endpoints(&self, frame: &mut String, raw: &str) {
        let producer_box = [
            box_top("CHARACTER PRODUCER", PRODUCER_BOX_INNER_WIDTH),
            box_row("  publishes CharacterEvent", PRODUCER_BOX_INNER_WIDTH),
            box_row(&format!("  raw: \"{raw}\""), PRODUCER_BOX_INNER_WIDTH),
            box_row("  cadence: one character", PRODUCER_BOX_INNER_WIDTH),
            box_row("  knows only this bus", PRODUCER_BOX_INNER_WIDTH),
            box_bottom(PRODUCER_BOX_INNER_WIDTH),
        ];

        let mut billboard = String::from("│  [");
        for &(color, character) in &self.billboard {
            billboard.push_str(&color.paint(&character.to_string()));
        }
        billboard.push_str(&" ".repeat(BILLBOARD_WIDTH - self.billboard.len()));
        billboard.push_str("]    │");

        let terminal_box = [
            box_top("TERMINAL UI", TERMINAL_BOX_INNER_WIDTH),
            box_row("  consumes CharacterEvent", TERMINAL_BOX_INNER_WIDTH),
            box_row("  consumes UiCountUpdateEvent", TERMINAL_BOX_INNER_WIDTH),
            billboard,
            box_row(
                &format!(
                    "  R {:04}  G {:04}  B {:04}  A/AS-IS {:04}",
                    self.counts.red, self.counts.green, self.counts.blue, self.counts.none,
                ),
                TERMINAL_BOX_INNER_WIDTH,
            ),
            box_bottom(TERMINAL_BOX_INNER_WIDTH),
        ];

        for (producer, terminal) in producer_box.iter().zip(&terminal_box) {
            frame_line(frame, &side_by_side(producer, TERMINAL_BOX_X, terminal));
        }
    }

    fn render(
        &self,
        raw: char,
        transformed: Option<&str>,
        active: Colorizer,
        stage: Stage,
    ) -> io::Result<()> {
        let marker = match stage {
            Stage::Moving(position) => Some((
                position,
                if position < PITSTOP_POSITION {
                    Colorizer::Pass
                } else {
                    active
                },
            )),
            Stage::Delivered => None,
        };
        let raw = visible(&raw.to_string());
        let transformed_text = transformed.map_or_else(|| "waiting".into(), visible);
        let transform_status = match active {
            Colorizer::Pass => "PASS · as-is + one-hot count",
            Colorizer::Red => "RED · r! + one-hot count",
            Colorizer::Green => "GREEN · g! + one-hot count",
            Colorizer::Blue => "BLUE · b! + one-hot count",
        };

        let mut frame = String::from("\x1b[H");
        frame_line(&mut frame, "\x1b[1;96mHIWAY\x1b[0m  live typed events");
        frame_line(
            &mut frame,
            "one bounded event bus · Tokio drives four borrowed futures · central 6 ms tick",
        );
        frame_line(&mut frame, &"─".repeat(PIPE_WIDTH));
        frame_line(&mut frame, "");
        self.render_endpoints(&mut frame, &raw);

        let transform_label = format!("TRANSFORM: {transform_status}");
        frame_line(
            &mut frame,
            &placed_line(&[
                (PRODUCER_TAP, "│"),
                (22, &transform_label),
                (TERMINAL_UI_TAP, "│"),
            ]),
        );
        frame_line(
            &mut frame,
            &placed_line(&[
                (PRODUCER_TAP, "│"),
                (PITSTOP_POSITION, "│"),
                (TERMINAL_UI_TAP, "│"),
            ]),
        );
        frame_line(&mut frame, &track(marker, active));
        frame_line(&mut frame, &placed_line(&[(COUNT_KEEPER_TAP, "│")]));
        frame_line(&mut frame, &placed_line(&[(COUNT_KEEPER_TAP, "│")]));

        let count_box = [
            box_top("COUNT KEEPER", COUNT_BOX_INNER_WIDTH),
            box_row("  consumes RgbaCountEvent", COUNT_BOX_INNER_WIDTH),
            box_row(
                &format!(
                    "  R {:04}  G {:04}  B {:04}  A {:04}",
                    self.counts.red, self.counts.green, self.counts.blue, self.counts.none,
                ),
                COUNT_BOX_INNER_WIDTH,
            ),
            box_row("  publishes UiCountUpdateEvent", COUNT_BOX_INNER_WIDTH),
            box_bottom(COUNT_BOX_INNER_WIDTH),
        ];
        for row in &count_box {
            frame_line(&mut frame, &placed_line(&[(COUNT_BOX_X, row)]));
        }

        frame_line(&mut frame, "");
        frame_line(
            &mut frame,
            &format!(
                "in flight: raw \"{raw}\" → transformed \"{transformed_text}\" · delivered {:04}",
                self.delivered,
            ),
        );
        frame.push_str("\x1b[J");

        let mut stdout = io::stdout().lock();
        stdout.write_all(frame.as_bytes())?;
        stdout.flush()
    }
}

async fn run_ticker(bus: &DemoBus) -> DemoResult {
    let mut ticker = interval(TICK_INTERVAL);
    loop {
        ticker.tick().await;
        let _ = bus.tick();
    }
}

async fn run_demo() -> DemoResult {
    let bus = Bus::with_pipeline(ColorPipeline {
        epoch: Instant::now(),
    });

    let producer = CharacterProducer::new(&bus);
    let count_keeper = CountKeeper::new(&bus)?;
    let terminal_ui = TerminalUi::new(&bus)?;

    tokio::try_join!(
        producer.run(),
        count_keeper.run(),
        terminal_ui.run(),
        run_ticker(&bus),
    )?;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> DemoResult {
    if !io::stdout().is_terminal() {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "hiway-demo requires an interactive terminal",
        )
        .into());
    }

    let _terminal = Terminal::enter()?;
    tokio::select! {
        result = run_demo() => result,
        result = tokio::signal::ctrl_c() => {
            result?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestPipeline;

    impl Pipeline<Events, 4> for TestPipeline {
        fn apply<'a>(
            &'a self,
            event: Events,
        ) -> impl std::future::Future<Output = Result<Batch<Events, 4>, OutputFull>> + Send + 'a
        where
            Events: 'a,
        {
            ensure_send(async move {
                match event {
                    Events::Character(mut character) => {
                        let color = match character.text.as_str() {
                            "P" => Colorizer::Pass,
                            "R" => Colorizer::Red,
                            "G" => Colorizer::Green,
                            "B" => Colorizer::Blue,
                            other => panic!("unexpected test character: {other}"),
                        };
                        character.text = color.apply(&character.text);
                        Batch::try_from_iter([
                            character.into(),
                            RgbaCountEvent::from_colorizer(color).into(),
                        ])
                    }
                    other => Batch::try_from_iter([other]),
                }
            })
        }
    }

    #[test]
    fn colorizer_cycle_and_payload_contract() {
        let states = [
            (0, Colorizer::Pass, "H"),
            (2, Colorizer::Red, "r!H"),
            (4, Colorizer::Green, "g!H"),
            (6, Colorizer::Blue, "b!H"),
            (8, Colorizer::Pass, "H"),
        ];

        for (seconds, expected_color, expected_payload) in states {
            let color = Colorizer::at(Duration::from_secs(seconds));
            assert_eq!(color, expected_color);
            assert_eq!(color.apply("H"), expected_payload);
            assert_eq!(Colorizer::from_payload(expected_payload), expected_color);
        }
    }

    #[tokio::test]
    async fn terminal_transform_preserves_characters_and_emits_one_hot_counts() {
        let bus: Bus<Events, 4, 8, 4, 4, TestPipeline> = Bus::with_pipeline(TestPipeline);
        let mut characters = bus.consume::<CharacterEvent>().unwrap();
        let mut counts = bus.consume::<RgbaCountEvent>().unwrap();
        let cases = [
            ("P", "P", Colorizer::Pass),
            ("R", "r!R", Colorizer::Red),
            ("G", "g!G", Colorizer::Green),
            ("B", "b!B", Colorizer::Blue),
        ];

        for (input, expected_text, color) in cases {
            bus.publish(CharacterEvent { text: input.into() })
                .await
                .unwrap();
            assert!(bus.tick());

            assert_eq!(characters.recv().await.unwrap().text, expected_text);
            assert_eq!(
                counts.recv().await.unwrap(),
                RgbaCountEvent::from_colorizer(color)
            );
        }
    }

    #[tokio::test]
    async fn count_keeper_owns_totals_and_publishes_ui_updates() {
        let bus = Bus::with_pipeline(ColorPipeline {
            epoch: Instant::now(),
        });
        let mut keeper = CountKeeper::new(&bus).unwrap();
        let mut updates = bus.consume::<UiCountUpdateEvent>().unwrap();

        let cases = [
            (
                Colorizer::Pass,
                UiCountUpdateEvent {
                    none: 1,
                    red: 0,
                    green: 0,
                    blue: 0,
                },
            ),
            (
                Colorizer::Red,
                UiCountUpdateEvent {
                    none: 1,
                    red: 1,
                    green: 0,
                    blue: 0,
                },
            ),
            (
                Colorizer::Green,
                UiCountUpdateEvent {
                    none: 1,
                    red: 1,
                    green: 1,
                    blue: 0,
                },
            ),
            (
                Colorizer::Blue,
                UiCountUpdateEvent {
                    none: 1,
                    red: 1,
                    green: 1,
                    blue: 1,
                },
            ),
        ];

        for (color, expected) in cases {
            bus.publish(RgbaCountEvent::from_colorizer(color))
                .await
                .unwrap();
            assert!(bus.tick());
            keeper.update().await.unwrap();
            assert!(bus.tick());
            assert_eq!(updates.recv().await.unwrap(), expected);
        }
    }
}
