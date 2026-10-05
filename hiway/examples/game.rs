//! A game owns its streams and grants each component only its declared operations.

use hiway::{
    events, port, DynamicFabric, EventPort, EventReceiver, Grant, Limits, Permission, PortExt,
    ReceiveError, Rights, StreamConfig, StreamItem,
};

type GameResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Position {
    x: i32,
    y: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AttackRequest {
    target: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Attack {
    target: u32,
    damage: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MonsterInfo {
    id: u32,
    species: &'static str,
    level: u32,
    max_health: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Weather {
    name: &'static str,
    temperature_c: i16,
}

#[events]
enum GameEvents {
    PlayerMoved(Position),
    PlayerAttackRequested(AttackRequest),
    PlayerAttacked(Attack),
    MonsterSpawned(MonsterInfo),
    MonsterDefeated(u32),
    WeatherChanged(Weather),
}

#[port(
    factory = Grant,
    send(game_events::PlayerAttackRequested),
    recv(
        game_events::PlayerMoved,
        game_events::MonsterSpawned,
        game_events::MonsterDefeated,
        game_events::WeatherChanged,
    )
)]
struct UiPort;

struct Ui<P> {
    port: P,
    frame: u64,
    selected_target: u32,
    notifications: u32,
    missed_events: u64,
    last_position: Option<Position>,
    last_spawn: Option<MonsterInfo>,
    defeated: u32,
    weather: Option<Weather>,
}

#[port(
    factory = Grant,
    send(game_events::PlayerMoved, game_events::PlayerAttacked),
    required(game_events::PlayerAttackRequested)
)]
struct PlayerPort;

struct Player<P> {
    port: P,
    name: &'static str,
    level: u32,
    health: u32,
    max_health: u32,
    mana: u32,
    attack_power: u32,
    armor: u32,
    position: Position,
}

#[port(
    factory = Grant,
    send(
        game_events::MonsterSpawned,
        game_events::MonsterDefeated,
        game_events::WeatherChanged
    ),
    recv(game_events::PlayerMoved, game_events::PlayerAttacked)
)]
struct WorldPort;

struct World<P> {
    port: P,
    difficulty: u8,
    time_of_day: u32,
    weather: Weather,
    active_monsters: u32,
    last_player_position: Option<Position>,
    last_attack: Option<Attack>,
}

#[port(factory = Grant, required(game_events::PlayerAttacked))]
struct MonsterPort;

struct Monster<P> {
    port: P,
    id: u32,
    species: &'static str,
    level: u32,
    health: u32,
    max_health: u32,
    armor: u32,
}

impl<P> Player<P> {
    fn new(port: P) -> Self {
        Self {
            port,
            name: "Aria",
            level: 12,
            health: 100,
            max_health: 100,
            mana: 40,
            attack_power: 9,
            armor: 3,
            position: Position { x: 0, y: 0 },
        }
    }
}

impl<P> Player<P>
where
    P: EventPort<game_events::PlayerMoved>
        + EventPort<game_events::PlayerAttacked>
        + EventReceiver<game_events::PlayerAttackRequested>,
{
    async fn move_to(&mut self, position: Position) -> GameResult {
        self.position = position;
        self.port
            .publish(game_events::PlayerMoved(position))
            .await?;
        Ok(())
    }

    async fn process_attack_request(&self) -> GameResult {
        match self.port.recv_now::<game_events::PlayerAttackRequested>()? {
            Some(StreamItem::Data { value: request, .. }) => {
                self.port
                    .publish(game_events::PlayerAttacked(Attack {
                        target: request.target,
                        damage: self.attack_power,
                    }))
                    .await?;
            }
            Some(StreamItem::Gap { from, to }) => {
                eprintln!("player missed attack requests {from}..{to}");
            }
            None => {}
        }
        Ok(())
    }
}

impl<P> Monster<P> {
    fn new(port: P) -> Self {
        Self {
            port,
            id: 7,
            species: "Mossback",
            level: 4,
            health: 8,
            max_health: 8,
            armor: 1,
        }
    }

    fn info(&self) -> MonsterInfo {
        MonsterInfo {
            id: self.id,
            species: self.species,
            level: self.level,
            max_health: self.max_health,
        }
    }
}

impl<P> Monster<P>
where
    P: EventReceiver<game_events::PlayerAttacked>,
{
    fn process_attack(&mut self) -> Result<bool, ReceiveError> {
        let attack = match self.port.recv_now::<game_events::PlayerAttacked>()? {
            Some(StreamItem::Data { value, .. }) => value,
            Some(StreamItem::Gap { from, to }) => {
                eprintln!("monster {} missed attacks {from}..{to}", self.id);
                return Ok(false);
            }
            None => return Ok(false),
        };
        if attack.target != self.id {
            return Ok(false);
        }
        let damage = attack.damage.saturating_sub(self.armor);
        self.health = self.health.saturating_sub(damage);
        Ok(self.health == 0)
    }
}

impl<P> World<P> {
    fn new(port: P) -> Self {
        Self {
            port,
            difficulty: 3,
            time_of_day: 720,
            weather: Weather {
                name: "clear",
                temperature_c: 18,
            },
            active_monsters: 0,
            last_player_position: None,
            last_attack: None,
        }
    }
}

impl<P> World<P>
where
    P: EventPort<game_events::MonsterSpawned>
        + EventPort<game_events::MonsterDefeated>
        + EventPort<game_events::WeatherChanged>
        + EventReceiver<game_events::PlayerMoved>
        + EventReceiver<game_events::PlayerAttacked>,
{
    async fn spawn<Q>(&mut self, monster: &Monster<Q>) -> GameResult {
        self.active_monsters += 1;
        self.port
            .publish(game_events::MonsterSpawned(monster.info()))
            .await?;
        Ok(())
    }

    async fn defeat(&mut self, monster_id: u32) -> GameResult {
        self.active_monsters = self.active_monsters.saturating_sub(1);
        self.port
            .publish(game_events::MonsterDefeated(monster_id))
            .await?;
        Ok(())
    }

    async fn set_weather(&mut self, weather: Weather) -> GameResult {
        self.weather = weather;
        self.port
            .publish(game_events::WeatherChanged(weather))
            .await?;
        Ok(())
    }

    fn drain_player_events(&mut self) -> Result<(), ReceiveError> {
        while let Some(item) = self.port.recv_now::<game_events::PlayerMoved>()? {
            match item {
                StreamItem::Data { value, .. } => self.last_player_position = Some(*value),
                StreamItem::Gap { from, to } => {
                    eprintln!("world missed player movement {from}..{to}");
                }
            }
        }
        while let Some(item) = self.port.recv_now::<game_events::PlayerAttacked>()? {
            match item {
                StreamItem::Data { value, .. } => self.last_attack = Some(*value),
                StreamItem::Gap { from, to } => {
                    eprintln!("world missed attacks {from}..{to}");
                }
            }
        }
        Ok(())
    }
}

impl<P> Ui<P> {
    fn new(port: P) -> Self {
        Self {
            port,
            frame: 0,
            selected_target: 0,
            notifications: 0,
            missed_events: 0,
            last_position: None,
            last_spawn: None,
            defeated: 0,
            weather: None,
        }
    }
}

impl<P> Ui<P>
where
    P: EventPort<game_events::PlayerAttackRequested>
        + EventReceiver<game_events::PlayerMoved>
        + EventReceiver<game_events::MonsterSpawned>
        + EventReceiver<game_events::MonsterDefeated>
        + EventReceiver<game_events::WeatherChanged>,
{
    async fn request_attack(&mut self, target: u32) -> GameResult {
        self.selected_target = target;
        self.port
            .publish(game_events::PlayerAttackRequested(AttackRequest { target }))
            .await?;
        Ok(())
    }

    fn drain_events(&mut self) -> Result<(), ReceiveError> {
        self.frame += 1;
        while let Some(item) = self.port.recv_now::<game_events::PlayerMoved>()? {
            match item {
                StreamItem::Data { value, .. } => {
                    self.notifications += 1;
                    self.last_position = Some(*value);
                }
                StreamItem::Gap { from, to } => self.missed_events += to - from,
            }
        }
        while let Some(item) = self.port.recv_now::<game_events::MonsterSpawned>()? {
            match item {
                StreamItem::Data { value, .. } => {
                    self.notifications += 1;
                    self.last_spawn = Some(*value);
                }
                StreamItem::Gap { from, to } => self.missed_events += to - from,
            }
        }
        while let Some(item) = self.port.recv_now::<game_events::MonsterDefeated>()? {
            match item {
                StreamItem::Data { .. } => {
                    self.notifications += 1;
                    self.defeated += 1;
                }
                StreamItem::Gap { from, to } => self.missed_events += to - from,
            }
        }
        while let Some(item) = self.port.recv_now::<game_events::WeatherChanged>()? {
            match item {
                StreamItem::Data { value, .. } => {
                    self.notifications += 1;
                    self.weather = Some(*value);
                }
                StreamItem::Gap { from, to } => self.missed_events += to - from,
            }
        }
        Ok(())
    }
}

fn game_fabric() -> GameResult<DynamicFabric> {
    let game_bus = DynamicFabric::new();
    let stream = StreamConfig::default();
    game_bus.create_stream::<game_events::PlayerMoved>(stream)?;
    game_bus.create_stream::<game_events::PlayerAttackRequested>(stream)?;
    game_bus.create_stream::<game_events::PlayerAttacked>(stream)?;
    game_bus.create_stream::<game_events::MonsterSpawned>(stream)?;
    game_bus.create_stream::<game_events::MonsterDefeated>(stream)?;
    game_bus.create_stream::<game_events::WeatherChanged>(stream)?;
    Ok(game_bus)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> GameResult {
    let game_bus = game_fabric()?;
    let publish = hiway::StreamLimits {
        retained_items: 8,
        subscriptions: 0,
        waiters: 1,
    };
    let observe = hiway::StreamLimits {
        retained_items: 0,
        subscriptions: 1,
        waiters: 1,
    };
    let component_limits = Limits {
        streams: 6,
        subscriptions: 4,
        retained_items: 32,
        waiters: 6,
        ..Limits::ZERO
    };
    let ui_grant = game_bus.grant(
        &[
            Permission::new::<game_events::PlayerAttackRequested>(Rights::PUBLISH)
                .with_limits(publish),
            Permission::new::<game_events::PlayerMoved>(Rights::OBSERVE).with_limits(observe),
            Permission::new::<game_events::MonsterSpawned>(Rights::OBSERVE).with_limits(observe),
            Permission::new::<game_events::MonsterDefeated>(Rights::OBSERVE).with_limits(observe),
            Permission::new::<game_events::WeatherChanged>(Rights::OBSERVE).with_limits(observe),
        ],
        component_limits,
    )?;
    let player_grant = game_bus.grant(
        &[
            Permission::new::<game_events::PlayerMoved>(Rights::PUBLISH).with_limits(publish),
            Permission::new::<game_events::PlayerAttacked>(Rights::PUBLISH).with_limits(publish),
            Permission::new::<game_events::PlayerAttackRequested>(
                Rights::OBSERVE | Rights::REQUIRED,
            )
            .with_limits(observe),
        ],
        component_limits,
    )?;
    let world_grant = game_bus.grant(
        &[
            Permission::new::<game_events::MonsterSpawned>(Rights::PUBLISH).with_limits(publish),
            Permission::new::<game_events::MonsterDefeated>(Rights::PUBLISH).with_limits(publish),
            Permission::new::<game_events::WeatherChanged>(Rights::PUBLISH).with_limits(publish),
            Permission::new::<game_events::PlayerMoved>(Rights::OBSERVE).with_limits(observe),
            Permission::new::<game_events::PlayerAttacked>(Rights::OBSERVE).with_limits(observe),
        ],
        component_limits,
    )?;
    let monster_grant = game_bus.grant(
        &[
            Permission::new::<game_events::PlayerAttacked>(Rights::OBSERVE | Rights::REQUIRED)
                .with_limits(observe),
        ],
        component_limits,
    )?;
    let mut ui = Ui::new(UiPort::bind(&ui_grant)?);
    let mut player = Player::new(PlayerPort::bind(&player_grant)?);
    let mut world = World::new(WorldPort::bind(&world_grant)?);
    let mut monster = Monster::new(MonsterPort::bind(&monster_grant)?);

    player.move_to(Position { x: 5, y: 2 }).await?;
    world.spawn(&monster).await?;
    ui.request_attack(monster.id).await?;
    player.process_attack_request().await?;
    if monster.process_attack()? {
        world.defeat(monster.id).await?;
    }
    world
        .set_weather(Weather {
            name: "rain",
            temperature_c: 12,
        })
        .await?;

    ui.drain_events()?;
    world.drain_player_events()?;

    println!(
        "{} (lvl {}, {}/{} HP, {} mana, {} armor) defeated {} (lvl {}, {}/{} HP); world difficulty {}, tick {}, {}°C {}; UI frame {}, {} notifications, {} missed events",
        player.name,
        player.level,
        player.health,
        player.max_health,
        player.mana,
        player.armor,
        monster.species,
        monster.level,
        monster.health,
        monster.max_health,
        world.difficulty,
        world.time_of_day,
        world.weather.temperature_c,
        world.weather.name,
        ui.frame,
        ui.notifications,
        ui.missed_events,
    );
    Ok(())
}
