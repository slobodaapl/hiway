//! Small std-hosted game loop with one injected event bus.

use hiway::{events, port, EventPort, EventReceiver, PortExt, SendError};

type GameBus = hiway::DynamicFabric;

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
    factory = GameBus,
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
    last_position: Option<Position>,
    last_spawn: Option<MonsterInfo>,
    defeated: u32,
    weather: Option<Weather>,
}

#[port(
    factory = GameBus,
    send(game_events::PlayerMoved, game_events::PlayerAttacked),
    recv(game_events::PlayerAttackRequested)
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
    factory = GameBus,
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

#[port(factory = GameBus, recv(game_events::PlayerAttacked))]
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
    async fn move_to(&mut self, position: Position) -> Result<(), SendError> {
        self.position = position;
        self.port.publish(game_events::PlayerMoved(position)).await
    }

    async fn process_attack_request(&self) -> Result<(), SendError> {
        let Some(request) = self.port.try_recv::<game_events::PlayerAttackRequested>() else {
            return Ok(());
        };
        self.port
            .publish(game_events::PlayerAttacked(Attack {
                target: request.target,
                damage: self.attack_power,
            }))
            .await
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
    fn process_attack(&mut self) -> bool {
        let Some(attack) = self.port.try_recv::<game_events::PlayerAttacked>() else {
            return false;
        };
        if attack.target != self.id {
            return false;
        }
        let damage = attack.damage.saturating_sub(self.armor);
        self.health = self.health.saturating_sub(damage);
        self.health == 0
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
    async fn spawn<Q>(&mut self, monster: &Monster<Q>) -> Result<(), SendError> {
        self.active_monsters += 1;
        self.port
            .publish(game_events::MonsterSpawned(monster.info()))
            .await
    }

    async fn defeat(&mut self, monster_id: u32) -> Result<(), SendError> {
        self.active_monsters = self.active_monsters.saturating_sub(1);
        self.port
            .publish(game_events::MonsterDefeated(monster_id))
            .await
    }

    async fn set_weather(&mut self, weather: Weather) -> Result<(), SendError> {
        self.weather = weather;
        self.port
            .publish(game_events::WeatherChanged(weather))
            .await
    }

    fn drain_player_events(&mut self) {
        while let Some(position) = self.port.try_recv::<game_events::PlayerMoved>() {
            self.last_player_position = Some(position);
        }
        while let Some(attack) = self.port.try_recv::<game_events::PlayerAttacked>() {
            self.last_attack = Some(attack);
        }
    }
}

impl<P> Ui<P> {
    fn new(port: P) -> Self {
        Self {
            port,
            frame: 0,
            selected_target: 0,
            notifications: 0,
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
    async fn request_attack(&mut self, target: u32) -> Result<(), SendError> {
        self.selected_target = target;
        self.port
            .publish(game_events::PlayerAttackRequested(AttackRequest { target }))
            .await
    }

    fn drain_events(&mut self) {
        self.frame += 1;
        while let Some(position) = self.port.try_recv::<game_events::PlayerMoved>() {
            self.notifications += 1;
            self.last_position = Some(position);
        }
        while let Some(info) = self.port.try_recv::<game_events::MonsterSpawned>() {
            self.notifications += 1;
            self.last_spawn = Some(info);
        }
        while self
            .port
            .try_recv::<game_events::MonsterDefeated>()
            .is_some()
        {
            self.notifications += 1;
            self.defeated += 1;
        }
        while let Some(weather) = self.port.try_recv::<game_events::WeatherChanged>() {
            self.notifications += 1;
            self.weather = Some(weather);
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let game_bus = GameBus::new();
    let mut ui = Ui::new(UiPort::bind(&game_bus)?);
    let mut player = Player::new(PlayerPort::bind(&game_bus)?);
    let mut world = World::new(WorldPort::bind(&game_bus)?);
    let mut monster = Monster::new(MonsterPort::bind(&game_bus)?);

    player.move_to(Position { x: 5, y: 2 }).await?;
    world.spawn(&monster).await?;
    ui.request_attack(monster.id).await?;
    player.process_attack_request().await?;
    if monster.process_attack() {
        world.defeat(monster.id).await?;
    }
    world
        .set_weather(Weather {
            name: "rain",
            temperature_c: 12,
        })
        .await?;

    ui.drain_events();
    world.drain_player_events();

    println!(
        "{} (lvl {}, {}/{} HP, {} mana, {} armor) defeated {} (lvl {}, {}/{} HP); world difficulty {}, tick {}, {}°C {}; UI frame {}, {} notifications",
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
    );
    Ok(())
}
