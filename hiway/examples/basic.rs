use hiway::{Bus, HiwayEvent};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Message(&'static str);

#[derive(Clone, Debug, HiwayEvent)]
enum Events {
    Message(Message),
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let bus = Bus::default().transform(async |mut message: Message| {
        message.0 = "transformed";
        [message.into()]
    });
    let mut messages = bus.consume::<Message>().unwrap();

    bus.publish(Message("input")).await.unwrap();
    assert!(bus.tick());
    assert_eq!(messages.recv().await.unwrap(), Message("transformed"));
}
