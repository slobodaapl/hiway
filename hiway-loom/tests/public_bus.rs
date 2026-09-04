#![cfg(loom)]

use std::sync::Arc;

use hiway::{DeliveryPolicy, EventId, EventSpec, Hiway, MappedTarget, Receiver, SharedInbox};
use loom::{future::block_on, thread};

struct Value;

impl EventSpec for Value {
    type Payload = u8;
    const ID: EventId = EventId::from_name("loom::value");
}

#[test]
fn direct_sender_and_bounded_inbox_preserve_values() {
    loom::model(|| {
        let hiway = Arc::new(Hiway::new());
        let inbox = SharedInbox::<u8, 2>::new();
        let target = MappedTarget::new(inbox.target(), |value: u8| value);
        let _subscription = hiway
            .fabric()
            .subscribe_mapped::<Value, _, _>(&target, DeliveryPolicy::Reliable)
            .unwrap();
        let sender = hiway.alloc_sender::<Value>().unwrap();

        let first = {
            let sender = sender.clone();
            thread::spawn(move || sender.try_send(1).unwrap())
        };
        let second = thread::spawn(move || sender.try_send(2).unwrap());
        first.join().unwrap();
        second.join().unwrap();

        let mut values = [
            block_on(inbox.recv()).unwrap(),
            block_on(inbox.recv()).unwrap(),
        ];
        values.sort_unstable();
        assert_eq!(values, [1, 2]);
    });
}
