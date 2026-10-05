use hiway::{events, graph, StaticStream};

#[events]
enum Events {
    Done(u32),
}

#[graph(done = (events::Done, 2, 2, 2))]
struct BindingGraph;

mod port_identifier {
    use super::{events, BindingGraph, StaticStream};
    use hiway::port;

    #[port(send(events::Done))]
    struct __Sender0;

    #[port(recv(events::Done))]
    struct Receiver;

    #[test]
    fn internal_looking_port_name_binds_and_routes() {
        let stream = StaticStream::new();
        let graph = BindingGraph::new(&stream);
        let sender = __Sender0::bind(&graph).unwrap();
        let receiver = Receiver::bind(&graph).unwrap();

        sender.publish_now_done(73).unwrap();
        let item = receiver.recv_now_done().unwrap().unwrap();

        match item {
            hiway::StreamItem::Data { value, .. } => assert_eq!(*value, 73),
            hiway::StreamItem::Gap { .. } => panic!("unexpected gap"),
        }
    }
}

mod event_alias {
    use super::{events, BindingGraph, StaticStream};
    use events::Done as __Factory;
    use hiway::port;

    #[port(send(__Factory))]
    struct Sender;

    #[test]
    fn unqualified_event_alias_binds() {
        let stream = StaticStream::new();
        let graph = BindingGraph::new(&stream);

        let _sender = Sender::bind(&graph).unwrap();
    }
}
