use hiway::{
    events, DynamicFabric, Limits, Permission, Rights, SchemaRevision, StreamConfig, StreamItem,
    SubscriptionRole, UnixLink, WireCodec, WireError,
};
use tokio::net::UnixStream;

#[events]
enum WorkerEvents {
    Progress(u32),
}

impl WireCodec for worker_events::Progress {
    fn encoded_len(_: &u32) -> usize {
        4
    }
    fn encode(value: &u32, output: &mut [u8]) -> Result<usize, WireError> {
        let output = output.get_mut(..4).ok_or(WireError::BufferTooSmall)?;
        output.copy_from_slice(&value.to_le_bytes());
        Ok(4)
    }
    fn decode(bytes: &[u8], revision: SchemaRevision) -> Result<u32, WireError> {
        if revision != SchemaRevision(1) {
            return Err(WireError::UnsupportedRevision);
        }
        Ok(u32::from_le_bytes(
            bytes.try_into().map_err(|_| WireError::InvalidPayload)?,
        ))
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let idle_ms = std::env::args()
        .nth(1)
        .map(|value| value.parse::<u64>())
        .transpose()?
        .unwrap_or(0);
    if idle_ms > 10_000 {
        return Err(std::io::Error::other("idle probe must be at most 10000 milliseconds").into());
    }
    let worker = DynamicFabric::new();
    let ui = DynamicFabric::new();
    worker.create_stream::<worker_events::Progress>(StreamConfig::default())?;
    ui.create_stream::<worker_events::Progress>(StreamConfig::default())?;
    let limits = Limits {
        streams: 1,
        grants: 0,
        subscriptions: 2,
        retained_items: 8,
        waiters: 8,
        connections: 2,
        bytes: 1024,
    };
    let permissions = [Permission::new::<worker_events::Progress>(
        Rights::PUBLISH
            .union(Rights::OBSERVE)
            .union(Rights::REQUIRED),
    )
    .with_limits(hiway::StreamLimits {
        retained_items: 7,
        subscriptions: 2,
        waiters: 6,
    })];
    let worker_grant = worker.grant(&permissions, limits)?;
    let ui_grant = ui.grant(&permissions, limits)?;
    let publisher = worker_grant.sender::<worker_events::Progress>()?;
    let screen = ui_grant.subscribe::<worker_events::Progress>(SubscriptionRole::Required)?;

    // The host provisions these channels. A sandboxed worker would receive
    // only its ends; no socket name or payload ID grants extra authority.
    let (worker_data, ui_data) = UnixStream::pair()?;
    let (worker_control, ui_control) = UnixStream::pair()?;
    let export = UnixLink::export::<worker_events::Progress>(
        worker_grant,
        worker_data,
        worker_control,
        SubscriptionRole::Required,
        4,
    )?;
    let import = UnixLink::import::<worker_events::Progress>(ui_grant, ui_data, ui_control, 4)?;

    let exchange = async {
        if idle_ms != 0 {
            tokio::time::sleep(std::time::Duration::from_millis(idle_ms)).await;
        }
        publisher.send(87).await?;
        match screen.recv().await? {
            StreamItem::Data { value, .. } => println!("UI: worker is {value}% complete"),
            StreamItem::Gap { from, to } => println!("UI: missed progress {from}..{to}"),
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    };
    // The application drives all three futures. Finishing the exchange drops
    // the links and their subscriptions; Hiway starts no runtime or task.
    tokio::select! {
        result = exchange => result?,
        result = export => {
            result?;
            return Err(std::io::Error::other("export ended before observation").into());
        }
        result = import => {
            result?;
            return Err(std::io::Error::other("import ended before observation").into());
        }
    }
    Ok(())
}
