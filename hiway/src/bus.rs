use std::{future::Future, marker::PhantomData, sync::Arc};

use parking_lot::{Mutex, RwLock};
use tokio::sync::broadcast;

use crate::{event::TransformInput, transform::Transform, HiwayEvent, RecvError};

pub(crate) const DEFAULT_CAPACITY: usize = 256;

type Batch<E> = Arc<[E]>;
type Frame<E> = Arc<Vec<Batch<E>>>;

pub(crate) struct BusInner<E: HiwayEvent> {
    tx: broadcast::Sender<Frame<E>>,
    // Serializes the swap and send so concurrent ticks cannot reorder frames.
    commit: Mutex<()>,
    // Prepared publications accumulate without a fixed bound between ticks.
    // Tick swaps this Vec without iterating through it.
    ready: Mutex<Vec<Batch<E>>>,
    transforms: RwLock<Vec<Transform<E>>>,
}

/// A typed in-process event bus.
pub struct Bus<E: HiwayEvent> {
    pub(crate) inner: Arc<BusInner<E>>,
}

impl<E: HiwayEvent> Clone for Bus<E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<E: HiwayEvent> Default for Bus<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: HiwayEvent> Bus<E> {
    /// Creates a bus with capacity for 256 committed tick frames.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    /// Creates a bus with capacity for `capacity` committed tick frames.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is zero.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "hiway bus capacity must be greater than zero");
        let (tx, _initial_rx) = broadcast::channel(capacity);
        Self {
            inner: Arc::new(BusInner {
                tx,
                commit: Mutex::new(()),
                ready: Mutex::new(Vec::new()),
                transforms: RwLock::new(Vec::new()),
            }),
        }
    }

    /// Append a synchronous transformation stage for one payload type or the
    /// complete event enum. Return no events to drop the input, one to replace
    /// it, or several to expand it. Other payload types pass through unchanged.
    pub fn transform<V, F>(&self, transform: F)
    where
        V: TransformInput<E> + Send + 'static,
        F: Fn(V) -> Vec<E> + Send + Sync + 'static,
    {
        self.inner
            .transforms
            .write()
            .push(Transform::new(transform));
    }

    /// Append the asynchronous form of [`transform`](Self::transform).
    pub fn transform_async<V, F, Fut>(&self, transform: F)
    where
        V: TransformInput<E> + Send + 'static,
        F: Fn(V) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Vec<E>> + Send + 'static,
    {
        self.inner
            .transforms
            .write()
            .push(Transform::new_async(transform));
    }

    /// Transform and prepare one event value for the next [`tick`](Self::tick).
    ///
    /// The stages are snapshotted before the first asynchronous transformation
    /// runs. Each stage receives every output from the previous stage, and
    /// each final output is queued in stage order as one atomic batch.
    pub async fn publish<V>(&self, event: V)
    where
        V: Into<E>,
    {
        let transforms = self.inner.transforms.read().clone();
        let mut events = vec![event.into()];

        for transform in transforms {
            let mut next = Vec::new();
            for event in events {
                next.extend(transform.apply(event).await);
            }
            events = next;
            if events.is_empty() {
                break;
            }
        }

        if !events.is_empty() {
            self.inner.ready.lock().push(events.into());
        }
    }

    /// Commit every prepared publication currently queued as one tick frame.
    #[must_use]
    pub fn tick(&self) -> bool {
        let Some(commit) = self.inner.commit.try_lock() else {
            return false;
        };
        let frame = {
            let Some(mut ready) = self.inner.ready.try_lock() else {
                return false;
            };
            if ready.is_empty() {
                return false;
            }
            Arc::new(std::mem::take(&mut *ready))
        };

        let sent = self.inner.tx.send(frame);
        drop(commit);
        drop(sent);
        true
    }

    /// Subscribe to every final event broadcast by this bus.
    #[must_use]
    pub fn subscribe(&self) -> Subscription<E> {
        Subscription {
            rx: self.inner.tx.subscribe(),
            frame: None,
            batch_index: 0,
            event_index: 0,
        }
    }

    /// Subscribe to one derived event payload and skip other variants locally.
    #[must_use]
    pub fn consume<V>(&self) -> TypedSubscription<E, V>
    where
        V: TryFrom<E, Error = E>,
    {
        self.subscribe().consume()
    }

    /// Build a dispatching listener on this bus.
    #[cfg(feature = "dispatch")]
    #[must_use]
    pub fn listen(&self) -> crate::Listener<E> {
        crate::Listener::new(self.subscribe())
    }
}

/// Raw subscriber receiving every final event.
pub struct Subscription<E: HiwayEvent> {
    rx: broadcast::Receiver<Frame<E>>,
    frame: Option<Frame<E>>,
    batch_index: usize,
    event_index: usize,
}

impl<E: HiwayEvent> Subscription<E> {
    /// Receives the next committed event.
    ///
    /// # Errors
    ///
    /// Returns [`RecvError::Closed`] after every bus handle is dropped and all
    /// buffered frames are read. Returns [`RecvError::Lagged`] if this
    /// subscriber misses committed frames.
    pub async fn recv(&mut self) -> Result<E, RecvError> {
        loop {
            if let Some(frame) = &self.frame {
                if let Some(batch) = frame.get(self.batch_index) {
                    if let Some(event) = batch.get(self.event_index) {
                        self.event_index += 1;
                        return Ok(event.clone());
                    }
                    self.batch_index += 1;
                    self.event_index = 0;
                    continue;
                }
            }

            self.frame = Some(self.rx.recv().await.map_err(RecvError::from)?);
            self.batch_index = 0;
            self.event_index = 0;
        }
    }

    /// Convert this subscriber into one that accepts only payload `V`.
    #[must_use]
    pub fn consume<V>(self) -> TypedSubscription<E, V>
    where
        V: TryFrom<E, Error = E>,
    {
        TypedSubscription {
            inner: self,
            _payload: PhantomData,
        }
    }
}

/// Subscriber that extracts one payload type from a derived event enum.
pub struct TypedSubscription<E, V>
where
    E: HiwayEvent,
    V: TryFrom<E, Error = E>,
{
    inner: Subscription<E>,
    _payload: PhantomData<fn() -> V>,
}

impl<E, V> TypedSubscription<E, V>
where
    E: HiwayEvent,
    V: TryFrom<E, Error = E>,
{
    /// Receives the next committed payload of type `V`.
    ///
    /// # Errors
    ///
    /// Returns [`RecvError::Closed`] after every bus handle is dropped and all
    /// buffered frames are read. Returns [`RecvError::Lagged`] if this
    /// subscriber misses committed frames.
    pub async fn recv(&mut self) -> Result<V, RecvError> {
        loop {
            match V::try_from(self.inner.recv().await?) {
                Ok(payload) => return Ok(payload),
                Err(_other_variant) => {}
            }
        }
    }
}
