use std::{future::Future, marker::PhantomData, ops::ControlFlow, sync::Arc};

use dptree::{di::DependencyMap, Handler};

use crate::{HiwayEvent, RecvError, Subscription};

type Tree = Handler<'static, ()>;

/// Typed event dispatcher backed by `dptree`.
pub struct Dispatcher<E: HiwayEvent> {
    tree: Tree,
    dependencies: DependencyMap,
    _event: PhantomData<fn(E)>,
}

impl<E: HiwayEvent> Default for Dispatcher<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: HiwayEvent> Dispatcher<E> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            tree: dptree::entry(),
            dependencies: DependencyMap::new(),
            _event: PhantomData,
        }
    }

    /// Add a typed branch for one payload variant.
    #[must_use]
    pub fn on<V, Handle, Fut>(mut self, handle: Handle) -> Self
    where
        V: TryFrom<E, Error = E> + Clone + Send + Sync + 'static,
        Handle: Fn(V) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let handle = Arc::new(handle);
        let branch: Tree =
            dptree::filter_map(|event: E| V::try_from(event).ok()).endpoint(move |payload: V| {
                let handle = handle.clone();
                async move { (handle)(payload).await }
            });

        self.tree = self.tree.branch(branch);
        self
    }

    /// Add a branch that receives every event enum value.
    #[must_use]
    pub fn on_event<Handle, Fut>(mut self, handle: Handle) -> Self
    where
        Handle: Fn(E) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let handle = Arc::new(handle);
        let branch: Tree = dptree::entry().endpoint(move |event: E| {
            let handle = handle.clone();
            async move { (handle)(event).await }
        });

        self.tree = self.tree.branch(branch);
        self
    }

    /// Add a raw `dptree` branch for advanced routing and dependency use.
    #[must_use]
    pub fn branch(mut self, branch: dptree::Handler<'static, ()>) -> Self {
        self.tree = self.tree.branch(branch);
        self
    }

    /// Add a value available to raw and typed handlers.
    #[must_use]
    pub fn with_dependency<T>(mut self, dependency: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        self.dependencies.insert(dependency);
        self
    }

    /// Dispatch one transformed event. Returns whether a branch handled it.
    #[must_use]
    pub async fn dispatch(&self, event: E) -> bool {
        let mut dependencies = self.dependencies.clone();
        dependencies.insert(event);
        matches!(
            self.tree.dispatch(dependencies).await,
            ControlFlow::Break(())
        )
    }

    /// Continuously receive from a subscription and feed events to the
    /// dispatcher.
    ///
    /// # Errors
    ///
    /// Returns [`RecvError::Closed`] when the bus closes. Returns
    /// [`RecvError::Lagged`] if the subscription misses committed frames.
    pub async fn run(&self, subscription: &mut Subscription<E>) -> Result<(), RecvError> {
        loop {
            let event = subscription.recv().await?;
            let _handled = self.dispatch(event).await;
        }
    }
}

/// A subscription and dispatcher bundled into one listener.
pub struct Listener<E: HiwayEvent> {
    subscription: Subscription<E>,
    dispatcher: Dispatcher<E>,
}

impl<E: HiwayEvent> Listener<E> {
    pub(crate) fn new(subscription: Subscription<E>) -> Self {
        Self {
            subscription,
            dispatcher: Dispatcher::new(),
        }
    }

    /// Add a typed branch for one payload variant.
    #[must_use]
    pub fn on<V, Handle, Fut>(mut self, handle: Handle) -> Self
    where
        V: TryFrom<E, Error = E> + Clone + Send + Sync + 'static,
        Handle: Fn(V) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.dispatcher = self.dispatcher.on(handle);
        self
    }

    /// Add a branch that receives every event enum value.
    #[must_use]
    pub fn on_event<Handle, Fut>(mut self, handle: Handle) -> Self
    where
        Handle: Fn(E) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.dispatcher = self.dispatcher.on_event(handle);
        self
    }

    /// Add a raw `dptree` branch for advanced routing and dependency use.
    #[must_use]
    pub fn branch(mut self, branch: dptree::Handler<'static, ()>) -> Self {
        self.dispatcher = self.dispatcher.branch(branch);
        self
    }

    /// Add a value available to raw and typed handlers.
    #[must_use]
    pub fn with_dependency<T>(mut self, dependency: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        self.dispatcher = self.dispatcher.with_dependency(dependency);
        self
    }

    /// Receive and dispatch one event. Returns whether a branch handled it.
    ///
    /// # Errors
    ///
    /// Returns [`RecvError::Closed`] when the bus closes. Returns
    /// [`RecvError::Lagged`] if the subscription misses committed frames.
    pub async fn next(&mut self) -> Result<bool, RecvError> {
        let event = self.subscription.recv().await?;
        Ok(self.dispatcher.dispatch(event).await)
    }

    /// Run until the subscription closes or reports lag.
    ///
    /// # Errors
    ///
    /// Returns [`RecvError::Closed`] when the bus closes. Returns
    /// [`RecvError::Lagged`] if the subscription misses committed frames.
    pub async fn run(mut self) -> Result<(), RecvError> {
        loop {
            let _handled = self.next().await?;
        }
    }

    #[must_use]
    pub fn subscription(&self) -> &Subscription<E> {
        &self.subscription
    }

    #[must_use]
    pub fn subscription_mut(&mut self) -> &mut Subscription<E> {
        &mut self.subscription
    }
}
