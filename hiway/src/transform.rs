use std::{future::Future, pin::Pin, sync::Arc};

use crate::event::TransformInput;

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;
type Apply<E> = dyn Fn(E) -> BoxFuture<Vec<E>> + Send + Sync + 'static;

struct TransformInner<E> {
    apply: Arc<Apply<E>>,
}

pub(crate) struct Transform<E> {
    inner: Arc<TransformInner<E>>,
}

impl<E> Clone for Transform<E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<E: Send + 'static> Transform<E> {
    pub(crate) fn new<V, F>(f: F) -> Self
    where
        V: TransformInput<E> + Send + 'static,
        F: Fn(V) -> Vec<E> + Send + Sync + 'static,
    {
        Self::new_async(move |input| {
            let events = f(input);
            async move { events }
        })
    }

    pub(crate) fn new_async<V, F, Fut>(f: F) -> Self
    where
        V: TransformInput<E> + Send + 'static,
        F: Fn(V) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Vec<E>> + Send + 'static,
    {
        let apply = Arc::new(move |event: E| -> BoxFuture<Vec<E>> {
            match V::try_from_event(event) {
                Ok(input) => Box::pin(f(input)),
                Err(other) => Box::pin(async move { vec![other] }),
            }
        });
        Self {
            inner: Arc::new(TransformInner { apply }),
        }
    }

    pub(crate) async fn apply(&self, event: E) -> Vec<E> {
        (self.inner.apply)(event).await
    }
}
