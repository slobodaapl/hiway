#[cfg(loom)]
pub(crate) use loom::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Mutex, MutexGuard,
};
#[cfg(not(loom))]
pub(crate) use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Mutex, MutexGuard,
};

pub(crate) use std::sync::TryLockError;

#[cfg(not(loom))]
pub(crate) use arc_swap::ArcSwap as Snapshot;

// Model ArcSwap's SeqCst pointer publication, not its reclamation algorithm.
// Immutable versions live in an uninstrumented side table: bookkeeping must not
// introduce synchronization edges absent from the production atomic pointer.
#[cfg(loom)]
pub(crate) struct Snapshot<T> {
    current: AtomicUsize,
    versions: std::sync::Mutex<Vec<std::sync::Arc<T>>>,
}

#[cfg(loom)]
impl<T> Snapshot<T> {
    pub(crate) fn from_pointee(value: T) -> Self {
        Self {
            current: AtomicUsize::new(0),
            versions: std::sync::Mutex::new(vec![std::sync::Arc::new(value)]),
        }
    }

    pub(crate) fn load(&self) -> std::sync::Arc<T> {
        let index = self.current.load(Ordering::SeqCst);
        self.versions.lock().unwrap()[index].clone()
    }

    pub(crate) fn load_full(&self) -> std::sync::Arc<T> {
        self.load()
    }

    pub(crate) fn store(&self, value: std::sync::Arc<T>) {
        let index = {
            let mut versions = self.versions.lock().unwrap();
            let index = versions.len();
            versions.push(value);
            index
        };
        self.current.swap(index, Ordering::SeqCst);
    }
}
