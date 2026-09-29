//! Replaceable synchronization for caller-owned streams.

use core::ops::DerefMut;

pub trait Lock<T> {
    type Guard<'a>: DerefMut<Target = T>
    where
        Self: 'a,
        T: 'a;
    fn lock(&self) -> Self::Guard<'_>;
    fn try_lock(&self) -> Option<Self::Guard<'_>>;
}

pub trait LockFamily {
    type Lock<T>: Lock<T>;
    fn wrap<T>(self, value: T) -> Self::Lock<T>;
}

#[derive(Clone, Copy, Default)]
pub struct Spin;

#[cfg(loom)]
pub(crate) use loom::sync::Mutex as DefaultMutex;
#[cfg(not(loom))]
pub(crate) use spin::Mutex as DefaultMutex;

impl LockFamily for Spin {
    type Lock<T> = DefaultMutex<T>;
    fn wrap<T>(self, value: T) -> Self::Lock<T> {
        DefaultMutex::new(value)
    }
}

#[cfg(not(loom))]
impl<T> Lock<T> for spin::Mutex<T> {
    type Guard<'a>
        = spin::MutexGuard<'a, T>
    where
        T: 'a;
    fn lock(&self) -> Self::Guard<'_> {
        self.lock()
    }
    fn try_lock(&self) -> Option<Self::Guard<'_>> {
        self.try_lock()
    }
}

#[cfg(loom)]
impl<T> Lock<T> for loom::sync::Mutex<T> {
    type Guard<'a>
        = loom::sync::MutexGuard<'a, T>
    where
        T: 'a;
    fn lock(&self) -> Self::Guard<'_> {
        self.lock().unwrap()
    }
    fn try_lock(&self) -> Option<Self::Guard<'_>> {
        self.try_lock().ok()
    }
}
