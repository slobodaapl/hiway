use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::Arc,
};

use parking_lot::RwLock;

use crate::{bus::DEFAULT_CAPACITY, Bus, HiwayEvent};

/// Registry for retrieving one typed bus per event enum from unrelated components.
#[derive(Clone, Default)]
pub struct Hiway {
    buses: Arc<RwLock<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>>,
}

impl Hiway {
    /// Creates an empty typed bus registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the shared bus for `E`, creating it with capacity for 256 tick frames.
    ///
    /// # Panics
    ///
    /// Panics if the internal type registry is inconsistent.
    #[must_use]
    pub fn bus<E: HiwayEvent>(&self) -> Bus<E> {
        self.bus_with_capacity(DEFAULT_CAPACITY)
    }

    /// Returns the shared bus for `E`, using `capacity` tick frames when creating it.
    ///
    /// The first lookup for `E` fixes the capacity. Later lookups return the
    /// existing bus and ignore `capacity`.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is zero when creating the bus, or if the internal
    /// type registry is inconsistent.
    #[must_use]
    pub fn bus_with_capacity<E: HiwayEvent>(&self, capacity: usize) -> Bus<E> {
        let mut buses = self.buses.write();
        let type_id = TypeId::of::<E>();

        if let Some(erased) = buses.get(&type_id) {
            let inner = Arc::downcast::<crate::bus::BusInner<E>>(erased.clone())
                .expect("hiway TypeId registry entry has an unexpected bus type");
            return Bus { inner };
        }

        let bus = Bus::<E>::with_capacity(capacity);
        let erased: Arc<dyn Any + Send + Sync> = bus.inner.clone();
        buses.insert(type_id, erased);
        bus
    }
}
