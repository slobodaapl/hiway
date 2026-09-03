/// Event enum accepted by a [`Bus`](crate::Bus).
///
/// The derive macro accepts enums whose variants each carry one unnamed
/// payload. It generates conversions plus a hidden ordinal tag used to route
/// typed subscriptions before subscriber storage and fanout. Use a zero-sized
/// payload struct for a signal without data.
///
/// ```compile_fail
/// use hiway::HiwayEvent;
///
/// #[derive(HiwayEvent)]
/// enum Unit {
///     Empty,
/// }
/// ```
///
/// ```compile_fail
/// use hiway::HiwayEvent;
///
/// #[derive(HiwayEvent)]
/// enum Named {
///     Value { text: String },
/// }
/// ```
///
/// ```compile_fail
/// use hiway::HiwayEvent;
///
/// #[derive(HiwayEvent)]
/// enum Many {
///     Value(String, usize),
/// }
/// ```
///
/// ```compile_fail
/// use hiway::HiwayEvent;
///
/// #[derive(HiwayEvent)]
/// enum Duplicate {
///     First(String),
///     Second(String),
/// }
/// ```
pub trait HiwayEvent: Clone + Send {
    #[doc(hidden)]
    fn __hiway_tag(&self) -> usize {
        0
    }
}

#[doc(hidden)]
pub trait EventTag<E> {
    #[doc(hidden)]
    fn __hiway_tag() -> usize;
}

#[doc(hidden)]
pub trait TransformInput<E>: Sized {
    fn try_from_event(event: E) -> Result<Self, E>;
}

impl<E> TransformInput<E> for E {
    fn try_from_event(event: E) -> Result<Self, E> {
        Ok(event)
    }
}
