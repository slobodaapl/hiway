/// Event value accepted by a [`Bus`](crate::Bus).
///
/// The derive macro accepts enums whose variants each carry one unnamed
/// payload. Use a zero-sized payload struct for a signal without data.
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
pub trait HiwayEvent: Clone + Send + Sync + 'static {}

#[doc(hidden)]
pub trait TransformInput<E>: Sized {
    fn try_from_event(event: E) -> Result<Self, E>;
}
