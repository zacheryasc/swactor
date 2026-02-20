use swactor::actor::ActorAddress;

/// Typed proxy wrapping a service address for ergonomic domain-specific APIs.
///
/// Implement this trait on a struct that wraps a service address and provides
/// domain-specific methods. Methods take `&self` + `&Ctx` (not stored `&Ctx` —
/// avoids lifetime issues with `&mut self` in handlers).
///
/// # Example
///
/// ```ignore
/// struct CounterHandle {
///     service: ActorAddress,
///     self_addr: ActorAddress,
/// }
///
/// impl ResourceHandle for CounterHandle {
///     type Service = CounterService;
///     fn from_parts(service_addr: ActorAddress, self_addr: ActorAddress) -> Self {
///         Self { service: service_addr, self_addr }
///     }
///     fn service_addr(&self) -> ActorAddress { self.service }
///     fn self_addr(&self) -> ActorAddress { self.self_addr }
/// }
///
/// impl CounterHandle {
///     pub fn increment(&self, ctx: &Ctx) -> Result<(), Error> {
///         ctx.send(self.service_addr(), Increment { reply_to: self.self_addr() })
///     }
/// }
/// ```
pub trait ResourceHandle: Sized {
    /// Marker type identifying the service (same `S` used with `ServiceRegistry`).
    type Service: 'static + Send + Sync;

    /// Construct a handle from a service address and the calling actor's address.
    fn from_parts(service_addr: ActorAddress, self_addr: ActorAddress) -> Self;

    /// The address of the underlying service actor.
    fn service_addr(&self) -> ActorAddress;

    /// The address of the actor holding this handle (for reply_to patterns).
    fn self_addr(&self) -> ActorAddress;
}
