//! Low-level serve/listener primitives for the nanobpm gateway, extracted (ADR
//! 0064 Phase 3) so both the gateway binary and the console crate can name them.
//!
//! - [`NoDelayListener`] — an [`axum::serve::Listener`] wrapper that disables
//!   Nagle on every accepted connection.
//! - [`PeerAddr`] — the `ConnectInfo` newtype the binary registers via
//!   [`axum::serve::IncomingStream`]. The console's loopback-gated filesystem
//!   browser extracts `ConnectInfo<PeerAddr>`, so it must name the *exact* type
//!   the binary wired in. Because the [`Connected`] impl references the local
//!   [`NoDelayListener`], the type and the impl are co-located here to satisfy
//!   the orphan rule.
//!
//! [`Connected`]: axum::extract::connect_info::Connected

use std::net::SocketAddr;

/// An [`axum::serve::Listener`] wrapper that disables Nagle (`TCP_NODELAY`) on
/// every accepted connection. The gateway's WebSocket surfaces — the SDK command
/// stream and the inter-node peer/Raft lane — exchange small, latency-sensitive
/// request/response frames; with Nagle + delayed-ACK each round-trip can stall
/// ~40 ms, which collapses Raft commit and job-stream throughput. The frames are
/// explicitly length-delimited, so there is nothing to gain from TCP-level
/// coalescing. (The client/dialling side sets the same option in the peer uplink.)
pub struct NoDelayListener(pub tokio::net::TcpListener);

impl axum::serve::Listener for NoDelayListener {
    type Io = tokio::net::TcpStream;
    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            match self.0.accept().await {
                Ok((stream, addr)) => {
                    let _ = stream.set_nodelay(true);
                    return (stream, addr);
                }
                // Mirror axum's own TcpListener accept: a transient accept error
                // (e.g. fd exhaustion) is retried after a short backoff rather
                // than tearing down the server.
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(1)).await,
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.0.local_addr()
    }
}

/// Connection peer address, wired through `ConnectInfo` so handlers can tell a
/// loopback client from a remote one (the console's filesystem browser is
/// loopback-only). A local newtype is required because the orphan rule forbids
/// implementing axum's `Connected` for the foreign `SocketAddr` directly.
#[derive(Clone, Copy)]
pub struct PeerAddr(
    // Read only by the console's loopback-gated filesystem browser; a server
    // built without the `console` feature still carries it but never inspects it.
    pub SocketAddr,
);

/// Enables `ConnectInfo<PeerAddr>` extraction when the app is served over the
/// custom [`NoDelayListener`]; axum ships a `Connected` impl for the stock
/// `TcpListener` but not for a wrapper, so we forward the peer address the
/// listener already yields.
impl axum::extract::connect_info::Connected<axum::serve::IncomingStream<'_, NoDelayListener>>
    for PeerAddr
{
    fn connect_info(stream: axum::serve::IncomingStream<'_, NoDelayListener>) -> Self {
        PeerAddr(*stream.remote_addr())
    }
}
