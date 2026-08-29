//! Cross-crate peer-uplink integration tests (ADR 0064 Phase 2).
//!
//! `peer.rs` moved into the `nano-server-raft` library crate, but these tests
//! drive a **real** intra-cluster falcon server — `ServerImpl` plus the falcon
//! dispatcher/router — which lives in this binary and therefore cannot be a
//! dependency of the raft library crate. They stay here, exercising the
//! re-exported `crate::peer` client against an in-process gateway exactly as
//! they did inline before the extraction. (Full inline-test relocation is the
//! ADR 0064 Phase 4 concern; this is the minimum the extraction forces.)

use crate::peer::{PeerLink, PeerSet};

/// Serves a real intra-cluster falcon endpoint on an ephemeral port and
/// returns its HTTP base URL. Models a peer node: a `PeerLink` connects to
/// its `/cluster` channel exactly as a forwarding gateway would in a cluster.
async fn serve_peer() -> String {
    let server = crate::ServerImpl::default();
    let registry = crate::falcon::Registry::new();
    crate::falcon::spawn_dispatcher(server.clone(), registry.clone());
    let app = crate::falcon::cluster_router(server, registry, None);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    format!("http://127.0.0.1:{port}")
}

/// The uplink drives a peer's engine over the Falcon protocol: a forwarded
/// `createProcessInstance` runs on the peer and its `CommandResult` is mapped
/// straight back. This is the transport every stage-1 forwarding op rides on.
#[tokio::test]
async fn peer_link_forwards_create_instance() {
    let base = serve_peer().await;
    let link = PeerLink::connect(&base).await.expect("connect to peer");
    assert!(link.is_connected());

    let res = link
        .create_instance(Some("demo".to_string()), None, None)
        .await
        .expect("forwarded create returns a result");
    assert_eq!(res.status, 200, "peer should accept the forwarded create");
    let body = res.body.expect("create result carries a body");
    assert!(
        body.get("processInstanceKey").is_some(),
        "result should carry the peer-minted processInstanceKey, got {body}"
    );
}

/// Two independent forwarded creates get distinct correlation ids and both
/// resolve — proving the correlation table routes responses correctly.
#[tokio::test]
async fn peer_link_correlates_concurrent_requests() {
    let base = serve_peer().await;
    let link = PeerLink::connect(&base).await.expect("connect to peer");

    let (a, b) = tokio::join!(
        link.create_instance(Some("demo".to_string()), None, None),
        link.create_instance(Some("demo".to_string()), None, None),
    );
    let ka = a.expect("first create").body.unwrap();
    let kb = b.expect("second create").body.unwrap();
    assert_ne!(
        ka.get("processInstanceKey"),
        kb.get("processInstanceKey"),
        "two creates must mint distinct instance keys"
    );
}

/// `PeerSet` dials a peer lazily from the topology address and reuses the live
/// link on the next call — the connection manager every forwarding op uses.
#[tokio::test]
async fn peer_set_dials_lazily_and_forwards() {
    let peer_base = serve_peer().await;
    // Node 0 of a 2-node cluster; node 1 is the served peer.
    let topology = crate::cluster::Topology {
        node_id: 0,
        peers: vec!["http://self-unused".to_string(), peer_base],
        num_partitions: 4,
        replication_factor: 1,
    };
    let peers = PeerSet::new(topology);
    assert!(peers.has_peers());

    let link = peers.link(1).await.expect("dial peer node 1");
    let res = link
        .create_instance(Some("demo".to_string()), None, None)
        .await
        .expect("forwarded create");
    assert_eq!(res.status, 200);

    // Second call reuses the cached, still-connected link (no redial).
    let link2 = peers.link(1).await.expect("reuse cached link");
    assert!(link2.is_connected());
}
