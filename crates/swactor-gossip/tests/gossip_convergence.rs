use swactor::config::RuntimeConfig;
use swactor::runtime::Runtime;
use swactor_gossip::{GossipActor, GossipMessage, GossipQueryResponse};

fn single_thread_runtime() -> Runtime {
    Runtime::new(RuntimeConfig {
        num_threads: 1,
        ..Default::default()
    })
}

/// Drive ticks until the inbox receives a response, or panic after a limit.
fn recv_query_response(
    rt: &Runtime,
    inbox: &swactor::runtime::Inbox<GossipQueryResponse>,
    max_ticks: usize,
) -> GossipQueryResponse {
    for _ in 0..max_ticks {
        rt.tick();
        if let Some(resp) = inbox.try_recv() {
            return resp;
        }
    }
    panic!("no GossipQueryResponse after {max_ticks} ticks");
}

// ────────────────────────────────────────────────────────────────────────────
// Test 1: Value propagates through a chain A → B → C
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn value_propagates_through_chain() {
    // Given: three gossip nodes wired A→B→C (each only gossips to the next)
    let rt = single_thread_runtime();
    let a = rt.spawn(GossipActor::new()).unwrap();
    let b = rt.spawn(GossipActor::new()).unwrap();
    let c = rt.spawn(GossipActor::new()).unwrap();

    rt.send_to(a, GossipMessage::AddPeer(b)).unwrap();
    rt.send_to(b, GossipMessage::AddPeer(c)).unwrap();
    rt.tick(); // deliver AddPeer messages

    // When: we set a value on A and trigger gossip hops
    rt.send_to(a, GossipMessage::Set {
        key: "color".into(),
        value: b"blue".to_vec(),
    })
    .unwrap();
    rt.tick(); // A processes Set

    rt.send_to(a, GossipMessage::DoGossipRound).unwrap();
    rt.tick(); // A pushes to B
    rt.tick(); // B processes Push

    rt.send_to(b, GossipMessage::DoGossipRound).unwrap();
    rt.tick(); // B pushes to C
    rt.tick(); // C processes Push

    // Then: querying C returns the value that originated at A
    let inbox = rt.new_inbox::<GossipQueryResponse>().unwrap();
    rt.send_to(c, GossipMessage::Query {
        key: "color".into(),
        reply_to: *inbox.addr(),
    })
    .unwrap();

    let resp = recv_query_response(&rt, &inbox, 10);
    assert_eq!(resp.key, "color");
    assert_eq!(resp.value.as_deref(), Some(b"blue".as_slice()));
    assert_eq!(resp.version, Some(1));
}

// ────────────────────────────────────────────────────────────────────────────
// Test 2: Higher version wins during merge
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn higher_version_wins() {
    // Given: two nodes A and B, each set the same key at different versions
    let rt = single_thread_runtime();
    let a = rt.spawn(GossipActor::new()).unwrap();
    let b = rt.spawn(GossipActor::new()).unwrap();

    rt.send_to(a, GossipMessage::AddPeer(b)).unwrap();
    rt.tick();

    // A sets "x" once (version 1)
    rt.send_to(a, GossipMessage::Set {
        key: "x".into(),
        value: b"old".to_vec(),
    })
    .unwrap();
    rt.tick();

    // B sets "x" three times (version 3)
    for val in [b"v1".as_slice(), b"v2", b"new"] {
        rt.send_to(b, GossipMessage::Set {
            key: "x".into(),
            value: val.to_vec(),
        })
        .unwrap();
    }
    rt.tick();

    // When: A pushes its lower-version state to B
    rt.send_to(a, GossipMessage::DoGossipRound).unwrap();
    rt.tick(); // A sends Push
    rt.tick(); // B receives Push

    // Then: B still has the higher-version value
    let inbox = rt.new_inbox::<GossipQueryResponse>().unwrap();
    rt.send_to(b, GossipMessage::Query {
        key: "x".into(),
        reply_to: *inbox.addr(),
    })
    .unwrap();

    let resp = recv_query_response(&rt, &inbox, 10);
    assert_eq!(resp.value.as_deref(), Some(b"new".as_slice()));
    assert_eq!(resp.version, Some(3));
}

// ────────────────────────────────────────────────────────────────────────────
// Test 3: Disjoint keys merge — both nodes end up with both keys
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn disjoint_keys_merge() {
    // Given: A owns key "a", B owns key "b", they are mutual peers
    let rt = single_thread_runtime();
    let a = rt.spawn(GossipActor::new()).unwrap();
    let b = rt.spawn(GossipActor::new()).unwrap();

    rt.send_to(a, GossipMessage::AddPeer(b)).unwrap();
    rt.send_to(b, GossipMessage::AddPeer(a)).unwrap();
    rt.tick();

    rt.send_to(a, GossipMessage::Set {
        key: "a".into(),
        value: b"from-a".to_vec(),
    })
    .unwrap();
    rt.send_to(b, GossipMessage::Set {
        key: "b".into(),
        value: b"from-b".to_vec(),
    })
    .unwrap();
    rt.tick();

    // When: both gossip to each other
    rt.send_to(a, GossipMessage::DoGossipRound).unwrap();
    rt.send_to(b, GossipMessage::DoGossipRound).unwrap();
    rt.tick(); // send Pushes
    rt.tick(); // receive Pushes

    // Then: A has key "b" and B has key "a"
    let inbox = rt.new_inbox::<GossipQueryResponse>().unwrap();

    rt.send_to(a, GossipMessage::Query {
        key: "b".into(),
        reply_to: *inbox.addr(),
    })
    .unwrap();
    let resp = recv_query_response(&rt, &inbox, 10);
    assert_eq!(resp.key, "b");
    assert_eq!(resp.value.as_deref(), Some(b"from-b".as_slice()));

    rt.send_to(b, GossipMessage::Query {
        key: "a".into(),
        reply_to: *inbox.addr(),
    })
    .unwrap();
    let resp = recv_query_response(&rt, &inbox, 10);
    assert_eq!(resp.key, "a");
    assert_eq!(resp.value.as_deref(), Some(b"from-a".as_slice()));
}

// ────────────────────────────────────────────────────────────────────────────
// Test 4: Query for nonexistent key returns None
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn query_nonexistent_key_returns_none() {
    // Given: a gossip node with no data
    let rt = single_thread_runtime();
    let a = rt.spawn(GossipActor::new()).unwrap();
    rt.tick();

    // When: we query a key that was never set
    let inbox = rt.new_inbox::<GossipQueryResponse>().unwrap();
    rt.send_to(a, GossipMessage::Query {
        key: "ghost".into(),
        reply_to: *inbox.addr(),
    })
    .unwrap();

    // Then: response has None value and None version
    let resp = recv_query_response(&rt, &inbox, 10);
    assert_eq!(resp.key, "ghost");
    assert!(resp.value.is_none());
    assert!(resp.version.is_none());
}

// ────────────────────────────────────────────────────────────────────────────
// Test 5: Idempotent push — double-push doesn't bump versions
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn idempotent_push() {
    // Given: A has a key set, B is its peer
    let rt = single_thread_runtime();
    let a = rt.spawn(GossipActor::new()).unwrap();
    let b = rt.spawn(GossipActor::new()).unwrap();

    rt.send_to(a, GossipMessage::AddPeer(b)).unwrap();
    rt.tick();

    rt.send_to(a, GossipMessage::Set {
        key: "k".into(),
        value: b"val".to_vec(),
    })
    .unwrap();
    rt.tick();

    // When: A gossips to B twice (same state, same version)
    for _ in 0..2 {
        rt.send_to(a, GossipMessage::DoGossipRound).unwrap();
        rt.tick(); // send Push
        rt.tick(); // receive Push
    }

    // Then: B's version is still 1 (merge is idempotent, not additive)
    let inbox = rt.new_inbox::<GossipQueryResponse>().unwrap();
    rt.send_to(b, GossipMessage::Query {
        key: "k".into(),
        reply_to: *inbox.addr(),
    })
    .unwrap();

    let resp = recv_query_response(&rt, &inbox, 10);
    assert_eq!(resp.version, Some(1));
    assert_eq!(resp.value.as_deref(), Some(b"val".as_slice()));
}
