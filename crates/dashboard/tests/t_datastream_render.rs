//! Dashboard render contract: source records → frames → reconstructed views.
//!
//! Feeds the real `FleetView` consumer a scripted set of one node's frames and
//! checks the `FleetUpdate` it produces carries the migrated metrics: the
//! distribution panels show real cache / registry / directory / peer-auth
//! values, the Actors table shows the real per-actor rows, and the datastore
//! view shows the real counters and recent events. Every expectation is derived
//! from the input records, never read back from the consumer, and the test
//! asserts on the reconstructed JSON / stats — not on rendered HTML — so it
//! survives a UI refactor.

use dashboard::datastream_source::{FleetUpdate, FleetView};
use datastream::catalog::{
    self, ActorRec, ActorRuntimeDetail, DatastoreState, DistributionState, IdentityRecord, ObjectRec,
    Record,
};
use datastream::frame::{Frame, Lifetime, NodeId, Position, StreamId};

fn node_stream() -> StreamId {
    StreamId::new(NodeId::new(&"ab".repeat(32)), Lifetime(1))
}

fn frame<R: Record>(record: &R, pos: u64) -> Frame {
    Frame::new(R::channel(), Position(pos), record.encode())
}

fn identity(stream: &StreamId, node_name: &str) -> IdentityRecord {
    IdentityRecord {
        node: stream.node.as_str().to_string(),
        life: stream.life.0,
        node_name: node_name.to_string(),
        listen_addr: "host:4242".to_string(),
        relay_url: String::new(),
        version: "v1".to_string(),
    }
}

/// Fold a script of frames for one node and return the final `FleetUpdate`.
fn ingest_all(view: &mut FleetView, stream: &StreamId, frames: &[Frame]) -> FleetUpdate {
    let mut last = None;
    for f in frames {
        last = Some(view.ingest(stream, f));
    }
    last.expect("at least one frame")
}

#[test]
fn distribution_panels_render_real_state_from_frames() {
    let stream = node_stream();
    let mut view = FleetView::new(None);

    let dist = DistributionState {
        cache_size: 4,
        directory_route_count: 9,
        registry_size: 6,
        registry_tombstones: 1,
        peer_auth_mode: "allow-list".into(),
        authorized_peer_count: 3,
        ..Default::default()
    };
    let frames = vec![
        frame(&identity(&stream, "swift-falcon"), 0),
        frame(&dist, 1),
    ];
    let update = ingest_all(&mut view, &stream, &frames);

    let dj: serde_json::Value =
        serde_json::from_str(update.dist_json.as_ref().expect("dist json for selected node"))
            .unwrap();
    // The migrated distribution metrics arrive as real values (not the zeros the
    // pre-datastream reconstruction used to fill).
    assert_eq!(dj["cache_size"], 4);
    assert_eq!(dj["directory_route_count"], 9);
    assert_eq!(dj["registry_size"], 6);
    assert_eq!(dj["registry_tombstones"], 1);
    assert_eq!(dj["peer_auth_mode"], "allow-list");
    assert_eq!(dj["authorized_peer_count"], 3);
    // Identity extras ride the identity channel.
    assert_eq!(dj["node_name"], "swift-falcon");
    assert_eq!(dj["listen_addr"], "host:4242");
    assert_eq!(dj["version"], "v1");
}

#[test]
fn actors_table_renders_real_per_actor_rows() {
    let stream = node_stream();
    let mut view = FleetView::new(None);

    let detail = ActorRuntimeDetail {
        actors: vec![
            ActorRec {
                address: "ab".repeat(32),
                name: "SwimActor".into(),
                mailbox_depth: 2,
                messages_processed: 42,
                last_msg_type: "Ping".into(),
                poisoned: false,
                message_type_counts: vec![("Ping".into(), 42)],
            },
            ActorRec {
                address: "cd".repeat(32),
                name: "RegistryActor".into(),
                mailbox_depth: 0,
                messages_processed: 7,
                last_msg_type: "Tick".into(),
                poisoned: true,
                message_type_counts: vec![],
            },
        ],
    };
    let frames = vec![frame(&identity(&stream, "node"), 0), frame(&detail, 1)];
    let update = ingest_all(&mut view, &stream, &frames);
    let stats = update.stats.expect("stats for the selected node");

    // The real per-actor rows are rendered (not synthetic per-channel rows): the
    // names, message tallies, and poisoned flag carry through unchanged.
    let swim = stats
        .actor_details
        .iter()
        .find(|a| a.name.as_deref() == Some("SwimActor"))
        .expect("SwimActor row");
    assert_eq!(swim.messages_processed, 42);
    assert_eq!(swim.mailbox_depth, 2);
    assert!(!swim.poisoned);

    let registry = stats
        .actor_details
        .iter()
        .find(|a| a.name.as_deref() == Some("RegistryActor"))
        .expect("RegistryActor row");
    assert!(registry.poisoned, "poisoned flag carried through");
}

#[test]
fn datastore_view_renders_counters_and_recent_events() {
    let stream = node_stream();
    let mut view = FleetView::new(None);

    let ds = DatastoreState {
        object_count: 3,
        total_bytes: 4096,
        put_ops: 5,
        get_ops: 9,
        delete_ops: 1,
        objects: vec![ObjectRec {
            hash: "h1".into(),
            name: Some("a.bin".into()),
            size_bytes: 1024,
        }],
        active_transfers: vec![],
    };
    let event = serde_json::json!({
        "timestamp_ms": 1u64, "kind": "put", "hash": "h1", "name": "a.bin", "size_bytes": 1024
    })
    .to_string();
    let frames = vec![
        frame(&identity(&stream, "node"), 0),
        frame(&ds, 1),
        Frame::new(catalog::datastore_event(), Position(2), event.into_bytes()),
    ];
    let update = ingest_all(&mut view, &stream, &frames);

    let dj: serde_json::Value =
        serde_json::from_str(update.datastore_json.as_ref().expect("datastore json"))
            .unwrap();
    assert_eq!(dj["object_count"], 3);
    assert_eq!(dj["total_bytes"], 4096);
    assert_eq!(dj["put_ops"], 5);
    assert_eq!(dj["get_ops"], 9);
    assert_eq!(dj["objects"][0]["hash"], "h1");
    // The op event streamed on the text channel is rebuilt into the timeline.
    let events = dj["recent_events"].as_array().expect("recent events array");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["kind"], "put");
    assert_eq!(events[0]["hash"], "h1");
}
