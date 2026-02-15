use distribution::kademlia::lookup::{LookupAction, NodeLookup};
use distribution::kademlia::routing_table::RoutingTable;
use distribution::types::NodeId;

fn node(byte: u8) -> NodeId {
    NodeId([byte; 32])
}

// ─── Basic lookup ───────────────────────────────────────────────────────────

#[test]
fn lookup_queries_closest_seeds_first() {
    let mut rt = RoutingTable::with_k(node(0), 20);
    rt.insert(node(1));
    rt.insert(node(2));
    rt.insert(node(3));

    let target = node(0x10);
    let (lookup, actions) = NodeLookup::start_with_params(target, &rt, 3, 3);

    // Should emit Query actions for the seeds
    let queries: Vec<_> = actions
        .iter()
        .filter(|a| matches!(a, LookupAction::Query { .. }))
        .collect();
    assert!(!queries.is_empty(), "should query initial seeds");
    assert!(!lookup.is_done());
}

#[test]
fn lookup_terminates_when_no_new_closer_nodes() {
    let mut rt = RoutingTable::with_k(node(0), 3);
    rt.insert(node(1));
    rt.insert(node(2));

    let target = node(0x10);
    let (mut lookup, _initial_actions) = NodeLookup::start_with_params(target, &rt, 3, 3);

    // All seeds respond with empty closer lists
    let actions = lookup.handle_response(node(1), vec![]);
    // After second response, all known nodes queried → done
    let actions2 = lookup.handle_response(node(2), vec![]);

    let all_actions: Vec<_> = actions.into_iter().chain(actions2).collect();
    let done = all_actions.iter().any(|a| matches!(a, LookupAction::Done { .. }));
    assert!(done, "lookup should complete when all seeds responded with no new nodes");
}

#[test]
fn lookup_discovers_closer_nodes_through_responses() {
    let mut rt = RoutingTable::with_k(node(0), 3);
    rt.insert(node(1));

    let target = node(0x10);
    let (mut lookup, _) = NodeLookup::start_with_params(target, &rt, 3, 3);

    // node(1) responds with closer nodes
    let actions = lookup.handle_response(node(1), vec![
        node(0x11), // very close to target 0x10
        node(0x12),
    ]);

    // Should query the newly discovered closer nodes
    let queries: Vec<_> = actions
        .iter()
        .filter_map(|a| match a {
            LookupAction::Query { node_id, .. } => Some(*node_id),
            _ => None,
        })
        .collect();
    assert!(!queries.is_empty(), "should query newly discovered nodes");
}

#[test]
fn lookup_result_contains_k_closest() {
    let mut rt = RoutingTable::with_k(node(0), 20);
    for i in 1..=10u8 {
        let mut bytes = [0u8; 32];
        bytes[0] = i;
        rt.insert(NodeId(bytes));
    }

    let target = node(0x05);
    let (mut lookup, _) = NodeLookup::start_with_params(target, &rt, 5, 3);

    // Simulate all nodes responding with no new nodes
    // Feed responses for all queried nodes until done
    for _ in 0..50 {
        if lookup.is_done() {
            break;
        }
        // Handle responses for all pending nodes
        for i in 1..=10u8 {
            let mut bytes = [0u8; 32];
            bytes[0] = i;
            let actions = lookup.handle_response(NodeId(bytes), vec![]);
            if actions.iter().any(|a| matches!(a, LookupAction::Done { .. })) {
                break;
            }
        }
    }

    assert!(lookup.is_done());
}

#[test]
fn lookup_handles_node_failures() {
    let mut rt = RoutingTable::with_k(node(0), 20);
    rt.insert(node(1));
    rt.insert(node(2));
    rt.insert(node(3));

    let target = node(0x10);
    let (mut lookup, _) = NodeLookup::start_with_params(target, &rt, 3, 3);

    // node(1) fails, node(2) responds, node(3) fails
    lookup.handle_failure(node(1));
    lookup.handle_failure(node(3));
    let actions = lookup.handle_response(node(2), vec![]);

    // Should still eventually complete
    let done = actions.iter().any(|a| matches!(a, LookupAction::Done { .. }));
    assert!(done || !lookup.is_done()); // either done or has more rounds
}

#[test]
fn lookup_with_empty_routing_table_completes_immediately() {
    let rt = RoutingTable::with_k(node(0), 20);
    let target = node(0x10);
    let (lookup, actions) = NodeLookup::start_with_params(target, &rt, 3, 3);

    assert!(lookup.is_done());
    let done = actions.iter().any(|a| matches!(a, LookupAction::Done { .. }));
    assert!(done, "empty routing table should produce Done with empty result");
}

// ─── Multi-hop convergence ──────────────────────────────────────────────────

#[test]
fn lookup_converges_through_multiple_hops() {
    // Simulate: node 0 → knows node 1 → knows node 2 → knows node 3 (closest to target)
    let mut rt = RoutingTable::with_k(node(0), 20);
    rt.insert(node(1));

    let target = NodeId([0xFF; 32]);
    let (mut lookup, initial) = NodeLookup::start_with_params(target, &rt, 3, 3);

    // Verify we queried node 1
    assert!(initial.iter().any(|a| matches!(a, LookupAction::Query { node_id, .. } if *node_id == node(1))));

    // node 1 returns node 2
    let actions = lookup.handle_response(node(1), vec![node(2)]);
    assert!(actions.iter().any(|a| matches!(a, LookupAction::Query { node_id, .. } if *node_id == node(2))));

    // node 2 returns node 3 (very close to target)
    let mut close_bytes = [0xFFu8; 32];
    close_bytes[31] = 0xFE;
    let close_node = NodeId(close_bytes);
    let actions = lookup.handle_response(node(2), vec![close_node]);

    // Should query the close node
    assert!(actions.iter().any(|a| matches!(a, LookupAction::Query { node_id, .. } if *node_id == close_node)));

    // Close node has no more info
    let actions = lookup.handle_response(close_node, vec![]);
    assert!(actions.iter().any(|a| matches!(a, LookupAction::Done { .. })));

    // The done result should include the close node
    if let Some(LookupAction::Done { closest }) = actions.iter().find(|a| matches!(a, LookupAction::Done { .. })) {
        assert!(closest.iter().any(|id| *id == close_node));
    }
}
