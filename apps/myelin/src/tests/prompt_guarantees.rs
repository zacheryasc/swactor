//! Behavior guarantees for the prompt wire types.

use myelin::node::prompt_wire::PromptEvent;

#[test]
fn event_terminal_state_is_explicit() {
    assert!(
        !PromptEvent::TextDelta {
            request_id: 1,
            text: "a".to_owned(),
        }
        .is_terminal()
    );
    assert!(
        PromptEvent::Done {
            request_id: 1,
            final_text: "a".to_owned(),
            tokens_generated: 1,
            elapsed_ms: 2,
        }
        .is_terminal()
    );
    assert!(
        PromptEvent::Fault {
            request_id: 1,
            error: "boom".to_owned(),
        }
        .is_terminal()
    );
}

#[test]
fn event_request_ids_are_always_available() {
    let events = [
        PromptEvent::TextDelta {
            request_id: 4,
            text: String::new(),
        },
        PromptEvent::Done {
            request_id: 5,
            final_text: String::new(),
            tokens_generated: 0,
            elapsed_ms: 0,
        },
        PromptEvent::Fault {
            request_id: 6,
            error: String::new(),
        },
    ];
    let ids: Vec<u64> = events.iter().map(PromptEvent::request_id).collect();
    assert_eq!(ids, vec![4, 5, 6]);
}
