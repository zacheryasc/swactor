//! Behavior guarantees for the prompt wire types.

use myelin::node::prompt_wire::PromptEvent;

fn is_terminal(event: &PromptEvent) -> bool {
    matches!(event, PromptEvent::Done { .. } | PromptEvent::Fault { .. })
}

#[test]
fn event_terminal_state_is_explicit() {
    assert!(!is_terminal(&PromptEvent::TextDelta {
        request_id: 1,
        text: "a".to_owned(),
    }));
    assert!(is_terminal(&PromptEvent::Done {
        request_id: 1,
        final_text: "a".to_owned(),
        tokens_generated: 1,
        elapsed_ms: 2,
    }));
    assert!(is_terminal(&PromptEvent::Fault {
        request_id: 1,
        error: "boom".to_owned(),
    }));
}
