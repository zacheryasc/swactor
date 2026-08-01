//! Behavior guarantees for the `prompt` module.

use myelin::prompt::rpc::{PromptEvent, SubmitPrompt};

#[test]
fn zero_request_limits_take_loop_defaults() {
    let request = SubmitPrompt {
        request_id: 7,
        prompt_text: "hello".to_owned(),
        max_tokens: 0,
    }
    .with_defaults(32);

    assert_eq!(request.max_tokens, 32);
}

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
