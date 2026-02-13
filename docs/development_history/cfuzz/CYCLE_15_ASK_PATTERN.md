# Cycle 15: Ask Pattern for Typed Request-Response — Development History

> Commit: `902471b` · 3 files · 166 insertions, 1 deletion

---

## Motivation

Request-response is one of the most common actor communication patterns: "send a question, wait for the answer." Before this change, implementing request-response in swactor required manual inbox creation, message construction with a reply-to address, sending, ticking, and polling — a verbose 5-step process. Every mature actor framework provides a convenience wrapper for this pattern.

## Competitor Analysis

| Framework | Pattern | Mechanism | Synchronous? |
|-----------|---------|-----------|-------------|
| Erlang | `gen_server:call` | `From` + `gen_server:reply` | Blocks caller (with timeout) |
| Akka | `ask` | Temporary actor + `Future` | Returns Future |
| Ractor | `call` | `RpcReplyPort` (oneshot channel) | Returns JoinHandle |
| Kameo | `ask` | Async + `Reply` trait | Returns Future |
| xactor | `Handler::handle` | Return value auto-routed | Implicit |
| **Swactor** | **`rt.ask()`** | **Inbox + closure** | **`recv_ticking` (tick-driven)** |

### Key Findings
- Swactor's synchronous tick model requires explicit `reply_to` — there's no async runtime to suspend the caller
- **Implicit auto-reply rejected** — would add magic to the message pipeline and complicate the actor interface
- **Decision**: convenience wrapper over existing inbox pattern (not a new mechanism)

## Implementation

### Ask\<R\> Struct
- Wraps an `Inbox<R>` with convenience methods
- `try_recv()` — poll without ticking (works in both single and multi-threaded modes)
- `recv_ticking(rt, max_ticks)` — tick the runtime until a response arrives or timeout (single-threaded only)
- `reply_addr()` — access the inbox address for manual use

### Runtime::ask()
- `rt.ask(addr, |reply_to| Msg { reply_to })` — one-line request-response
- Creates inbox, builds message via closure (user provides the reply_to field), sends, returns `Ask<R>`
- Purely sugar over the existing `new_inbox → send_to → tick → try_recv` pattern

### No Internal Changes
- Zero changes to `ContextInner` or `ActorInterface`
- No implicit auto-reply magic
- Actors reply by explicitly sending to the `reply_to` address (same as before)

**Key files modified:** `src/runtime.rs`, `tests/runtime_api.rs`

## Design Decisions

- **Closure-based message construction** — `rt.ask(addr, |reply_to| Msg { reply_to })` lets the user embed the reply address in any message shape. No trait requirements on the message type (beyond `Message`).
- **`recv_ticking` for single-threaded** — in single-threaded mode, the runtime must be ticked for the target actor to process the request and reply. `recv_ticking` does this automatically. In multi-threaded mode, use `try_recv` with your own tick loop.
- **No implicit reply** — frameworks like xactor auto-route the handler's return value as a reply. This is magical and doesn't fit swactor's explicit model. The ask pattern wraps existing mechanics without adding new ones.
- **max_ticks timeout** — instead of wall-clock timeout, uses tick count for deterministic behavior (consistent with Cycle 10 timers).

## Tests Added

5 new tests (122 → 127 total):

- `ask_recv_ticking_returns_response` — basic PingPong ask roundtrip
- `ask_multiple_times_tracks_state` — 3 sequential asks to CounterActor, state increments
- `ask_timeout_when_no_response` — ask dead actor → timeout error
- `ask_try_recv_returns_none_before_tick` — poll before tick → None, after tick → Some
- `ask_reply_addr_is_accessible` — reply address is valid for manual use

## Result

- 127 tests pass (120 behavioral + 7 proptest)
- All workspace crates compile, zero warnings
