# runtime-dashboard

Visual dashboard and architectural diagrams for the swactor runtime.

## Generating Diagrams

Render all `.dot` sources into SVGs:

```bash
./render_docs.sh
```

**Prerequisites** (one of):
- [Graphviz](https://graphviz.org/) — `apt install graphviz` / `brew install graphviz`
- [Node.js](https://nodejs.org/) — the script auto-installs `@viz-js/viz` into `tools/`

Generated SVGs are written to `docs/generated/` (gitignored).

## Diagram Index

### DOT sources (`docs/*.dot` → `docs/generated/*.svg`)

| Diagram | Description |
|---------|-------------|
| `architecture.dot` | Structural map of all structs/traits, grouped by module, with ownership/Arc/borrow/trait-impl edges |
| `dataflow.dot` | 5 behavioral flows: cross-worker send, same-worker send, actor spawn, external inbox, 6-phase tick cycle |
| `tick_cycle.dot` | Focused view of the `Worker::tick_once` pipeline and its 6 phases |
| `type_erasure.dot` | How generic message/actor types are erased via `Box<dyn Any>` and `Box<dyn AnyActor>` |

### Hand-authored SVGs (`docs/*.svg`, committed)

| Diagram | Description |
|---------|-------------|
| `actor_lifecycle.svg` | Lifecycle states of an actor from spawn to shutdown |
| `message_lifecycle.svg` | Path of a message from send through inbox to handler |
| `runtime_lifecycle.svg` | Runtime startup, worker creation, and shutdown sequence |
