import { SwactorRuntime } from "./pkg/swactor_wasm.js";

let passed = 0;
let failed = 0;

function assert(cond, msg) {
  if (!cond) {
    console.error(`  FAIL: ${msg}`);
    failed++;
  } else {
    passed++;
  }
}

function assertEq(a, b, msg) {
  const aj = JSON.stringify(a);
  const bj = JSON.stringify(b);
  if (aj !== bj) {
    console.error(`  FAIL: ${msg}  (got ${aj}, expected ${bj})`);
    failed++;
  } else {
    passed++;
  }
}

function drain(rt) {
  const results = [];
  let v;
  while ((v = rt.try_recv()) !== undefined) results.push(v);
  return results;
}

// ---- accumulator ----------------------------------------------------------
{
  console.log("test: accumulator processes messages");
  const rt = new SwactorRuntime();
  const c = rt.spawn_counter();
  rt.send(c, 1);
  rt.send(c, 2);
  rt.send(c, 10);
  rt.tick();
  assertEq(drain(rt), [1, 3, 13], "running totals");
  rt.free();
}

// ---- relay ----------------------------------------------------------------
{
  console.log("test: relay forwards to counter");
  const rt = new SwactorRuntime();
  const c = rt.spawn_counter();
  const r = rt.spawn_relay(c);
  rt.send(r, 5);
  rt.send(r, 7);
  // tick 1: relay receives and forwards (cross-actor, same worker → pending_local)
  // tick 2: counter receives forwarded messages
  rt.tick();
  rt.tick();
  assertEq(drain(rt), [5, 12], "relayed totals");
  rt.free();
}

// ---- multiple counters ----------------------------------------------------
{
  console.log("test: multiple independent counters");
  const rt = new SwactorRuntime();
  const a = rt.spawn_counter();
  const b = rt.spawn_counter();
  rt.send(a, 10);
  rt.send(b, 100);
  rt.tick();
  const results = drain(rt);
  // order depends on HashMap iteration, so just check set equality
  assert(
    results.includes(10) && results.includes(100) && results.length === 2,
    "both counters report"
  );
  rt.free();
}

// ---- actor_count ----------------------------------------------------------
{
  console.log("test: actor_count tracks spawns");
  const rt = new SwactorRuntime();
  rt.spawn_counter();
  rt.spawn_counter();
  rt.spawn_counter();
  rt.tick(); // drain spawn queue
  assertEq(rt.actor_count(), 3, "three actors");
  rt.free();
}

// ---- send to invalid index returns false ----------------------------------
{
  console.log("test: send to bad index returns false");
  const rt = new SwactorRuntime();
  assert(!rt.send(999, 1), "out-of-bounds send");
  rt.free();
}

// ---- results --------------------------------------------------------------
console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
