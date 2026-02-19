import {
  WasmRuntime,
  WasmAddr,
  spawn_counter,
  spawn_relay,
  spawn_sentinel,
  spawn_group_member,
} from "./pkg/wasm.js";

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

function drainInbox(inbox) {
  const results = [];
  let v;
  while ((v = inbox.try_recv()) !== undefined) results.push(v);
  return results;
}

// ---- accumulator ----------------------------------------------------------
{
  console.log("test: accumulator processes messages via WasmAddr");
  const rt = new WasmRuntime();
  const inbox = rt.new_inbox_u32();
  const c = spawn_counter(rt, inbox.addr());
  rt.send_u32(c, 1);
  rt.send_u32(c, 2);
  rt.send_u32(c, 10);
  rt.tick();
  assertEq(drainInbox(inbox), [1, 3, 13], "running totals");
  inbox.free();
  rt.free();
}

// ---- relay ----------------------------------------------------------------
{
  console.log("test: relay forwards to counter");
  const rt = new WasmRuntime();
  const inbox = rt.new_inbox_u32();
  const c = spawn_counter(rt, inbox.addr());
  const r = spawn_relay(rt, c);
  rt.send_u32(r, 5);
  rt.send_u32(r, 7);
  // tick 1: relay receives and forwards (same worker → pending_local)
  // tick 2: counter receives forwarded messages
  rt.tick();
  rt.tick();
  assertEq(drainInbox(inbox), [5, 12], "relayed totals");
  inbox.free();
  rt.free();
}

// ---- multiple counters ----------------------------------------------------
{
  console.log("test: multiple independent counters");
  const rt = new WasmRuntime();
  const inbox = rt.new_inbox_u32();
  const a = spawn_counter(rt, inbox.addr());
  const b = spawn_counter(rt, inbox.addr());
  rt.send_u32(a, 10);
  rt.send_u32(b, 100);
  rt.tick();
  const results = drainInbox(inbox);
  assert(
    results.includes(10) && results.includes(100) && results.length === 2,
    "both counters report"
  );
  inbox.free();
  rt.free();
}

// ---- actor_count ----------------------------------------------------------
{
  console.log("test: actor_count tracks spawns");
  const rt = new WasmRuntime();
  const inbox = rt.new_inbox_u32();
  spawn_counter(rt, inbox.addr());
  spawn_counter(rt, inbox.addr());
  spawn_counter(rt, inbox.addr());
  rt.tick(); // drain spawn queue
  assertEq(rt.actor_count(), 3, "three actors");
  inbox.free();
  rt.free();
}

// ---- WasmAddr toString ----------------------------------------------------
{
  console.log("test: WasmAddr has string representation");
  const rt = new WasmRuntime();
  const inbox = rt.new_inbox_u32();
  const addr = spawn_counter(rt, inbox.addr());
  const s = addr.toString();
  // no_random generates deterministic addresses — just check it's a non-empty hex string
  assert(typeof s === "string" && s.length > 0, "addr toString is non-empty string");
  inbox.free();
  rt.free();
}

// ---- stop_actor -----------------------------------------------------------
{
  console.log("test: stop_actor removes actor");
  const rt = new WasmRuntime();
  const inbox = rt.new_inbox_u32();
  const c = spawn_counter(rt, inbox.addr());
  rt.tick(); // drain spawn
  assertEq(rt.actor_count(), 1, "one actor before stop");
  rt.stop_actor(c);
  rt.tick(); // process stop + cleanup
  assertEq(rt.actor_count(), 0, "zero actors after stop");
  inbox.free();
  rt.free();
}

// ---- bytes inbox ----------------------------------------------------------
{
  console.log("test: byte inbox receives Uint8Array");
  const rt = new WasmRuntime();
  const inbox = rt.new_inbox_bytes();
  // Send bytes directly (no actor — just to the inbox address)
  rt.send_bytes(inbox.addr(), new Uint8Array([1, 2, 3]));
  rt.tick();
  const result = inbox.try_recv();
  assert(result instanceof Uint8Array, "result is Uint8Array");
  assertEq(Array.from(result), [1, 2, 3], "bytes match");
  inbox.free();
  rt.free();
}

// ---- uptime ---------------------------------------------------------------
{
  console.log("test: uptime_ms returns a number");
  const rt = new WasmRuntime();
  const uptime = rt.uptime_ms();
  assert(typeof uptime === "number" && uptime >= 0, "uptime is non-negative number");
  rt.free();
}

// ---- naming: register and resolve -----------------------------------------
{
  console.log("test: naming — register_name and where_is");
  const rt = new WasmRuntime();
  const inbox = rt.new_inbox_u32();
  const c = spawn_counter(rt, inbox.addr());
  rt.tick(); // drain spawn

  const ok = rt.register_name("my_counter", c);
  assert(ok, "register_name succeeds");

  const found = rt.where_is("my_counter");
  assert(found !== undefined, "where_is finds registered actor");
  assertEq(found.toString(), c.toString(), "where_is returns correct address");

  const notFound = rt.where_is("nonexistent");
  assert(notFound === undefined, "where_is returns undefined for unknown name");

  found.free();
  inbox.free();
  rt.free();
}

// ---- naming: unregister ---------------------------------------------------
{
  console.log("test: naming — unregister_name");
  const rt = new WasmRuntime();
  const inbox = rt.new_inbox_u32();
  const c = spawn_counter(rt, inbox.addr());
  rt.tick();

  rt.register_name("temp", c);
  const prev = rt.unregister_name("temp");
  assert(prev !== undefined, "unregister returns previous address");
  assertEq(prev.toString(), c.toString(), "unregister returns correct address");

  const gone = rt.where_is("temp");
  assert(gone === undefined, "name no longer resolves after unregister");

  prev.free();
  inbox.free();
  rt.free();
}

// ---- naming: registered_names ---------------------------------------------
{
  console.log("test: naming — registered_names");
  const rt = new WasmRuntime();
  const inbox = rt.new_inbox_u32();
  const a = spawn_counter(rt, inbox.addr());
  const b = spawn_counter(rt, inbox.addr());
  rt.tick();

  rt.register_name("alpha", a);
  rt.register_name("beta", b);
  const names = rt.registered_names().split(",").sort();
  assertEq(names, ["alpha", "beta"], "registered_names lists all names");

  inbox.free();
  rt.free();
}

// ---- naming: duplicate name rejected --------------------------------------
{
  console.log("test: naming — duplicate name rejected");
  const rt = new WasmRuntime();
  const inbox = rt.new_inbox_u32();
  const a = spawn_counter(rt, inbox.addr());
  const b = spawn_counter(rt, inbox.addr());
  rt.tick();

  const ok1 = rt.register_name("unique", a);
  const ok2 = rt.register_name("unique", b);
  assert(ok1, "first registration succeeds");
  assert(!ok2, "duplicate registration fails");

  inbox.free();
  rt.free();
}

// ---- groups: join and broadcast -------------------------------------------
{
  console.log("test: groups — join_group and publish_to_group_u32");
  const rt = new WasmRuntime();
  const inbox = rt.new_inbox_u32();
  const a = spawn_group_member(rt, "workers", inbox.addr());
  const b = spawn_group_member(rt, "workers", inbox.addr());
  rt.tick(); // spawn + on_start (join group)

  assertEq(rt.group_member_count("workers"), 2, "two members in group");

  rt.publish_to_group_u32("workers", 42);
  rt.tick(); // group members receive
  rt.tick(); // group members forward to inbox

  const results = drainInbox(inbox);
  assertEq(results.length, 2, "both members received broadcast");
  assert(results.every((v) => v === 42), "correct value broadcast");

  inbox.free();
  rt.free();
}

// ---- groups: leave --------------------------------------------------------
{
  console.log("test: groups — leave_group");
  const rt = new WasmRuntime();
  const inbox = rt.new_inbox_u32();
  const a = spawn_group_member(rt, "pool", inbox.addr());
  const b = spawn_group_member(rt, "pool", inbox.addr());
  rt.tick(); // spawn + on_start

  assertEq(rt.group_member_count("pool"), 2, "two members before leave");
  rt.leave_group(a, "pool");
  assertEq(rt.group_member_count("pool"), 1, "one member after leave");

  inbox.free();
  rt.free();
}

// ---- groups: group_names --------------------------------------------------
{
  console.log("test: groups — group_names");
  const rt = new WasmRuntime();
  const inbox = rt.new_inbox_u32();
  spawn_group_member(rt, "alpha", inbox.addr());
  spawn_group_member(rt, "beta", inbox.addr());
  rt.tick(); // spawn + join

  const names = rt.group_names().split(",").sort();
  assertEq(names, ["alpha", "beta"], "group_names lists all groups");

  inbox.free();
  rt.free();
}

// ---- watching: sentinel detects death -------------------------------------
{
  console.log("test: watching — sentinel reports actor death");
  const rt = new WasmRuntime();
  const inbox = rt.new_inbox_u32();
  const deathInbox = rt.new_inbox_string();

  const target = spawn_counter(rt, inbox.addr());
  const sentinel = spawn_sentinel(rt, target, deathInbox);
  rt.tick(); // spawn + on_start (watch)

  rt.stop_actor(target);
  // tick to process stop, cleanup, and deliver death notification
  for (let i = 0; i < 5; i++) rt.tick();

  const notification = deathInbox.try_recv();
  assert(notification !== undefined, "sentinel received death notification");
  assert(
    typeof notification === "string" && notification.length > 0,
    "notification is a non-empty string"
  );

  inbox.free();
  deathInbox.free();
  rt.free();
}

// ---- stats: total_messages ------------------------------------------------
{
  console.log("test: stats — total_messages");
  const rt = new WasmRuntime();
  const inbox = rt.new_inbox_u32();
  const c = spawn_counter(rt, inbox.addr());
  rt.send_u32(c, 1);
  rt.send_u32(c, 2);
  rt.send_u32(c, 3);
  rt.tick();
  assert(rt.total_messages() >= 3, "total_messages counts processed messages");
  inbox.free();
  rt.free();
}

// ---- stats: total_panics starts at zero -----------------------------------
{
  console.log("test: stats — total_panics starts at zero");
  const rt = new WasmRuntime();
  assertEq(rt.total_panics(), 0, "no panics initially");
  rt.free();
}

// ---- results --------------------------------------------------------------
console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
