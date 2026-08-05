// swactor ping-pong -- host driver.
//
// Spawns a Ping and a Pong actor on the wasm actor runtime, then drives it
// tick-by-tick, printing each volley as it happens. Pass an optional volley cap:
//
//     node run.mjs [volleys]      (default 10)

import { App } from "./pkg/pingpong.js";

const max = Math.max(2, Number(process.argv[2] ?? 10));

const app = new App();
const log = app.new_log();
const pong = app.spawn_pong(log.addr(), max);
app.spawn_ping(pong, log.addr(), max);

console.log(`\n== swactor ping-pong ==  target ${max} volleys\n`);

for (let t = 0; t < 1000; t++) {
  app.tick();
  let line;
  while ((line = log.try_recv())) console.log("  " + line);
  if (app.actor_count() === 0) break;
}

console.log(
  `\nactors remaining: ${app.actor_count()}, messages processed: ${app.total_messages()}\n`,
);
