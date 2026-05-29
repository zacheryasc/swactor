// Shared model + helpers for the live fleet dashboard.
//
// `buildModel(records)` folds a flat record stream into a per-stage structure
// (time-series + events) that the board renders from. Lifted from the vastai
// mockups (`crates/dashboard/examples/vastai_mockups/vastai_model.js`) so the
// live board and the offline mockups stay shape-compatible: a record is
// `{ at_ms, node: { stage_index, label, contract_id }, body: { t, ... } }`.
//
// The live board feeds this the *bridged* form of each collector `LiveRecord`
// (see fleet_live.html) and re-folds on every batch; pure function of its input,
// no DOM, no globals mutated.

const STAGE_COLORS = ["#4a90e2", "#f06292", "#ffb74d", "#81c784", "#ba68c8", "#4dd0e1", "#aed581", "#ff8a65"];

function stageColor(idx) {
  return STAGE_COLORS[idx % STAGE_COLORS.length];
}

// Map a vast.ai status string to a coarse health bucket + display color.
function statusInfo(status) {
  switch (status) {
    case "running":  return { kind: "running",  color: "#4ade80", label: "running" };
    case "loading":  return { kind: "loading",  color: "#fbbf24", label: "loading" };
    case "created":  return { kind: "loading",  color: "#fbbf24", label: "created" };
    case "offline":  return { kind: "dead",     color: "#9ca3af", label: "offline" };
    case "exited":   return { kind: "dead",     color: "#f87171", label: "exited" };
    default:         return { kind: "unknown",  color: "#6b7280", label: status || "—" };
  }
}

function fmtDur(ms) {
  if (ms == null) return "—";
  const s = Math.floor(ms / 1000);
  const m = Math.floor(s / 60);
  const r = s % 60;
  return m > 0 ? `${m}m${String(r).padStart(2, "0")}s` : `${r}s`;
}

function fmtClock(ms) {
  const s = Math.floor(ms / 1000);
  const m = Math.floor(s / 60);
  return `${String(m).padStart(2, "0")}:${String(s % 60).padStart(2, "0")}`;
}

function fmtBytesRate(bps) {
  if (bps == null) return "—";
  const u = ["B/s", "KB/s", "MB/s", "GB/s"];
  let i = 0, v = bps;
  while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
  return `${v.toFixed(v < 10 ? 1 : 0)} ${u[i]}`;
}

function fmtGB(bytes) {
  if (bytes == null) return "—";
  return `${(bytes / 1e9).toFixed(1)} GB`;
}

function fmtUSD(v) {
  if (v == null) return "—";
  return `$${v.toFixed(v < 1 ? 4 : 2)}`;
}

// Fold the record stream into a model the board consumes.
function buildModel(records, meta) {
  meta = meta || { run_id: "run" };
  const stages = new Map(); // stageIndex -> stage object

  const runPrefix = meta.label ? meta.label + "-" : "";
  const trim = (l) => (l && runPrefix && l.startsWith(runPrefix) ? l.slice(runPrefix.length) : l);

  function stage(idx, label) {
    label = trim(label);
    if (!stages.has(idx)) {
      stages.set(idx, {
        idx,
        label: label || `stage-${idx}`,
        color: stageColor(idx),
        gpuName: null,
        geo: null,
        dph: null,
        samples: [],      // in-VM HostSample series
        instances: [],    // external poller observations
        logs: [],         // log lines (flattened)
        lifecycle: [],    // lifecycle events for this stage
        contracts: new Map(),
      });
    }
    const s = stages.get(idx);
    if (label && s.label.startsWith("stage-")) s.label = label;
    return s;
  }

  const lifecycleAll = [];
  let deployStart = null;

  for (const r of records) {
    const b = r.body;
    if (!b) continue;
    const node = r.node || {};
    // stage_index keys every series; records without one (e.g. the external
    // poller's deploy_start) are folded into run-level state instead.
    const sIdx = node.stage_index;
    const t = r.at_ms;

    if (b.t === "lifecycle") {
      if (b.event === "deploy_start") {
        deployStart = { at: t, numStages: b.num_stages };
        lifecycleAll.push({ at: t, stage: null, event: b.event, num_stages: b.num_stages });
        continue;
      }
      if (sIdx == null) continue;
      const s = stage(sIdx, node.label);
      s.lifecycle.push({ at: t, ...b });
      lifecycleAll.push({ at: t, stage: sIdx, ...b });
      if (b.event === "contract_leased") {
        const c = s.contracts.get(b.contract_id) || { id: b.contract_id };
        c.leasedAt = t; c.offer = b.offer_id;
        s.contracts.set(b.contract_id, c);
      } else if (b.event === "teardown") {
        const c = s.contracts.get(b.contract_id) || { id: b.contract_id };
        c.endAt = t; c.reason = b.reason;
        s.contracts.set(b.contract_id, c);
      }
      continue;
    }

    if (b.t === "instance") {
      if (sIdx == null) continue;
      const s = stage(sIdx, node.label);
      s.gpuName = s.gpuName || b.gpu_name;
      s.geo = s.geo || b.geolocation;
      s.dph = b.dph_total ?? s.dph;
      s.instances.push({
        at: t,
        contractId: b.id,
        status: b.actual_status,
        statusMsg: b.status_msg,
        cost: b.accumulated_cost,
        dph: b.dph_total,
        gpuUtil: b.gpu_util,
        gpuTemp: b.gpu_temp,
        cpuUtil: b.cpu_util,
        memUsageMb: b.mem_usage,
        gpuRam: b.gpu_ram,
        numGpus: b.num_gpus,
        diskUsage: b.disk_usage,
      });
      const c = s.contracts.get(b.id) || { id: b.id };
      if (b.actual_status === "exited" || b.actual_status === "offline") {
        c.terminalStatus = b.actual_status;
      }
      s.contracts.set(b.id, c);
      continue;
    }

    if (b.t === "host_sample") {
      if (sIdx == null) continue;
      const s = stage(sIdx, node.label);
      const g = (b.gpus && b.gpus[0]) || {};
      const disk = (b.disk && b.disk[0]) || {};
      const net = (b.net && b.net[0]) || {};
      if (g.name) s.gpuName = s.gpuName || g.name;
      s.samples.push({
        at: t,
        contractId: node.contract_id,
        util: g.util_pct,
        vram: g.mem_used_mb,
        vramTotal: g.mem_total_mb,
        temp: g.temp_c,
        power: g.power_w,
        powerLimit: g.power_limit_w,
        cpu: b.cpu ? b.cpu.util_pct : null,
        load1: b.cpu ? b.cpu.load1 : null,
        numCpus: b.cpu ? b.cpu.num_cpus : null,
        memUsedKb: b.mem ? b.mem.used_kb : null,
        memTotalKb: b.mem ? b.mem.total_kb : null,
        diskR: disk.read_bytes_per_s,
        diskW: disk.write_bytes_per_s,
        fsUsed: disk.fs_used_bytes,
        fsTotal: disk.fs_total_bytes,
        netRx: net.rx_bytes_per_s,
        netTx: net.tx_bytes_per_s,
      });
      continue;
    }

    if (b.t === "logs") {
      if (sIdx == null) continue;
      const s = stage(sIdx, node.label);
      for (const ln of b.lines || []) {
        s.logs.push({ at: t, stream: b.stream, line: ln.line, text: ln.text, contractId: node.contract_id });
      }
      continue;
    }
  }

  const stageList = [...stages.values()].sort((a, b) => a.idx - b.idx);
  for (const s of stageList) {
    s.samples.sort((a, b) => a.at - b.at);
    s.instances.sort((a, b) => a.at - b.at);
    s.logs.sort((a, b) => a.at - b.at);
    s.lifecycle.sort((a, b) => a.at - b.at);
    s.contractList = [...s.contracts.values()].sort((a, b) => (a.leasedAt ?? 0) - (b.leasedAt ?? 0));
    let cost = 0;
    const byContract = new Map();
    for (const o of s.instances) byContract.set(o.contractId, o.cost ?? 0);
    for (const v of byContract.values()) cost += v;
    s.totalCost = cost;
  }

  const allLogs = [];
  for (const s of stageList) for (const l of s.logs) allLogs.push({ ...l, stage: s.idx });
  allLogs.sort((a, b) => a.at - b.at || (a.line ?? 0) - (b.line ?? 0));

  return { meta, stages: stageList, lifecycleAll: lifecycleAll.sort((a, b) => a.at - b.at), allLogs, deployStart };
}

// State of one stage at time T (latest observation/sample at or before T).
function stageStateAt(stage, t) {
  const lastAtOrBefore = (arr) => {
    let r = null;
    for (const x of arr) { if (x.at <= t) r = x; else break; }
    return r;
  };
  const inst = lastAtOrBefore(stage.instances);
  const samp = lastAtOrBefore(stage.samples);
  let activeContract = null;
  for (const c of stage.contractList) {
    if ((c.leasedAt ?? 0) <= t && (c.endAt == null || c.endAt > t)) activeContract = c;
  }
  return { inst, samp, activeContract };
}

// Last N log lines at or before time T (across one stage or a provided list).
function logsUpTo(logs, t, n) {
  const out = [];
  for (const l of logs) { if (l.at <= t) out.push(l); }
  return n ? out.slice(-n) : out;
}

// Live-mode status for a stage: with no external poller there are no `instance`
// records, so derive health from how recently we last saw a host_sample. A
// fresh sample (within `staleMs`) means the node is alive and reporting; a
// stale one means it stopped (crashed / torn down / network gone). If an
// `instance` record *is* present (plugin emitting external view), prefer it.
function liveStatus(stage, now, staleMs) {
  const inst = stage.instances.length ? stage.instances[stage.instances.length - 1] : null;
  if (inst && inst.status) return statusInfo(inst.status);
  const last = stage.samples.length ? stage.samples[stage.samples.length - 1] : null;
  if (!last) return statusInfo("created");
  return statusInfo(now - last.at <= staleMs ? "running" : "offline");
}

window.FleetModel = {
  buildModel, stageStateAt, logsUpTo, liveStatus, statusInfo, stageColor,
  fmtDur, fmtClock, fmtBytesRate, fmtGB, fmtUSD,
};
