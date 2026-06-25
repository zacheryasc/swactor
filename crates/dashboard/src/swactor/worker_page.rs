pub const WORKER_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>swactor workers</title>
  <style>
    :root { color-scheme: dark; font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #0f172a; color: #e2e8f0; }
    body { margin: 0; padding: 20px; }
    header { display: flex; align-items: baseline; gap: 16px; margin-bottom: 18px; }
    h1 { margin: 0; font-size: 28px; }
    select, button { background: #1e293b; color: #e2e8f0; border: 1px solid #334155; border-radius: 8px; padding: 8px 10px; }
    .muted { color: #94a3b8; }
    .grid { display: grid; grid-template-columns: repeat(4, minmax(150px, 1fr)); gap: 12px; margin-bottom: 16px; }
    .card { background: #1e293b; border: 1px solid #334155; border-radius: 14px; padding: 14px; }
    .label { color: #94a3b8; font-size: 12px; text-transform: uppercase; letter-spacing: .08em; }
    .value { font-size: 24px; margin-top: 6px; font-variant-numeric: tabular-nums; }
    table { width: 100%; border-collapse: collapse; background: #111827; border: 1px solid #334155; border-radius: 12px; overflow: hidden; margin-bottom: 16px; }
    th, td { padding: 9px 10px; border-bottom: 1px solid #1f2937; text-align: left; font-variant-numeric: tabular-nums; }
    th { color: #93c5fd; background: #1e293b; cursor: pointer; }
    tr[data-selected="true"] { background: #1d4ed833; }
    .bad { color: #f87171; font-weight: 700; }
    .warn { color: #fbbf24; }
    .ok { color: #34d399; }
    .section { margin-top: 20px; }
    .spark-card { display: grid; gap: 8px; margin-bottom: 16px; }
    .spark-head { display: flex; justify-content: space-between; gap: 12px; align-items: baseline; }
    .legend { display: flex; gap: 14px; color: #94a3b8; font-size: 12px; }
    .legend i { display: inline-block; width: 20px; height: 3px; margin-right: 6px; vertical-align: middle; border-radius: 99px; }
    .spark { width: 100%; height: 72px; background: #111827; border: 1px solid #334155; border-radius: 12px; }
    .empty { padding: 28px; border: 1px dashed #475569; border-radius: 12px; color: #94a3b8; }
    @media (max-width: 900px) { .grid { grid-template-columns: repeat(2, minmax(150px, 1fr)); } body { padding: 12px; } }
  </style>
</head>
<body>
  <header>
    <h1>Swactor workers</h1>
    <span class="muted" id="status">loading…</span>
    <select id="runtime"></select>
  </header>

  <section class="grid" id="summary"></section>
  <section class="card spark-card">
    <div class="spark-head">
      <div><div class="label">activity</div><div class="muted">message rate and queued depth</div></div>
      <div class="legend"><span><i style="background:#60a5fa"></i>msg/s</span><span><i style="background:#fbbf24"></i>queued</span></div>
    </div>
    <svg class="spark" id="spark" viewBox="0 0 800 120" preserveAspectRatio="none"></svg>
  </section>

  <section class="section">
    <h2>Workers</h2>
    <table>
      <thead><tr id="workers-head"></tr></thead>
      <tbody id="workers"></tbody>
    </table>
  </section>

  <section class="section">
    <h2>Actors <span class="muted" id="actor-filter"></span></h2>
    <table>
      <thead><tr><th>actor</th><th>name</th><th>worker</th><th>queued</th><th>msg/s</th><th>processed</th><th>last message</th><th>state</th></tr></thead>
      <tbody id="actors"></tbody>
    </table>
  </section>

<script>
const runtimeSelect = document.getElementById('runtime');
const statusEl = document.getElementById('status');
const summaryEl = document.getElementById('summary');
const workersEl = document.getElementById('workers');
const workersHeadEl = document.getElementById('workers-head');
const actorsEl = document.getElementById('actors');
const actorFilterEl = document.getElementById('actor-filter');
const sparkEl = document.getElementById('spark');
let selectedRuntime = '';
let selectedWorker = null;

function fmt(n, digits = 0) {
  if (n === null || n === undefined) return '—';
  if (typeof n === 'number') return n.toLocaleString(undefined, { maximumFractionDigits: digits });
  return String(n);
}
function workerLabel(id) { return id === null || id === undefined ? 'aggregate' : id; }
function setSummary(rt) {
  const totals = rt.totals || {};
  const cards = [
    ['workers', totals.workers, true],
    ['actors', totals.actors, true],
    ['queued', totals.mailbox_depth, true],
    ['msg/s', totals.msg_per_sec, true],
    ['local/s', totals.local_per_sec, true],
    ['cross/s', totals.cross_per_sec, true],
    ['tick p50 μs', totals.tick_p50_us, true],
    ['inbox/s', totals.inbox_per_sec, totals.inbox_per_sec > 0],
    ['drops', totals.messages_dropped, totals.messages_dropped > 0],
    ['panics', totals.panics, totals.panics > 0],
  ].filter(([, , show]) => show);
  summaryEl.innerHTML = cards.map(([k, v]) => `<div class="card"><div class="label">${k}</div><div class="value">${fmt(v, 1)}</div></div>`).join('');
}
function drawSpark(history) {
  const points = (history || []).slice(-80);
  if (points.length < 2) { sparkEl.innerHTML = `<text x="400" y="64" text-anchor="middle" fill="currentColor">waiting for activity</text>`; return; }
  const maxMsg = Math.max(1, ...points.map(p => p.msg_per_sec || 0));
  const maxDepth = Math.max(1, ...points.map(p => p.mailbox_depth || 0));
  const line = (field, max, color) => {
    const coords = points.map((p, i) => {
      const x = points.length === 1 ? 0 : i * 800 / (points.length - 1);
      const y = 110 - ((p[field] || 0) / max) * 100;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    }).join(' ');
    return `<polyline fill="none" stroke="${color}" stroke-width="3" points="${coords}" />`;
  };
  sparkEl.innerHTML = line('msg_per_sec', maxMsg, '#60a5fa') + line('mailbox_depth', maxDepth, '#fbbf24');
}
function renderWorkers(rt) {
  const rows = [...(rt.workers || [])].sort((a, b) => (b.mailbox_depth || 0) - (a.mailbox_depth || 0));
  const showInbox = rows.some(w => (w.inbox_per_sec || 0) > 0);
  const showDrops = rows.some(w => (w.messages_dropped || 0) > 0);
  const showPanics = rows.some(w => (w.panics || 0) > 0);
  const columns = [
    ['worker', w => workerLabel(w.id)],
    ['actors', w => fmt(w.actor_count)],
    ['queued', w => `<span class="${w.mailbox_depth ? 'warn' : ''}">${fmt(w.mailbox_depth)}</span>`],
    ['msg/s', w => fmt(w.msg_per_sec, 1)],
    ['local/s', w => fmt(w.local_per_sec, 1)],
    ['cross/s', w => fmt(w.cross_per_sec, 1)],
    ['tick p50 μs', w => fmt(w.tick_p50_us)],
    ['inbox/s', w => fmt(w.inbox_per_sec, 1), showInbox],
    ['drops', w => `<span class="bad">${fmt(w.messages_dropped)}</span>`, showDrops],
    ['panics', w => `<span class="bad">${fmt(w.panics)}</span>`, showPanics],
  ].filter(([, , show = true]) => show);

  workersHeadEl.innerHTML = columns.map(([label]) => `<th>${label}</th>`).join('');
  workersEl.innerHTML = rows.map(w => {
    const sel = String(workerLabel(w.id)) === String(selectedWorker);
    return `<tr data-selected="${sel}" data-worker="${workerLabel(w.id)}">${columns.map(([, cell]) => `<td>${cell(w)}</td>`).join('')}</tr>`;
  }).join('') || `<tr><td colspan="${columns.length}" class="empty">No worker frames received yet.</td></tr>`;
  workersEl.querySelectorAll('tr[data-worker]').forEach(row => row.onclick = () => {
    selectedWorker = row.dataset.worker;
    render(currentSnapshot);
  });
}
function renderActors(rt) {
  const rows = [...(rt.actors || [])]
    .filter(a => selectedWorker === null || String(workerLabel(a.worker_id)) === String(selectedWorker))
    .sort((a, b) => (b.mailbox_depth || 0) - (a.mailbox_depth || 0));
  actorFilterEl.textContent = selectedWorker === null ? '' : `(worker ${selectedWorker})`;
  actorsEl.innerHTML = rows.map(a => `<tr>
    <td>${a.address}</td><td>${a.name || ''}</td><td>${workerLabel(a.worker_id)}</td>
    <td class="${a.mailbox_depth ? 'warn' : ''}">${fmt(a.mailbox_depth)}</td><td>${fmt(a.msg_per_sec, 1)}</td><td>${fmt(a.messages_processed)}</td>
    <td>${a.last_msg_type || ''}</td><td class="${a.poisoned ? 'bad' : 'ok'}">${a.poisoned ? 'poisoned' : 'ok'}</td>
  </tr>`).join('') || '<tr><td colspan="8" class="empty">No actor detail frames received yet.</td></tr>';
}
let currentSnapshot = null;
function render(snapshot) {
  currentSnapshot = snapshot;
  const runtimes = snapshot.runtimes || [];
  const keys = runtimes.map(r => r.stream.key);
  if (!selectedRuntime && keys.length) selectedRuntime = keys[0];
  runtimeSelect.innerHTML = keys.map(k => `<option value="${k}" ${k === selectedRuntime ? 'selected' : ''}>${k}</option>`).join('');
  const rt = runtimes.find(r => r.stream.key === selectedRuntime) || runtimes[0];
  if (!rt) {
    statusEl.textContent = 'waiting for runtime frames';
    summaryEl.innerHTML = '<div class="empty">No swactor runtime frames received yet.</div>';
    workersEl.innerHTML = '';
    actorsEl.innerHTML = '';
    sparkEl.innerHTML = '';
    return;
  }
  statusEl.textContent = `${rt.live ? 'live' : 'stale'} · last seen ${fmt(rt.last_seen_ms_ago)}ms ago`;
  setSummary(rt);
  drawSpark(rt.history);
  renderWorkers(rt);
  renderActors(rt);
}
runtimeSelect.onchange = () => { selectedRuntime = runtimeSelect.value; selectedWorker = null; if (currentSnapshot) render(currentSnapshot); };
async function refresh() {
  const res = await fetch('/api/view/swactor/workers', { cache: 'no-store' });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  render(await res.json());
}
setInterval(() => refresh().catch(err => statusEl.textContent = err.message), 750);
refresh().catch(err => statusEl.textContent = err.message);
</script>
</body>
</html>"#;
