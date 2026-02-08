pub const DASHBOARD_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Gossip Simulation Dashboard</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; background: #0f1117; color: #e0e0e0; }

  .header {
    display: flex; align-items: center; justify-content: space-between;
    padding: 12px 20px; background: #161822; border-bottom: 1px solid #2a2d3a;
  }
  .header h1 { font-size: 18px; font-weight: 600; color: #c0c6d4; }
  .status-badge {
    display: flex; align-items: center; gap: 6px; font-size: 13px; color: #9ca3af;
  }
  .status-dot { width: 8px; height: 8px; border-radius: 50%; background: #3b82f6; }
  .status-dot.done { background: #22c55e; }
  .status-dot.replay { background: #f59e0b; }

  .main { display: grid; grid-template-columns: 1fr 1fr; grid-template-rows: auto 1fr; height: calc(100vh - 48px); }

  .graph-panel {
    grid-row: 1 / 3; border-right: 1px solid #2a2d3a; position: relative;
  }
  canvas { width: 100%; height: 100%; display: block; }

  .side-panel { display: flex; flex-direction: column; overflow: hidden; min-height: 0; }

  .stats-panel {
    flex-shrink: 0;
    padding: 12px 16px; border-bottom: 1px solid #2a2d3a; background: #161822;
  }
  .stats-panel h2 { font-size: 13px; color: #6b7280; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 8px; }
  .stats-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; }
  .stat-item .stat-value { font-size: 22px; font-weight: 700; color: #e5e7eb; }
  .stat-item .stat-label { font-size: 11px; color: #6b7280; text-transform: uppercase; }

  .worker-panel {
    flex-shrink: 0;
    padding: 12px 16px; border-bottom: 1px solid #2a2d3a; height: 180px; overflow-y: auto;
  }
  .worker-panel h2 { font-size: 13px; color: #6b7280; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 8px; }
  .worker-columns { display: flex; gap: 8px; overflow-x: auto; }
  .worker-col {
    flex: 1; min-width: 160px; background: #1a1d2e; border-radius: 6px; padding: 8px;
    font-size: 11px; max-height: 140px; overflow-y: auto;
  }
  .worker-col-header { font-weight: 600; color: #818cf8; margin-bottom: 4px; font-size: 12px; }
  .worker-entry { color: #9ca3af; padding: 1px 0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }

  .event-panel {
    flex: 1; min-height: 0; display: flex; flex-direction: column; overflow: hidden;
  }
  .event-panel h2 {
    font-size: 13px; color: #6b7280; text-transform: uppercase; letter-spacing: 0.05em;
    padding: 12px 16px 8px;
  }
  .event-table-wrap { flex: 1; overflow-y: auto; padding: 0 16px 8px; }
  table { width: 100%; border-collapse: collapse; font-size: 12px; }
  th { text-align: left; color: #6b7280; font-weight: 500; padding: 4px 8px; border-bottom: 1px solid #2a2d3a; position: sticky; top: 0; background: #0f1117; }
  td { padding: 3px 8px; border-bottom: 1px solid #1a1d2e; white-space: nowrap; }
  tr.highlight-push td { background: rgba(59,130,246,0.1); }
  tr.highlight-set td { background: rgba(34,197,94,0.1); }

  .replay-controls {
    display: flex; align-items: center; gap: 8px; padding: 8px 16px;
    background: #161822; border-bottom: 1px solid #2a2d3a;
  }
  .replay-controls button {
    background: #2a2d3a; color: #e0e0e0; border: none; border-radius: 4px;
    padding: 4px 10px; cursor: pointer; font-size: 13px;
  }
  .replay-controls button:hover { background: #3b3f52; }
  .replay-controls input[type=range] { flex: 1; }
  .replay-controls .replay-pos { font-size: 12px; color: #9ca3af; min-width: 60px; text-align: right; }
  .replay-controls .speed-group { display: flex; align-items: center; gap: 4px; margin-left: 8px; }
  .replay-controls .speed-group input {
    width: 64px; background: #1a1d2e; color: #e0e0e0; border: 1px solid #2a2d3a;
    border-radius: 4px; padding: 2px 6px; font-size: 12px; text-align: right;
  }
  .replay-controls .speed-group label { font-size: 11px; color: #6b7280; white-space: nowrap; }

  .node-flash { animation: flash 0.4s ease-out; }
  @keyframes flash { 0% { filter: brightness(2); } 100% { filter: brightness(1); } }
</style>
</head>
<body>

<div class="header">
  <h1>Gossip Simulation Dashboard</h1>
  <div class="status-badge">
    <div class="status-dot" id="statusDot"></div>
    <span id="statusText">Connecting...</span>
  </div>
</div>

<div class="main">
  <div class="graph-panel">
    <canvas id="graphCanvas"></canvas>
  </div>
  <div class="side-panel">
    <div class="stats-panel">
      <h2>Stats</h2>
      <div class="stats-grid">
        <div class="stat-item"><div class="stat-value" id="statNodes">0</div><div class="stat-label">Nodes</div></div>
        <div class="stat-item"><div class="stat-value" id="statEdges">0</div><div class="stat-label">Edges</div></div>
        <div class="stat-item"><div class="stat-value" id="statMessages">0</div><div class="stat-label">Messages</div></div>
        <div class="stat-item"><div class="stat-value" id="statRound">0/0</div><div class="stat-label">Round</div></div>
      </div>
    </div>
    <div class="worker-panel">
      <h2>Worker Logs</h2>
      <div class="worker-columns" id="workerColumns"></div>
    </div>
    <div class="event-panel">
      <h2>Event Log</h2>
      <div class="replay-controls" id="replayControls" style="display:none">
        <button id="btnFirst" title="First">|&#9664;</button>
        <button id="btnPrev" title="Previous">&#9664;</button>
        <button id="btnPlay" title="Play">&#9654;</button>
        <button id="btnNext" title="Next">&#9654;</button>
        <button id="btnLast" title="Last">&#9654;|</button>
        <input type="range" id="replaySlider" min="0" max="0" value="0">
        <span class="replay-pos" id="replayPos">0/0</span>
        <span class="speed-group">
          <input type="number" id="speedInput" value="100" min="10" max="2000" step="10">
          <label>ms/event</label>
        </span>
      </div>
      <div class="event-table-wrap" id="eventTableWrap">
        <table>
          <thead><tr><th>Seq</th><th>Round</th><th>Thread</th><th>Node</th><th>Event</th><th>Details</th></tr></thead>
          <tbody id="eventTableBody"></tbody>
        </table>
      </div>
    </div>
  </div>
</div>

<script>
const MODE = "__DASHBOARD_MODE__"; // replaced by server: "live" or "replay"
const MAX_TABLE_ROWS = 2000;

// ── State ──────────────────────────────────────────────────────────
let nodes = [];
let edges = [];
let nodePositions = {};
let allEvents = [];
let replayCursor = 0;
let workerLogs = {};
let isDone = false;

// ── Canvas / Graph ─────────────────────────────────────────────────
const canvas = document.getElementById('graphCanvas');
const ctx2d = canvas.getContext('2d');
let nodeFlash = {}; // nodeName -> timestamp
let edgeFlash = {}; // "a->b" -> timestamp

function resizeCanvas() {
  const rect = canvas.parentElement.getBoundingClientRect();
  canvas.width = rect.width * devicePixelRatio;
  canvas.height = rect.height * devicePixelRatio;
  canvas.style.width = rect.width + 'px';
  canvas.style.height = rect.height + 'px';
  ctx2d.setTransform(devicePixelRatio, 0, 0, devicePixelRatio, 0, 0);
}
window.addEventListener('resize', () => { resizeCanvas(); drawGraph(); });

function initPositions() {
  const rect = canvas.parentElement.getBoundingClientRect();
  const cx = rect.width / 2, cy = rect.height / 2;
  const r = Math.min(cx, cy) * 0.7;
  const n = nodes.length;
  nodes.forEach((node, i) => {
    const angle = (2 * Math.PI * i) / n - Math.PI / 2;
    nodePositions[node.name] = { x: cx + r * Math.cos(angle), y: cy + r * Math.sin(angle) };
  });
  // Run force simulation
  runForceLayout(rect.width, rect.height);
}

function runForceLayout(w, h) {
  const pos = nodePositions;
  const cx = w / 2, cy = h / 2;
  const k = Math.sqrt((w * h) / Math.max(nodes.length, 1));

  for (let iter = 0; iter < 300; iter++) {
    const disp = {};
    nodes.forEach(n => { disp[n.name] = { x: 0, y: 0 }; });

    // Repulsion
    for (let i = 0; i < nodes.length; i++) {
      for (let j = i + 1; j < nodes.length; j++) {
        const a = nodes[i].name, b = nodes[j].name;
        let dx = pos[a].x - pos[b].x, dy = pos[a].y - pos[b].y;
        let dist = Math.sqrt(dx * dx + dy * dy) || 0.01;
        let force = (k * k) / dist;
        let fx = (dx / dist) * force, fy = (dy / dist) * force;
        disp[a].x += fx; disp[a].y += fy;
        disp[b].x -= fx; disp[b].y -= fy;
      }
    }

    // Attraction (edges)
    edges.forEach(([a, b]) => {
      if (!pos[a] || !pos[b]) return;
      let dx = pos[a].x - pos[b].x, dy = pos[a].y - pos[b].y;
      let dist = Math.sqrt(dx * dx + dy * dy) || 0.01;
      let force = (dist * dist) / k;
      let fx = (dx / dist) * force, fy = (dy / dist) * force;
      disp[a].x -= fx; disp[a].y -= fy;
      disp[b].x += fx; disp[b].y += fy;
    });

    // Gravity toward center
    nodes.forEach(n => {
      let dx = pos[n.name].x - cx, dy = pos[n.name].y - cy;
      let dist = Math.sqrt(dx * dx + dy * dy) || 0.01;
      disp[n.name].x -= dx * 0.01;
      disp[n.name].y -= dy * 0.01;
    });

    // Apply with damping
    const temp = Math.max(0.1, 1 - iter / 300);
    nodes.forEach(n => {
      let d = disp[n.name];
      let dist = Math.sqrt(d.x * d.x + d.y * d.y) || 0.01;
      let cap = Math.min(dist, 10 * temp);
      pos[n.name].x += (d.x / dist) * cap;
      pos[n.name].y += (d.y / dist) * cap;
      // Keep within bounds
      pos[n.name].x = Math.max(40, Math.min(w - 40, pos[n.name].x));
      pos[n.name].y = Math.max(40, Math.min(h - 40, pos[n.name].y));
    });
  }
}

function drawGraph() {
  const rect = canvas.parentElement.getBoundingClientRect();
  const w = rect.width, h = rect.height;
  ctx2d.clearRect(0, 0, w, h);
  const now = performance.now();

  // Draw edges
  edges.forEach(([a, b]) => {
    const pa = nodePositions[a], pb = nodePositions[b];
    if (!pa || !pb) return;
    const key = a + '->' + b;
    const flash = edgeFlash[key];
    let alpha = 0.25;
    if (flash && now - flash < 600) {
      alpha = 0.25 + 0.75 * (1 - (now - flash) / 600);
    }
    ctx2d.beginPath();
    ctx2d.moveTo(pa.x, pa.y);
    ctx2d.lineTo(pb.x, pb.y);
    ctx2d.strokeStyle = `rgba(99, 102, 241, ${alpha})`;
    ctx2d.lineWidth = flash && now - flash < 600 ? 2.5 : 1;
    ctx2d.stroke();
  });

  // Draw nodes
  nodes.forEach(node => {
    const p = nodePositions[node.name];
    if (!p) return;
    const flash = nodeFlash[node.name];
    let radius = 8;
    let color = '#6366f1';
    if (flash && now - flash < 400) {
      const t = 1 - (now - flash) / 400;
      radius = 8 + 6 * t;
      color = '#818cf8';
    }
    ctx2d.beginPath();
    ctx2d.arc(p.x, p.y, radius, 0, 2 * Math.PI);
    ctx2d.fillStyle = color;
    ctx2d.fill();
    ctx2d.strokeStyle = '#4f46e5';
    ctx2d.lineWidth = 1.5;
    ctx2d.stroke();

    ctx2d.fillStyle = '#c7d2fe';
    ctx2d.font = '11px system-ui, sans-serif';
    ctx2d.textAlign = 'center';
    ctx2d.fillText(node.name, p.x, p.y + radius + 14);
  });

  // Continue animation if flashes active
  let anyFlash = false;
  for (const t of Object.values(nodeFlash)) { if (now - t < 400) anyFlash = true; }
  for (const t of Object.values(edgeFlash)) { if (now - t < 600) anyFlash = true; }
  if (anyFlash) requestAnimationFrame(drawGraph);
}

// ── UI updates ─────────────────────────────────────────────────────
function updateStats(s) {
  document.getElementById('statNodes').textContent = s.total_nodes;
  document.getElementById('statEdges').textContent = s.total_edges;
  document.getElementById('statMessages').textContent = s.total_messages;
  document.getElementById('statRound').textContent = s.current_round + '/' + s.total_rounds;
}

function makeRow(ev) {
  const tr = document.createElement('tr');
  if (ev.kind === 'GossipRoundStarted') tr.className = 'highlight-push';
  if (ev.kind === 'LocalSet') tr.className = 'highlight-set';
  const detailStr = formatDetail(ev.kind, ev.detail);
  tr.innerHTML = `<td>${ev.seq}</td><td>${ev.tick}</td><td>${ev.thread || '-'}</td><td>${ev.node}</td><td>${ev.kind}</td><td>${detailStr}</td>`;
  return tr;
}

function addEventToTable(ev) {
  const tbody = document.getElementById('eventTableBody');
  if (tbody.children.length >= MAX_TABLE_ROWS) {
    tbody.removeChild(tbody.firstChild);
  }
  tbody.appendChild(makeRow(ev));

  const wrap = document.getElementById('eventTableWrap');
  wrap.scrollTop = wrap.scrollHeight;
}

function formatDetail(kind, detail) {
  if (!detail) return '';
  switch (kind) {
    case 'GossipRoundStarted': return '&rarr; ' + detail.target;
    case 'PushReceived': return '&larr; ' + detail.from + ' (' + detail.keys_updated + ' keys)';
    case 'LocalSet': return 'key=' + detail.key;
    case 'PeerAdded': return '+ ' + detail.peer;
    case 'PeerRemoved': return '- ' + detail.peer;
    case 'QueryReceived': return 'key=' + detail.key;
    case 'StateSnapshot': return detail.entries + ' entries, ' + detail.peer_count + ' peers';
    default: return JSON.stringify(detail);
  }
}

function addWorkerEntry(thread, text) {
  if (!thread) return;
  if (!workerLogs[thread]) {
    workerLogs[thread] = [];
    rebuildWorkerColumns();
  }
  workerLogs[thread].push(text);
  if (workerLogs[thread].length > 50) workerLogs[thread].shift();
  updateWorkerColumn(thread);
}

function rebuildWorkerColumns() {
  const container = document.getElementById('workerColumns');
  container.innerHTML = '';
  for (const thread of Object.keys(workerLogs).sort()) {
    const col = document.createElement('div');
    col.className = 'worker-col';
    col.id = 'worker-' + thread;
    col.innerHTML = '<div class="worker-col-header">' + thread + '</div>';
    container.appendChild(col);
  }
}

function updateWorkerColumn(thread) {
  const col = document.getElementById('worker-' + thread);
  if (!col) return;
  const entries = workerLogs[thread];
  // Keep header + entries
  let html = '<div class="worker-col-header">' + thread + '</div>';
  for (const e of entries) {
    html += '<div class="worker-entry">' + e + '</div>';
  }
  col.innerHTML = html;
  col.scrollTop = col.scrollHeight;
}

function processEvent(ev) {
  addEventToTable(ev);

  // Flash node
  nodeFlash[ev.node] = performance.now();

  // Flash edges on Push
  if (ev.kind === 'GossipRoundStarted' && ev.detail && ev.detail.target) {
    edgeFlash[ev.node + '->' + ev.detail.target] = performance.now();
    edgeFlash[ev.detail.target + '->' + ev.node] = performance.now();
  }
  if (ev.kind === 'PushReceived' && ev.detail && ev.detail.from) {
    edgeFlash[ev.detail.from + '->' + ev.node] = performance.now();
    edgeFlash[ev.node + '->' + ev.detail.from] = performance.now();
  }

  // Worker log
  let text = ev.node + ': ' + ev.kind;
  if (ev.kind === 'GossipRoundStarted') text += ' -> ' + ev.detail.target;
  if (ev.kind === 'PushReceived') text += ' <- ' + ev.detail.from;
  addWorkerEntry(ev.thread, text);

  requestAnimationFrame(drawGraph);
}

// ── Live mode (SSE) ────────────────────────────────────────────────
function startLive() {
  const dot = document.getElementById('statusDot');
  const statusText = document.getElementById('statusText');

  const es = new EventSource('/events');

  es.addEventListener('init', (e) => {
    const data = JSON.parse(e.data);
    nodes = data.nodes;
    edges = data.edges;
    statusText.textContent = 'Live: ' + data.name;
    dot.className = 'status-dot';
    resizeCanvas();
    initPositions();
    drawGraph();
  });

  es.addEventListener('gossip', (e) => {
    const ev = JSON.parse(e.data);
    allEvents.push(ev);
    processEvent(ev);
  });

  es.addEventListener('stats', (e) => {
    const data = JSON.parse(e.data);
    updateStats(data);
  });

  es.addEventListener('done', (e) => {
    isDone = true;
    dot.className = 'status-dot done';
    statusText.textContent += ' (done)';
    es.close();
  });

  es.onerror = () => {
    if (!isDone) {
      statusText.textContent = 'Disconnected';
      dot.style.background = '#ef4444';
    }
  };
}

// ── Replay mode ────────────────────────────────────────────────────
async function startReplay() {
  const dot = document.getElementById('statusDot');
  const statusText = document.getElementById('statusText');
  dot.className = 'status-dot replay';

  const resp = await fetch('/trace.json');
  const trace = await resp.json();

  nodes = trace.node_names.map((name, i) => ({
    name,
    addr: trace.node_addrs[i] ? JSON.stringify(trace.node_addrs[i]) : ''
  }));
  edges = trace.topology_edges.map(e => [e[0], e[1]]);

  // Build events list (filter out StateSnapshot for display)
  let seq = 0;
  allEvents = trace.events
    .filter(ev => ev.kind !== 'StateSnapshot')
    .map(ev => {
      const kind = typeof ev.kind === 'string' ? ev.kind : Object.keys(ev.kind)[0];
      const detail = typeof ev.kind === 'string' ? {} : ev.kind[kind] || {};
      return {
        seq: seq++,
        tick: ev.tick,
        node: ev.node_name,
        thread: ev.thread_name,
        kind,
        detail
      };
    });

  statusText.textContent = 'Replay: ' + trace.name + ' (' + allEvents.length + ' events)';
  updateStats({
    total_nodes: trace.node_names.length,
    total_edges: trace.topology_edges.length,
    total_messages: allEvents.length,
    current_round: trace.num_rounds,
    total_rounds: trace.num_rounds
  });

  resizeCanvas();
  initPositions();
  drawGraph();

  // Show replay controls
  const controls = document.getElementById('replayControls');
  controls.style.display = 'flex';
  const slider = document.getElementById('replaySlider');
  slider.max = allEvents.length;
  slider.value = 0;
  replayCursor = 0;
  updateReplayPos();

  document.getElementById('btnFirst').onclick = () => { stopPlayback(); replayTo(0); };
  document.getElementById('btnPrev').onclick = () => { stopPlayback(); replayTo(Math.max(0, replayCursor - 1)); };
  document.getElementById('btnNext').onclick = () => { stopPlayback(); replayTo(Math.min(allEvents.length, replayCursor + 1)); };
  document.getElementById('btnLast').onclick = () => { stopPlayback(); replayTo(allEvents.length); };
  slider.oninput = () => { stopPlayback(); replayTo(parseInt(slider.value)); };

  document.getElementById('btnPlay').onclick = () => {
    if (playTimer !== null) { stopPlayback(); } else { startPlayback(); }
  };
  document.getElementById('speedInput').onchange = () => {
    if (playTimer !== null) { startPlayback(); }
  };
}

let replayRafId = 0;
let playTimer = null;

function stopPlayback() {
  if (playTimer !== null) {
    clearInterval(playTimer);
    playTimer = null;
    document.getElementById('btnPlay').innerHTML = '&#9654;';
    document.getElementById('btnPlay').title = 'Play';
  }
}

function startPlayback() {
  stopPlayback();
  if (replayCursor >= allEvents.length) return; // already at end
  const speed = parseInt(document.getElementById('speedInput').value) || 100;
  document.getElementById('btnPlay').innerHTML = '&#9208;';
  document.getElementById('btnPlay').title = 'Pause';
  playTimer = setInterval(() => {
    if (replayCursor >= allEvents.length) {
      stopPlayback();
      return;
    }
    replayTo(replayCursor + 1);
  }, speed);
}

function replayTo(pos) {
  // Debounce: only run once per animation frame
  replayCursor = pos;
  if (replayRafId) return;
  replayRafId = requestAnimationFrame(() => {
    replayRafId = 0;
    replayToImpl(replayCursor);
  });
}

function replayToImpl(pos) {
  nodeFlash = {};
  edgeFlash = {};

  // ── Build table rows into a fragment (no reflows) ──
  const frag = document.createDocumentFragment();
  const start = Math.max(0, pos - MAX_TABLE_ROWS);
  for (let i = start; i < pos && i < allEvents.length; i++) {
    frag.appendChild(makeRow(allEvents[i]));
  }
  const tbody = document.getElementById('eventTableBody');
  tbody.textContent = '';  // fast clear
  tbody.appendChild(frag);

  const wrap = document.getElementById('eventTableWrap');
  wrap.scrollTop = wrap.scrollHeight;

  // ── Rebuild worker logs in one pass ──
  workerLogs = {};
  const workerStart = Math.max(0, pos - 200); // only last ~200 events for worker logs
  for (let i = workerStart; i < pos && i < allEvents.length; i++) {
    const ev = allEvents[i];
    if (!ev.thread) continue;
    if (!workerLogs[ev.thread]) workerLogs[ev.thread] = [];
    let text = ev.node + ': ' + ev.kind;
    if (ev.kind === 'GossipRoundStarted' && ev.detail) text += ' -> ' + ev.detail.target;
    if (ev.kind === 'PushReceived' && ev.detail) text += ' <- ' + ev.detail.from;
    workerLogs[ev.thread].push(text);
    if (workerLogs[ev.thread].length > 50) workerLogs[ev.thread].shift();
  }
  rebuildWorkerColumns();
  for (const thread of Object.keys(workerLogs)) {
    updateWorkerColumn(thread);
  }

  // Flash the last event
  if (pos > 0 && pos <= allEvents.length) {
    const ev = allEvents[pos - 1];
    nodeFlash[ev.node] = performance.now();
    if (ev.kind === 'GossipRoundStarted' && ev.detail && ev.detail.target) {
      edgeFlash[ev.node + '->' + ev.detail.target] = performance.now();
    }
    if (ev.kind === 'PushReceived' && ev.detail && ev.detail.from) {
      edgeFlash[ev.detail.from + '->' + ev.node] = performance.now();
    }
  }

  document.getElementById('replaySlider').value = pos;
  updateReplayPos();
  drawGraph();
}

function updateReplayPos() {
  document.getElementById('replayPos').textContent = replayCursor + '/' + allEvents.length;
}

// ── Boot ───────────────────────────────────────────────────────────
resizeCanvas();
if (MODE === 'replay') {
  startReplay();
} else {
  startLive();
}
</script>
</body>
</html>
"##;
