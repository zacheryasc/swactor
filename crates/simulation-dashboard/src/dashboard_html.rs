pub const DASHBOARD_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Simulation Dashboard</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; background: #0f1117; color: #e0e0e0; }
  .header { display: flex; align-items: center; justify-content: space-between; padding: 12px 20px; background: #161822; border-bottom: 1px solid #2a2d3a; }
  .header h1 { font-size: 18px; font-weight: 600; color: #c0c6d4; }
  .trace-select { background: #1a1d2e; color: #e0e0e0; border: 1px solid #2a2d3a; border-radius: 4px; padding: 6px 12px; font-size: 13px; cursor: pointer; min-width: 240px; max-width: 420px; }
  .trace-select:hover { border-color: #4f46e5; }
  .trace-select:focus { outline: none; border-color: #6366f1; }
  .trace-select:disabled { cursor: default; opacity: 0.5; }
  .status-badge { display: flex; align-items: center; gap: 6px; font-size: 13px; color: #9ca3af; }
  .status-dot { width: 8px; height: 8px; border-radius: 50%; background: #f59e0b; }
  .status-dot.ready { background: #22c55e; }
  .status-dot.loading { background: #3b82f6; animation: pulse 1s infinite; }
  .status-dot.error { background: #ef4444; }
  @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.4; } }
  .main { display: grid; grid-template-columns: 1fr 1fr; grid-template-rows: auto 1fr; height: calc(100vh - 48px); }
  .graph-panel { grid-row: 1 / 3; border-right: 1px solid #2a2d3a; position: relative; }
  canvas { width: 100%; height: 100%; display: block; }
  .layout-progress { position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); background: rgba(22,24,34,0.9); border: 1px solid #2a2d3a; border-radius: 8px; padding: 16px 24px; text-align: center; font-size: 13px; color: #9ca3af; display: none; z-index: 10; }
  .layout-progress .progress-bar { width: 200px; height: 4px; background: #2a2d3a; border-radius: 2px; margin-top: 8px; overflow: hidden; }
  .layout-progress .progress-fill { height: 100%; background: #6366f1; border-radius: 2px; width: 0%; transition: width 0.1s; }
  .side-panel { display: flex; flex-direction: column; overflow: hidden; min-height: 0; }
  .stats-panel { flex-shrink: 0; padding: 12px 16px; border-bottom: 1px solid #2a2d3a; background: #161822; }
  .stats-panel h2 { font-size: 13px; color: #6b7280; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 8px; }
  .stats-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; }
  .stat-item .stat-value { font-size: 22px; font-weight: 700; color: #e5e7eb; }
  .stat-item .stat-label { font-size: 11px; color: #6b7280; text-transform: uppercase; }
  .worker-panel { flex-shrink: 0; padding: 12px 16px; border-bottom: 1px solid #2a2d3a; height: 180px; overflow-y: auto; }
  .worker-panel h2 { font-size: 13px; color: #6b7280; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 8px; }
  .worker-columns { display: flex; gap: 8px; overflow-x: auto; }
  .worker-col { flex: 1; min-width: 160px; background: #1a1d2e; border-radius: 6px; padding: 8px; font-size: 11px; max-height: 140px; overflow-y: auto; }
  .worker-col-header { font-weight: 600; color: #818cf8; margin-bottom: 4px; font-size: 12px; }
  .worker-entry { color: #9ca3af; padding: 1px 0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .event-panel { flex: 1; min-height: 0; display: flex; flex-direction: column; overflow: hidden; }
  .event-panel h2 { font-size: 13px; color: #6b7280; text-transform: uppercase; letter-spacing: 0.05em; padding: 12px 16px 8px; }
  .event-table-wrap { flex: 1; overflow-y: auto; padding: 0 16px 8px; }
  table { width: 100%; border-collapse: collapse; font-size: 12px; }
  th { text-align: left; color: #6b7280; font-weight: 500; padding: 4px 8px; border-bottom: 1px solid #2a2d3a; position: sticky; top: 0; background: #0f1117; }
  td { padding: 3px 8px; border-bottom: 1px solid #1a1d2e; white-space: nowrap; }
  tr.highlight-push td { background: rgba(59,130,246,0.1); }
  tr.highlight-set td { background: rgba(34,197,94,0.1); }
  .replay-controls { display: flex; align-items: center; gap: 8px; padding: 8px 16px; background: #161822; border-bottom: 1px solid #2a2d3a; }
  .replay-controls button { background: #2a2d3a; color: #e0e0e0; border: none; border-radius: 4px; padding: 4px 10px; cursor: pointer; font-size: 13px; }
  .replay-controls button:hover { background: #3b3f52; }
  .replay-controls input[type=range] { flex: 1; }
  .replay-controls .replay-pos { font-size: 12px; color: #9ca3af; min-width: 60px; text-align: right; }
  .replay-controls .speed-group { display: flex; align-items: center; gap: 4px; margin-left: 8px; }
  .replay-controls .speed-group input { width: 64px; background: #1a1d2e; color: #e0e0e0; border: 1px solid #2a2d3a; border-radius: 4px; padding: 2px 6px; font-size: 12px; text-align: right; }
  .replay-controls .speed-group label { font-size: 11px; color: #6b7280; white-space: nowrap; }
  .tab-bar { display: flex; border-bottom: 1px solid #2a2d3a; background: #161822; flex-shrink: 0; }
  .tab-btn { flex: 1; padding: 8px 0; font-size: 13px; font-weight: 500; color: #6b7280; background: none; border: none; border-bottom: 2px solid transparent; cursor: pointer; text-align: center; }
  .tab-btn:hover { color: #9ca3af; }
  .tab-btn.active { color: #818cf8; border-bottom-color: #818cf8; }
  .tab-content { display: none; flex: 1; flex-direction: column; overflow: hidden; min-height: 0; }
  .tab-content.active { display: flex; }
  .analytics-scroll { flex: 1; overflow-y: auto; padding: 12px 16px; }
  .badge-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; margin-bottom: 16px; }
  .badge { background: #1a1d2e; border-radius: 6px; padding: 10px 12px; border-left: 3px solid #6b7280; }
  .badge.green { border-left-color: #22c55e; }
  .badge.yellow { border-left-color: #f59e0b; }
  .badge.red { border-left-color: #ef4444; }
  .badge .badge-val { font-size: 18px; font-weight: 700; color: #e5e7eb; }
  .badge .badge-lbl { font-size: 11px; color: #6b7280; text-transform: uppercase; }
  .chart-section { margin-bottom: 16px; }
  .chart-section h3 { font-size: 12px; color: #6b7280; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 6px; }
  .chart-section canvas { width: 100%; border-radius: 4px; background: #1a1d2e; display: block; }
  .chart-empty { padding: 24px; text-align: center; color: #4b5563; font-size: 13px; }
  .node-detail { position: absolute; top: 60px; left: 20px; width: 280px; background: rgba(22,24,34,0.95); border: 1px solid #2a2d3a; border-radius: 8px; padding: 14px; z-index: 20; display: none; font-size: 12px; }
  .node-detail h3 { font-size: 14px; color: #c7d2fe; margin-bottom: 8px; }
  .node-detail .close-btn { position: absolute; top: 8px; right: 10px; background: none; border: none; color: #6b7280; cursor: pointer; font-size: 16px; }
  .node-detail .close-btn:hover { color: #e0e0e0; }
  .node-detail .nd-row { display: flex; justify-content: space-between; padding: 3px 0; border-bottom: 1px solid #1a1d2e; }
  .node-detail .nd-key { color: #6b7280; }
  .node-detail .nd-val { color: #e5e7eb; font-weight: 500; }
  .node-detail .nd-events { max-height: 160px; overflow-y: auto; margin-top: 8px; }
  .node-detail .nd-ev { color: #9ca3af; padding: 1px 0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; font-size: 11px; }
  .heatmap-wrap { position: relative; }
  .heatmap-tooltip { position: absolute; display: none; background: rgba(22,24,34,0.95); border: 1px solid #2a2d3a; border-radius: 4px; padding: 4px 8px; font-size: 11px; color: #e0e0e0; white-space: nowrap; pointer-events: none; z-index: 5; }
</style>
</head>
<body>
<div class="header">
  <h1>Simulation Dashboard</h1>
  <select id="traceSelect" class="trace-select" disabled><option value="">Loading traces...</option></select>
  <div class="status-badge">
    <div class="status-dot loading" id="statusDot"></div>
    <span id="statusText">Loading traces...</span>
  </div>
</div>
<div class="main">
  <div class="graph-panel">
    <canvas id="graphCanvas"></canvas>
    <div class="layout-progress" id="layoutProgress">
      <div>Computing layout...</div>
      <div class="progress-bar"><div class="progress-fill" id="layoutProgressFill"></div></div>
    </div>
    <div class="node-detail" id="nodeDetail">
      <button class="close-btn" id="nodeDetailClose">&times;</button>
      <h3 id="ndName"></h3>
      <div id="ndStats"></div>
      <div class="nd-events" id="ndEvents"></div>
    </div>
  </div>
  <div class="side-panel">
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
    <div class="tab-bar">
      <button class="tab-btn active" data-tab="replay">Replay</button>
      <button class="tab-btn" data-tab="analytics">Analytics</button>
    </div>
    <div class="tab-content active" id="tab-replay">
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
        <div class="event-table-wrap" id="eventTableWrap">
          <table>
            <thead><tr><th>Seq</th><th>Round</th><th>Thread</th><th>Node</th><th>Event</th><th>Details</th></tr></thead>
            <tbody id="eventTableBody"></tbody>
          </table>
        </div>
      </div>
    </div>
    <div class="tab-content" id="tab-analytics">
      <div class="analytics-scroll">
        <div class="badge-grid" id="badgeGrid"></div>
        <div class="chart-section" id="convSection">
          <h3>Convergence Curve</h3>
          <canvas id="convCanvas" height="180"></canvas>
        </div>
        <div class="chart-section" id="heatSection">
          <h3>Propagation Heatmap</h3>
          <div class="heatmap-wrap">
            <canvas id="heatCanvas" height="250"></canvas>
            <div class="heatmap-tooltip" id="heatTooltip"></div>
          </div>
        </div>
        <div class="chart-section" id="loadSection">
          <h3>Load Distribution</h3>
          <canvas id="loadCanvas" height="150"></canvas>
        </div>
      </div>
    </div>
  </div>
</div>

<script>
const MAX_TABLE_ROWS = 2000;

// ── Node data (indexed by integer) ────────────────────────────────
let nodeNames = [];          // nodeNames[i] = "node_42"
let nodeIdx = new Map();     // "node_42" -> 0
let N = 0;
let posX = new Float64Array(0);
let posY = new Float64Array(0);

// ── Edge data (flat typed arrays, sorted by appearance index) ─────
let totalEdges = 0;
let edgeSrc = new Int32Array(0);    // source node index
let edgeDst = new Int32Array(0);    // dest node index
let edgeAppear = new Int32Array(0); // appearance event index (-1 = initial)
let visEdges = 0;                   // edges[0..visEdges-1] are visible

// ── Flash state (at most 1 node + 1 edge at a time) ──────────────
let flashNode = -1, flashNodeT = 0;
let flashSrc = -1, flashDst = -1, flashEdgeT = 0;

// ── Events & replay ──────────────────────────────────────────────
let allEvents = [];
let replayCursor = 0;
let workerLogs = {};
let lastTablePos = -1, lastWorkerPos = -1;

// ── Analytics data ───────────────────────────────────────────────
let snapRounds = [];         // unique tick numbers that have snapshots
let snapEntries = [];        // snapEntries[roundIdx][nodeIdx] = entry count
let snapPeerCount = [];      // snapPeerCount[roundIdx][nodeIdx] = peer count
let totalKeys = 0;           // max entries seen across all snapshots
let numRounds = 0;           // total rounds from trace

let metricPushesSent = new Int32Array(0);
let metricPushesRecv = new Int32Array(0);
let metricRedundant = 0;
let metricTotalPushes = 0;
let metricNoPeers = 0;

// cumulative push-recv per round: cumulPushRecv[roundIdx][nodeIdx]
let cumulPushRecv = [];
// round number for each event index: eventRoundIdx[evtIdx] = roundIdx into snapRounds
let eventRoundMap = [];      // eventRoundMap[evtIdx] = tick

// ── Community detection ──────────────────────────────────────────
let community = new Int32Array(0);  // community[nodeIdx] = community id
let numCommunities = 0;
let communityHulls = [];   // communityHulls[cid] = [[x,y], ...]
let communityColors = ['#6366f1','#22c55e','#f59e0b','#ef4444','#3b82f6','#a855f7','#ec4899','#14b8a6'];

// ── View transform ───────────────────────────────────────────────
let vx = 0, vy = 0, vs = 1; // view x, y, scale
let isDragging = false, dragX = 0, dragY = 0, dragVx = 0, dragVy = 0;

// ── Tab switching ────────────────────────────────────────────────
document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById('tab-' + btn.dataset.tab).classList.add('active');
    if (btn.dataset.tab === 'analytics') drawAllCharts();
  });
});

// ── Quadtree pool (flat Float64Array, reused across iterations) ──
// Per node: [ox, oy, size, cx, cy, mass, c0, c1, c2, c3, body]
const QF = 11;
let qt = new Float64Array(0);
let qtN = 0;
let _fx = 0, _fy = 0; // force accumulator (avoids object alloc)

let layoutAbortFlag = false;

// ── Canvas ───────────────────────────────────────────────────────
const canvas = document.getElementById('graphCanvas');
const ctx2d = canvas.getContext('2d');

function resizeCanvas() {
  const r = canvas.parentElement.getBoundingClientRect();
  canvas.width = r.width * devicePixelRatio;
  canvas.height = r.height * devicePixelRatio;
  canvas.style.width = r.width + 'px';
  canvas.style.height = r.height + 'px';
}
window.addEventListener('resize', () => { resizeCanvas(); drawGraph(); });

// ── Zoom & pan ───────────────────────────────────────────────────
canvas.addEventListener('wheel', (e) => {
  e.preventDefault();
  const r = canvas.getBoundingClientRect();
  const mx = e.clientX - r.left, my = e.clientY - r.top;
  const f = e.deltaY < 0 ? 1.1 : 1 / 1.1;
  const ns = Math.max(0.05, Math.min(5, vs * f));
  const ratio = ns / vs;
  vx = mx - (mx - vx) * ratio;
  vy = my - (my - vy) * ratio;
  vs = ns;
  drawGraph();
}, { passive: false });

let clickStartX = 0, clickStartY = 0;
canvas.addEventListener('mousedown', (e) => {
  if (e.button !== 0) return;
  isDragging = true;
  dragX = e.clientX; dragY = e.clientY;
  clickStartX = e.clientX; clickStartY = e.clientY;
  dragVx = vx; dragVy = vy;
  canvas.style.cursor = 'grabbing';
});
window.addEventListener('mousemove', (e) => {
  if (!isDragging) return;
  vx = dragVx + (e.clientX - dragX);
  vy = dragVy + (e.clientY - dragY);
  drawGraph();
});
window.addEventListener('mouseup', (e) => {
  if (isDragging) {
    const wasDrag = Math.abs(e.clientX - clickStartX) > 3 || Math.abs(e.clientY - clickStartY) > 3;
    isDragging = false; canvas.style.cursor = '';
    if (!wasDrag) handleNodeClick(e);
  }
});
canvas.addEventListener('dblclick', () => { resetView(); drawGraph(); });

function resetView() {
  if (N === 0) { vx = 0; vy = 0; vs = 1; return; }
  const r = canvas.parentElement.getBoundingClientRect();
  const w = r.width, h = r.height;
  let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
  for (let i = 0; i < N; i++) {
    if (posX[i] < x0) x0 = posX[i]; if (posY[i] < y0) y0 = posY[i];
    if (posX[i] > x1) x1 = posX[i]; if (posY[i] > y1) y1 = posY[i];
  }
  if (!isFinite(x0)) { vx = 0; vy = 0; vs = 1; return; }
  const pad = 40;
  vs = Math.min(w / (x1 - x0 + pad * 2), h / (y1 - y0 + pad * 2), 2);
  vx = (w - (x0 + x1) * vs) / 2;
  vy = (h - (y0 + y1) * vs) / 2;
}

// ── Quadtree (flat array, zero-alloc per iteration) ──────────────
function qtAlloc(ox, oy, sz) {
  if (qtN * QF >= qt.length) {
    const nq = new Float64Array(Math.max(qt.length * 2, 256 * QF));
    nq.set(qt); qt = nq;
  }
  const i = qtN++, o = i * QF;
  qt[o]=ox; qt[o+1]=oy; qt[o+2]=sz;
  qt[o+3]=0; qt[o+4]=0; qt[o+5]=0;
  qt[o+6]=-1; qt[o+7]=-1; qt[o+8]=-1; qt[o+9]=-1;
  qt[o+10]=-1;
  return i;
}

function qtBuild() {
  let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
  for (let i = 0; i < N; i++) {
    if (posX[i] < x0) x0 = posX[i]; if (posY[i] < y0) y0 = posY[i];
    if (posX[i] > x1) x1 = posX[i]; if (posY[i] > y1) y1 = posY[i];
  }
  const sz = Math.max(x1 - x0, y1 - y0, 1) + 2;
  qtN = 0;
  qtAlloc(x0 - 1, y0 - 1, sz);
  for (let i = 0; i < N; i++) qtIns(0, i, posX[i], posY[i]);
}

function qtIns(ni, bi, bx, by) {
  const o = ni * QF;
  if (qt[o+5] === 0) {
    qt[o+3] = bx; qt[o+4] = by; qt[o+5] = 1; qt[o+10] = bi;
    return;
  }
  if (qt[o+10] >= 0) {
    if (qt[o+2] < 0.001) { qt[o+5]++; return; } // cell too small, just accumulate
    const eb = qt[o+10], ex = qt[o+3], ey = qt[o+4];
    qt[o+10] = -1;
    qtInsChild(ni, eb, ex, ey);
  }
  const m = qt[o+5];
  qt[o+3] = (qt[o+3]*m + bx) / (m+1);
  qt[o+4] = (qt[o+4]*m + by) / (m+1);
  qt[o+5] = m + 1;
  qtInsChild(ni, bi, bx, by);
}

function qtInsChild(ni, bi, bx, by) {
  const o = ni * QF, hs = qt[o+2] / 2;
  const mx = qt[o] + hs, my = qt[o+1] + hs;
  const qx = bx < mx ? 0 : 1, qy = by < my ? 0 : 1;
  const ci = o + 6 + qy * 2 + qx;
  if (qt[ci] < 0) qt[ci] = qtAlloc(qx ? mx : qt[o], qy ? my : qt[o+1], hs);
  qtIns(qt[ci], bi, bx, by);
}

function qtCalc(ni, px, py, k2, th2) {
  if (ni < 0) return;
  const o = ni * QF;
  if (qt[o+5] === 0) return;
  const dx = qt[o+3] - px, dy = qt[o+4] - py;
  let d2 = dx*dx + dy*dy;
  if (d2 < 0.0001) d2 = 0.0001;
  if (qt[o+10] >= 0 || qt[o+2]*qt[o+2]/d2 < th2) {
    const d = Math.sqrt(d2), f = -(k2 * qt[o+5]) / d2;
    _fx += (dx/d)*f; _fy += (dy/d)*f;
    return;
  }
  for (let c = 6; c < 10; c++) if (qt[o+c] >= 0) qtCalc(qt[o+c], px, py, k2, th2);
}

// ── Force layout ─────────────────────────────────────────────────
function initPositions() {
  const r = canvas.parentElement.getBoundingClientRect();
  const cx = r.width / 2, cy = r.height / 2;
  const rad = Math.min(cx, cy) * 0.7;
  for (let i = 0; i < N; i++) {
    const a = (2 * Math.PI * i) / N - Math.PI / 2;
    posX[i] = cx + rad * Math.cos(a);
    posY[i] = cy + rad * Math.sin(a);
  }
}

function runForceLayout(w, h) {
  return new Promise(resolve => {
    layoutAbortFlag = false;
    const cx = w/2, cy = h/2;
    const k = Math.sqrt(w*h / Math.max(N, 1)), k2 = k*k;
    const totalIters = Math.min(300, Math.max(50, Math.floor(40000 / Math.max(N, 1))));
    const CHUNK = 50, useBH = N > 100, th2 = 0.64;
    const dx = new Float64Array(N), dy = new Float64Array(N);
    const progressEl = document.getElementById('layoutProgress');
    const fillEl = document.getElementById('layoutProgressFill');
    if (N > 50) { progressEl.style.display = 'block'; fillEl.style.width = '0%'; }
    let iter = 0;

    function doChunk() {
      if (layoutAbortFlag) { progressEl.style.display = 'none'; resolve(); return; }
      const end = Math.min(iter + CHUNK, totalIters);
      for (; iter < end; iter++) {
        dx.fill(0); dy.fill(0);

        if (useBH) {
          qtBuild();
          for (let i = 0; i < N; i++) {
            _fx = 0; _fy = 0;
            qtCalc(0, posX[i], posY[i], k2, th2);
            dx[i] += _fx; dy[i] += _fy;
          }
        } else {
          for (let i = 0; i < N; i++) for (let j = i+1; j < N; j++) {
            let ddx = posX[i]-posX[j], ddy = posY[i]-posY[j];
            let dist = Math.sqrt(ddx*ddx + ddy*ddy) || 0.01;
            let f = k2/dist, fx = (ddx/dist)*f, fy = (ddy/dist)*f;
            dx[i] += fx; dy[i] += fy; dx[j] -= fx; dy[j] -= fy;
          }
        }

        for (let i = 0; i < visEdges; i++) {
          const si = edgeSrc[i], di = edgeDst[i];
          let ddx = posX[si]-posX[di], ddy = posY[si]-posY[di];
          let dist = Math.sqrt(ddx*ddx + ddy*ddy) || 0.01;
          let f = (dist*dist)/k, fx = (ddx/dist)*f, fy = (ddy/dist)*f;
          dx[si] -= fx; dy[si] -= fy; dx[di] += fx; dy[di] += fy;
        }

        for (let i = 0; i < N; i++) {
          dx[i] -= (posX[i]-cx)*0.01; dy[i] -= (posY[i]-cy)*0.01;
        }

        const temp = Math.max(0.1, 1 - iter/totalIters);
        for (let i = 0; i < N; i++) {
          let dist = Math.sqrt(dx[i]*dx[i] + dy[i]*dy[i]) || 0.01;
          let cap = Math.min(dist, 10*temp);
          posX[i] += (dx[i]/dist)*cap; posY[i] += (dy[i]/dist)*cap;
          posX[i] = Math.max(40, Math.min(w-40, posX[i]));
          posY[i] = Math.max(40, Math.min(h-40, posY[i]));
        }
      }
      fillEl.style.width = Math.round(100*iter/totalIters) + '%';
      if (iter < totalIters) { drawGraph(); setTimeout(doChunk, 0); }
      else { progressEl.style.display = 'none'; resetView(); drawGraph(); resolve(); }
    }
    doChunk();
  });
}

// ── Node click → detail panel ────────────────────────────────────
function handleNodeClick(e) {
  const r = canvas.getBoundingClientRect();
  const mx = e.clientX - r.left, my = e.clientY - r.top;
  const wx = (mx - vx) / vs, wy = (my - vy) / vs;
  const baseR = Math.max(2, Math.min(8, 400/Math.sqrt(Math.max(N, 1))));
  const hitR = baseR * 2;
  let best = -1, bestD = hitR * hitR;
  for (let i = 0; i < N; i++) {
    const dx = posX[i] - wx, dy = posY[i] - wy;
    const d2 = dx*dx + dy*dy;
    if (d2 < bestD) { bestD = d2; best = i; }
  }
  const panel = document.getElementById('nodeDetail');
  if (best < 0) { panel.style.display = 'none'; return; }
  showNodeDetail(best);
}

function showNodeDetail(ni) {
  const panel = document.getElementById('nodeDetail');
  document.getElementById('ndName').textContent = nodeNames[ni];

  // Find current round from replay position
  let curRound = 0;
  if (replayCursor > 0 && replayCursor <= allEvents.length) curRound = allEvents[replayCursor - 1].tick;

  // Key count from latest snapshot ≤ current round
  let keyCount = 0, peerCount = 0;
  for (let r = snapRounds.length - 1; r >= 0; r--) {
    if (snapRounds[r] <= curRound) { keyCount = snapEntries[r][ni] || 0; peerCount = snapPeerCount[r][ni] || 0; break; }
  }

  let html = '';
  html += '<div class="nd-row"><span class="nd-key">Community</span><span class="nd-val">' + (community[ni] !== undefined ? community[ni] : '-') + '</span></div>';
  html += '<div class="nd-row"><span class="nd-key">Pushes Sent</span><span class="nd-val">' + (metricPushesSent[ni] || 0) + '</span></div>';
  html += '<div class="nd-row"><span class="nd-key">Pushes Recv</span><span class="nd-val">' + (metricPushesRecv[ni] || 0) + '</span></div>';
  html += '<div class="nd-row"><span class="nd-key">Keys</span><span class="nd-val">' + keyCount + '/' + totalKeys + '</span></div>';
  html += '<div class="nd-row"><span class="nd-key">Peers</span><span class="nd-val">' + peerCount + '</span></div>';
  document.getElementById('ndStats').innerHTML = html;

  // Mini event log: last 20 events for this node up to cursor
  const name = nodeNames[ni];
  let evHtml = '<div style="font-size:11px;color:#6b7280;margin-bottom:4px">Recent events:</div>';
  let count = 0;
  for (let i = Math.min(replayCursor, allEvents.length) - 1; i >= 0 && count < 20; i--) {
    if (allEvents[i].node === name) {
      evHtml += '<div class="nd-ev">' + allEvents[i].kind + (allEvents[i].detail ? ': ' + formatDetail(allEvents[i].kind, allEvents[i].detail).replace(/&[lr]arr;/g, '→') : '') + '</div>';
      count++;
    }
  }
  document.getElementById('ndEvents').innerHTML = evHtml;

  panel.style.display = 'block';
}

document.getElementById('nodeDetailClose').addEventListener('click', () => {
  document.getElementById('nodeDetail').style.display = 'none';
});

// ── Community detection (BFS on visible edges) ──────────────────
function detectCommunities() {
  community = new Int32Array(N).fill(-1);
  numCommunities = 0;
  const adj = new Array(N);
  for (let i = 0; i < N; i++) adj[i] = [];
  for (let i = 0; i < totalEdges; i++) {
    adj[edgeSrc[i]].push(edgeDst[i]);
    adj[edgeDst[i]].push(edgeSrc[i]);
  }
  const queue = [];
  for (let i = 0; i < N; i++) {
    if (community[i] >= 0) continue;
    const cid = numCommunities++;
    community[i] = cid;
    queue.push(i);
    while (queue.length > 0) {
      const u = queue.pop();
      for (const v of adj[u]) {
        if (community[v] < 0) { community[v] = cid; queue.push(v); }
      }
    }
  }
}

// ── Convex hull (Graham scan) ───────────────────────────────────
function convexHull(points) {
  if (points.length < 3) return points.slice();
  points.sort((a, b) => a[0] - b[0] || a[1] - b[1]);
  const cross = (o, a, b) => (a[0]-o[0])*(b[1]-o[1]) - (a[1]-o[1])*(b[0]-o[0]);
  const lower = [];
  for (const p of points) {
    while (lower.length >= 2 && cross(lower[lower.length-2], lower[lower.length-1], p) <= 0) lower.pop();
    lower.push(p);
  }
  const upper = [];
  for (let i = points.length - 1; i >= 0; i--) {
    const p = points[i];
    while (upper.length >= 2 && cross(upper[upper.length-2], upper[upper.length-1], p) <= 0) upper.pop();
    upper.push(p);
  }
  lower.pop(); upper.pop();
  return lower.concat(upper);
}

function computeCommunityHulls() {
  communityHulls = [];
  for (let c = 0; c < numCommunities; c++) {
    const pts = [];
    for (let i = 0; i < N; i++) {
      if (community[i] === c) pts.push([posX[i], posY[i]]);
    }
    communityHulls.push(pts.length >= 3 ? convexHull(pts) : pts);
  }
}

// ── Analytics charts ────────────────────────────────────────────
function drawAllCharts() {
  drawBadges();
  drawConvergenceChart();
  drawHeatmap();
  drawLoadHistogram();
}

function drawBadges() {
  const grid = document.getElementById('badgeGrid');
  const hasSnaps = snapRounds.length > 0;

  // Delivery ratio
  let delivery = 1.0;
  if (hasSnaps) {
    const last = snapEntries[snapEntries.length - 1];
    let full = 0;
    for (let i = 0; i < N; i++) if (last[i] >= totalKeys && totalKeys > 0) full++;
    delivery = N > 0 ? full / N : 1;
  }

  // Convergence round
  let convRound = hasSnaps ? -1 : 0;
  if (hasSnaps && totalKeys > 0) {
    for (let r = 0; r < snapRounds.length; r++) {
      let allFull = true;
      for (let i = 0; i < N; i++) { if (snapEntries[r][i] < totalKeys) { allFull = false; break; } }
      if (allFull) { convRound = snapRounds[r]; break; }
    }
  }

  // Redundancy
  const redundancy = metricTotalPushes > 0 ? metricRedundant / metricTotalPushes : 0;

  // Load balance CV
  let mean = 0, variance = 0;
  if (N > 0) {
    for (let i = 0; i < N; i++) mean += metricPushesRecv[i];
    mean /= N;
    for (let i = 0; i < N; i++) { const d = metricPushesRecv[i] - mean; variance += d * d; }
    variance /= N;
  }
  const cv = mean > 0 ? Math.sqrt(variance) / mean : 0;

  // Amplification
  const amp = N > 0 ? metricTotalPushes / N : 0;

  const logN = N > 1 ? Math.log2(N) : 1;
  const delColor = delivery >= 0.99 ? 'green' : delivery >= 0.9 ? 'yellow' : 'red';
  const convColor = convRound < 0 ? 'red' : convRound <= 2 * logN ? 'green' : convRound <= 3 * logN ? 'yellow' : 'red';
  const redColor = redundancy < 0.2 ? 'green' : redundancy < 0.4 ? 'yellow' : 'red';
  const cvColor = cv < 0.3 ? 'green' : cv < 0.6 ? 'yellow' : 'red';

  grid.innerHTML =
    badge(delColor, (delivery * 100).toFixed(1) + '%', 'Delivery') +
    badge(convColor, convRound < 0 ? 'Never' : 'Round ' + convRound, 'Convergence') +
    badge(redColor, (redundancy * 100).toFixed(1) + '%', 'Redundancy') +
    badge(cvColor, 'CV ' + cv.toFixed(2), 'Load Balance') +
    badge('', metricTotalPushes.toLocaleString(), 'Total Pushes') +
    badge('', amp.toFixed(2) + '\u00d7', 'Amplification');
}

function badge(color, val, label) {
  return '<div class="badge ' + color + '"><div class="badge-val">' + val + '</div><div class="badge-lbl">' + label + '</div></div>';
}

function drawConvergenceChart() {
  const cv = document.getElementById('convCanvas');
  if (!snapRounds.length || totalKeys === 0) {
    cv.style.display = 'none';
    const sec = document.getElementById('convSection');
    if (!sec.querySelector('.chart-empty')) { const d = document.createElement('div'); d.className = 'chart-empty'; d.textContent = 'No snapshot data'; sec.appendChild(d); }
    return;
  }
  cv.style.display = 'block';
  const sec = document.getElementById('convSection');
  const emp = sec.querySelector('.chart-empty');
  if (emp) emp.remove();

  const dpr = devicePixelRatio;
  const w = cv.parentElement.clientWidth, h = 180;
  cv.width = w * dpr; cv.height = h * dpr;
  cv.style.width = w + 'px'; cv.style.height = h + 'px';
  const c = cv.getContext('2d');
  c.setTransform(dpr, 0, 0, dpr, 0, 0);

  const pad = { l: 45, r: 12, t: 12, b: 28 };
  const cw = w - pad.l - pad.r, ch = h - pad.t - pad.b;
  const maxRound = snapRounds[snapRounds.length - 1] || 1;

  // Background
  c.fillStyle = '#1a1d2e'; c.fillRect(0, 0, w, h);

  // Grid
  c.strokeStyle = '#2a2d3a'; c.lineWidth = 1;
  for (let pct = 0; pct <= 100; pct += 25) {
    const y = pad.t + ch * (1 - pct / 100);
    c.beginPath(); c.moveTo(pad.l, y); c.lineTo(pad.l + cw, y); c.stroke();
  }

  // Compute data points
  const pts = [];
  for (let r = 0; r < snapRounds.length; r++) {
    let full = 0;
    for (let i = 0; i < N; i++) if (snapEntries[r][i] >= totalKeys) full++;
    pts.push({ round: snapRounds[r], pct: N > 0 ? full / N * 100 : 0 });
  }

  // Draw fill
  c.beginPath();
  c.moveTo(pad.l, pad.t + ch);
  for (const p of pts) {
    const x = pad.l + (p.round / maxRound) * cw;
    const y = pad.t + ch * (1 - p.pct / 100);
    c.lineTo(x, y);
  }
  c.lineTo(pad.l + (pts[pts.length-1].round / maxRound) * cw, pad.t + ch);
  c.closePath();
  c.fillStyle = 'rgba(99,102,241,0.2)'; c.fill();

  // Draw line
  c.beginPath();
  for (let i = 0; i < pts.length; i++) {
    const x = pad.l + (pts[i].round / maxRound) * cw;
    const y = pad.t + ch * (1 - pts[i].pct / 100);
    i === 0 ? c.moveTo(x, y) : c.lineTo(x, y);
  }
  c.strokeStyle = '#6366f1'; c.lineWidth = 2; c.stroke();

  // Current replay round marker
  let curRound = 0;
  if (replayCursor > 0 && replayCursor <= allEvents.length) curRound = allEvents[replayCursor - 1].tick;
  const mx = pad.l + (curRound / maxRound) * cw;
  c.setLineDash([4, 3]); c.strokeStyle = '#f59e0b'; c.lineWidth = 1;
  c.beginPath(); c.moveTo(mx, pad.t); c.lineTo(mx, pad.t + ch); c.stroke();
  c.setLineDash([]);

  // Axes labels
  c.fillStyle = '#6b7280'; c.font = '10px system-ui,sans-serif';
  c.textAlign = 'right';
  for (let pct = 0; pct <= 100; pct += 25) {
    c.fillText(pct + '%', pad.l - 4, pad.t + ch * (1 - pct / 100) + 3);
  }
  c.textAlign = 'center';
  const step = Math.max(1, Math.ceil(maxRound / 8));
  for (let r = 0; r <= maxRound; r += step) {
    c.fillText(r, pad.l + (r / maxRound) * cw, h - 6);
  }
}

function drawHeatmap() {
  const cv = document.getElementById('heatCanvas');
  const tooltip = document.getElementById('heatTooltip');
  if (!snapRounds.length || totalKeys === 0) {
    cv.style.display = 'none';
    const sec = document.getElementById('heatSection');
    if (!sec.querySelector('.chart-empty')) { const d = document.createElement('div'); d.className = 'chart-empty'; d.textContent = 'No snapshot data'; sec.appendChild(d); }
    return;
  }
  cv.style.display = 'block';
  const sec = document.getElementById('heatSection');
  const emp = sec.querySelector('.chart-empty');
  if (emp) emp.remove();

  const dpr = devicePixelRatio;
  const w = cv.parentElement.clientWidth;
  const padL = 60, padR = 8, padT = 4, padB = 24;
  const cols = snapRounds.length, rows = N;
  const cellW = Math.max(1, Math.floor((w - padL - padR) / Math.max(cols, 1)));
  const cellH = N <= 50 ? 14 : Math.max(1, Math.min(4, Math.floor(220 / N)));
  const h = padT + rows * cellH + padB;
  cv.width = w * dpr; cv.height = h * dpr;
  cv.style.width = w + 'px'; cv.style.height = h + 'px';
  const c = cv.getContext('2d');
  c.setTransform(dpr, 0, 0, dpr, 0, 0);
  c.fillStyle = '#1a1d2e'; c.fillRect(0, 0, w, h);

  // Sort nodes by name
  const sortedIdx = Array.from({length: N}, (_, i) => i);
  sortedIdx.sort((a, b) => nodeNames[a].localeCompare(nodeNames[b]));

  // Draw cells
  for (let ri = 0; ri < rows; ri++) {
    const ni = sortedIdx[ri];
    for (let ci = 0; ci < cols; ci++) {
      const frac = totalKeys > 0 ? (snapEntries[ci][ni] || 0) / totalKeys : 0;
      const g = Math.round(frac * 200);
      c.fillStyle = 'rgb(' + (255 - g) + ',' + (255 - Math.round(frac * 55)) + ',' + (255 - g) + ')';
      if (frac > 0) c.fillStyle = 'rgb(' + Math.round(30 + (1-frac)*225) + ',' + Math.round(80 + (1-frac)*175) + ',' + Math.round(30 + (1-frac)*225) + ')';
      else c.fillStyle = '#2a2d3a';
      c.fillRect(padL + ci * cellW, padT + ri * cellH, cellW - (cellW > 2 ? 1 : 0), cellH - (cellH > 2 ? 1 : 0));
    }
  }

  // Node labels (only if space)
  if (cellH >= 10) {
    c.fillStyle = '#9ca3af'; c.font = '9px system-ui,sans-serif'; c.textAlign = 'right';
    for (let ri = 0; ri < rows; ri++) {
      c.fillText(nodeNames[sortedIdx[ri]], padL - 3, padT + ri * cellH + cellH - 2);
    }
  }

  // Round labels
  c.fillStyle = '#6b7280'; c.font = '9px system-ui,sans-serif'; c.textAlign = 'center';
  const labelStep = Math.max(1, Math.ceil(cols / 10));
  for (let ci = 0; ci < cols; ci += labelStep) {
    c.fillText(snapRounds[ci], padL + ci * cellW + cellW / 2, h - 6);
  }

  // Current round marker
  let curRound = 0;
  if (replayCursor > 0 && replayCursor <= allEvents.length) curRound = allEvents[replayCursor - 1].tick;
  let markerCol = 0;
  for (let ci = 0; ci < cols; ci++) { if (snapRounds[ci] <= curRound) markerCol = ci; }
  const mx = padL + markerCol * cellW + cellW / 2;
  c.setLineDash([3, 2]); c.strokeStyle = '#f59e0b'; c.lineWidth = 1;
  c.beginPath(); c.moveTo(mx, padT); c.lineTo(mx, padT + rows * cellH); c.stroke();
  c.setLineDash([]);

  // Hover tooltip
  cv.onmousemove = (e) => {
    const rect = cv.getBoundingClientRect();
    const ex = e.clientX - rect.left, ey = e.clientY - rect.top;
    const col = Math.floor((ex - padL) / cellW);
    const row = Math.floor((ey - padT) / cellH);
    if (col >= 0 && col < cols && row >= 0 && row < rows) {
      const ni = sortedIdx[row];
      const entries = snapEntries[col][ni] || 0;
      const pct = totalKeys > 0 ? Math.round(entries / totalKeys * 100) : 0;
      tooltip.textContent = nodeNames[ni] + ' at round ' + snapRounds[col] + ': ' + entries + '/' + totalKeys + ' keys (' + pct + '%)';
      tooltip.style.display = 'block';
      tooltip.style.left = (ex + 12) + 'px'; tooltip.style.top = (ey - 20) + 'px';
    } else { tooltip.style.display = 'none'; }
  };
  cv.onmouseleave = () => { tooltip.style.display = 'none'; };
}

function drawLoadHistogram() {
  const cv = document.getElementById('loadCanvas');
  const dpr = devicePixelRatio;
  const w = cv.parentElement.clientWidth, h = 150;
  cv.width = w * dpr; cv.height = h * dpr;
  cv.style.width = w + 'px'; cv.style.height = h + 'px';
  const c = cv.getContext('2d');
  c.setTransform(dpr, 0, 0, dpr, 0, 0);
  c.fillStyle = '#1a1d2e'; c.fillRect(0, 0, w, h);

  if (N === 0) return;

  const pad = { l: 40, r: 8, t: 8, b: 24 };
  const cw = w - pad.l - pad.r, ch = h - pad.t - pad.b;

  // Get data up to current replay pos from cumulative
  let data;
  let curRound = 0;
  if (replayCursor > 0 && replayCursor <= allEvents.length) curRound = allEvents[replayCursor - 1].tick;

  // Find the closest round in cumulPushRecv
  let bestR = -1;
  for (let r = 0; r < snapRounds.length; r++) {
    if (snapRounds[r] <= curRound) bestR = r;
  }

  if (bestR >= 0 && cumulPushRecv.length > bestR) {
    data = cumulPushRecv[bestR];
  } else {
    data = metricPushesRecv; // fallback: total
  }

  // Compute stats
  let maxVal = 0, mean = 0;
  for (let i = 0; i < N; i++) { if (data[i] > maxVal) maxVal = data[i]; mean += data[i]; }
  mean /= N;
  if (maxVal === 0) maxVal = 1;
  let std = 0;
  for (let i = 0; i < N; i++) { const d = data[i] - mean; std += d * d; }
  std = Math.sqrt(std / N);

  // Bin for large N
  const useBins = N > 100;
  let barData, barCount;
  if (useBins) {
    barCount = Math.min(50, N);
    barData = new Float64Array(barCount);
    const binCounts = new Int32Array(barCount);
    for (let i = 0; i < N; i++) {
      const bin = Math.min(barCount - 1, Math.floor(i / N * barCount));
      barData[bin] += data[i]; binCounts[bin]++;
    }
    for (let i = 0; i < barCount; i++) if (binCounts[i] > 0) barData[i] /= binCounts[i];
    // Recompute max
    maxVal = 0;
    for (let i = 0; i < barCount; i++) if (barData[i] > maxVal) maxVal = barData[i];
    if (maxVal === 0) maxVal = 1;
  } else {
    barCount = N;
    barData = data;
  }

  const barW = Math.max(1, cw / barCount - (barCount < 50 ? 1 : 0));

  for (let i = 0; i < barCount; i++) {
    const val = barData[i];
    const barH = (val / maxVal) * ch;
    const dev = std > 0 ? Math.abs(val - mean) / std : 0;
    // Color by deviation: green near mean, yellow moderate, red outlier
    let r, g, b;
    if (dev < 1) { r = 34; g = 197; b = 94; }
    else if (dev < 2) { r = 245; g = 158; b = 11; }
    else { r = 239; g = 68; b = 68; }
    c.fillStyle = 'rgb(' + r + ',' + g + ',' + b + ')';
    c.fillRect(pad.l + i * (cw / barCount), pad.t + ch - barH, barW, barH);
  }

  // Mean line
  const meanY = pad.t + ch - (mean / maxVal) * ch;
  c.setLineDash([4, 3]); c.strokeStyle = '#e0e0e0'; c.lineWidth = 1;
  c.beginPath(); c.moveTo(pad.l, meanY); c.lineTo(pad.l + cw, meanY); c.stroke();
  c.setLineDash([]);

  // Labels
  c.fillStyle = '#6b7280'; c.font = '10px system-ui,sans-serif';
  c.textAlign = 'right';
  c.fillText(maxVal, pad.l - 4, pad.t + 10);
  c.fillText('0', pad.l - 4, pad.t + ch);
  c.textAlign = 'left';
  c.fillText('mean: ' + mean.toFixed(1), pad.l + 4, meanY - 4);
  c.textAlign = 'center';
  c.fillText(useBins ? 'nodes (binned)' : 'node index', pad.l + cw / 2, h - 4);
}

// ── Drawing ──────────────────────────────────────────────────────
function drawGraph() {
  const r = canvas.parentElement.getBoundingClientRect();
  const w = r.width, h = r.height, dpr = devicePixelRatio, now = performance.now();

  ctx2d.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx2d.clearRect(0, 0, w, h);
  ctx2d.setTransform(dpr*vs, 0, 0, dpr*vs, dpr*vx, dpr*vy);

  // Viewport in world coordinates
  const v0x = -vx/vs, v0y = -vy/vs, v1x = (w-vx)/vs, v1y = (h-vy)/vs;

  // Adaptive sizing
  const baseR = Math.max(2, Math.min(8, 400/Math.sqrt(Math.max(N, 1))));
  const showStroke = N <= 200;

  // Semantic zoom levels
  const showHulls = vs < 0.5 && numCommunities > 1;
  const showNodes = vs >= 0.2;
  const showEdges = vs >= 0.2 && !(vs < 0.5 && N > 1000);
  const showLabels = vs * baseR > 6;
  const thickDetail = vs > 1.5;

  // Flash state
  const hasNodeFlash = flashNode >= 0 && now - flashNodeT < 400;
  const hasEdgeFlash = flashSrc >= 0 && now - flashEdgeT < 600;

  // ── Community hulls (zoomed out) ──
  if (showHulls) {
    for (let c = 0; c < numCommunities; c++) {
      const hull = communityHulls[c];
      if (!hull || hull.length < 2) continue;
      const color = communityColors[c % communityColors.length];
      // Expand hull slightly for padding
      let cx = 0, cy = 0;
      for (const p of hull) { cx += p[0]; cy += p[1]; }
      cx /= hull.length; cy /= hull.length;
      const pad = 20 / vs;

      ctx2d.beginPath();
      for (let i = 0; i < hull.length; i++) {
        const dx = hull[i][0] - cx, dy = hull[i][1] - cy;
        const d = Math.sqrt(dx*dx + dy*dy) || 1;
        const px = hull[i][0] + dx/d * pad, py = hull[i][1] + dy/d * pad;
        i === 0 ? ctx2d.moveTo(px, py) : ctx2d.lineTo(px, py);
      }
      ctx2d.closePath();
      ctx2d.fillStyle = color + '18'; ctx2d.fill();
      ctx2d.strokeStyle = color + '60'; ctx2d.lineWidth = 2/vs; ctx2d.stroke();

      // Label
      if (vs < 0.5) {
        let count = 0;
        for (let i = 0; i < N; i++) if (community[i] === c) count++;
        ctx2d.fillStyle = color;
        ctx2d.font = Math.max(12, 16/vs) + 'px system-ui,sans-serif';
        ctx2d.textAlign = 'center';
        ctx2d.fillText('Community ' + c + ' (' + count + ' nodes)', cx, cy);
      }
    }
  }

  // ── Edges ──
  if (showEdges && !(vs < 0.15 && visEdges > 10000)) {
    const baseAlpha = Math.min(0.25, 40/Math.sqrt(Math.max(visEdges, 1)));
    let flashEdgeI = -1;

    ctx2d.beginPath();
    ctx2d.strokeStyle = 'rgba(99,102,241,' + baseAlpha + ')';
    ctx2d.lineWidth = (thickDetail ? 1.5 : 1)/vs;
    let batched = 0;

    for (let i = 0; i < visEdges; i++) {
      const si = edgeSrc[i], di = edgeDst[i];
      const sx = posX[si], sy = posY[si], dx = posX[di], dy = posY[di];
      if ((sx < v0x && dx < v0x) || (sx > v1x && dx > v1x) ||
          (sy < v0y && dy < v0y) || (sy > v1y && dy > v1y)) continue;
      if (hasEdgeFlash && si === flashSrc && di === flashDst) { flashEdgeI = i; continue; }
      ctx2d.moveTo(sx, sy); ctx2d.lineTo(dx, dy); batched++;
    }
    if (batched > 0) ctx2d.stroke();

    if (flashEdgeI >= 0) {
      const si = edgeSrc[flashEdgeI], di = edgeDst[flashEdgeI];
      const alpha = baseAlpha + (1-baseAlpha) * (1-(now-flashEdgeT)/600);
      ctx2d.beginPath();
      ctx2d.moveTo(posX[si], posY[si]); ctx2d.lineTo(posX[di], posY[di]);
      ctx2d.strokeStyle = 'rgba(99,102,241,' + alpha + ')';
      ctx2d.lineWidth = 2.5/vs;
      ctx2d.stroke();
    }
  }

  // ── Nodes (community-colored) ──
  if (showNodes) {
    const useCommunityColor = numCommunities > 1;
    let flashNodeI = -1;

    if (useCommunityColor) {
      // Batch by community color
      for (let c = 0; c < numCommunities; c++) {
        ctx2d.beginPath();
        for (let i = 0; i < N; i++) {
          if (community[i] !== c) continue;
          const px = posX[i], py = posY[i];
          if (px+baseR < v0x || px-baseR > v1x || py+baseR < v0y || py-baseR > v1y) continue;
          if (hasNodeFlash && i === flashNode) { flashNodeI = i; continue; }
          ctx2d.moveTo(px+baseR, py); ctx2d.arc(px, py, baseR, 0, 2*Math.PI);
        }
        ctx2d.fillStyle = communityColors[c % communityColors.length]; ctx2d.fill();
        if (showStroke) { ctx2d.strokeStyle = '#1a1d2e'; ctx2d.lineWidth = 1/vs; ctx2d.stroke(); }
      }
    } else {
      ctx2d.beginPath();
      for (let i = 0; i < N; i++) {
        const px = posX[i], py = posY[i];
        if (px+baseR < v0x || px-baseR > v1x || py+baseR < v0y || py-baseR > v1y) continue;
        if (hasNodeFlash && i === flashNode) { flashNodeI = i; continue; }
        ctx2d.moveTo(px+baseR, py); ctx2d.arc(px, py, baseR, 0, 2*Math.PI);
      }
      ctx2d.fillStyle = '#6366f1'; ctx2d.fill();
      if (showStroke) { ctx2d.strokeStyle = '#4f46e5'; ctx2d.lineWidth = 1.5/vs; ctx2d.stroke(); }
    }

    if (flashNodeI >= 0) {
      const t = 1-(now-flashNodeT)/400, rad = baseR+6*t;
      ctx2d.beginPath();
      ctx2d.arc(posX[flashNodeI], posY[flashNodeI], rad, 0, 2*Math.PI);
      ctx2d.fillStyle = '#818cf8'; ctx2d.fill();
      if (showStroke) { ctx2d.strokeStyle = '#4f46e5'; ctx2d.lineWidth = 1.5/vs; ctx2d.stroke(); }
    }
  }

  // ── Labels ──
  if (showLabels) {
    ctx2d.fillStyle = '#c7d2fe';
    ctx2d.font = Math.max(8, Math.min(thickDetail ? 13 : 11, (thickDetail ? 13 : 11)/vs)) + 'px system-ui,sans-serif';
    ctx2d.textAlign = 'center';
    for (let i = 0; i < N; i++) {
      const px = posX[i], py = posY[i];
      if (px < v0x || px > v1x || py < v0y || py > v1y) continue;
      ctx2d.fillText(nodeNames[i], px, py + baseR + 14/vs);
    }
  }

  if (hasNodeFlash || hasEdgeFlash) requestAnimationFrame(drawGraph);
}

// ── Edge visibility (binary search, zero allocation) ─────────────
function updateVisibleEdges(pos) {
  let lo = 0, hi = totalEdges;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (edgeAppear[mid] < pos) lo = mid + 1; else hi = mid;
  }
  visEdges = lo;
}

// ── UI helpers ───────────────────────────────────────────────────
function updateStats(nn, ne, nm, cr, tr) {
  document.getElementById('statNodes').textContent = nn;
  document.getElementById('statEdges').textContent = ne;
  document.getElementById('statMessages').textContent = nm;
  document.getElementById('statRound').textContent = cr + '/' + tr;
}

function formatDetail(kind, detail) {
  if (!detail) return '';
  switch (kind) {
    case 'GossipRoundStarted': return '&rarr; ' + (detail.target_name || detail.target);
    case 'PushReceived': return '&larr; ' + (detail.from_name || detail.from) + ' (' + detail.keys_updated + ' keys)';
    case 'LocalSet': return 'key=' + detail.key;
    case 'PeerAdded': return '+ ' + (detail.peer_name || detail.peer);
    case 'PeerRemoved': return '- ' + (detail.peer_name || detail.peer);
    case 'QueryReceived': return 'key=' + detail.key;
    case 'StateSnapshot': return detail.entries + ' entries, ' + detail.peer_count + ' peers';
    default: return JSON.stringify(detail);
  }
}

function makeRow(ev) {
  const tr = document.createElement('tr');
  if (ev.kind === 'GossipRoundStarted') tr.className = 'highlight-push';
  else if (ev.kind === 'LocalSet') tr.className = 'highlight-set';
  tr.innerHTML = '<td>'+ev.seq+'</td><td>'+ev.tick+'</td><td>'+(ev.thread||'-')+'</td><td>'+ev.node+'</td><td>'+ev.kind+'</td><td>'+formatDetail(ev.kind, ev.detail)+'</td>';
  return tr;
}

function fmtWorker(ev) {
  let t = ev.node + ': ' + ev.kind;
  if (ev.kind === 'GossipRoundStarted' && ev.detail) t += ' -> ' + (ev.detail.target_name || ev.detail.target);
  if (ev.kind === 'PushReceived' && ev.detail) t += ' <- ' + (ev.detail.from_name || ev.detail.from);
  return t;
}

function rebuildWorkerColumns() {
  const c = document.getElementById('workerColumns');
  c.innerHTML = '';
  for (const t of Object.keys(workerLogs).sort()) {
    const col = document.createElement('div');
    col.className = 'worker-col'; col.id = 'worker-' + t;
    col.innerHTML = '<div class="worker-col-header">' + t + '</div>';
    c.appendChild(col);
  }
}

function updateWorkerColumn(thread) {
  const col = document.getElementById('worker-' + thread);
  if (!col) return;
  let h = '<div class="worker-col-header">' + thread + '</div>';
  for (const e of workerLogs[thread]) h += '<div class="worker-entry">' + e + '</div>';
  col.innerHTML = h;
  col.scrollTop = col.scrollHeight;
}

// ── Incremental table/worker updates ─────────────────────────────
function updateEventTable(pos) {
  const tbody = document.getElementById('eventTableBody');
  const incr = pos > lastTablePos && pos - lastTablePos <= 100 && lastTablePos >= 0;
  if (!incr) tbody.textContent = '';
  const frag = document.createDocumentFragment();
  const start = incr ? lastTablePos : Math.max(0, pos - MAX_TABLE_ROWS);
  for (let i = start; i < pos && i < allEvents.length; i++) frag.appendChild(makeRow(allEvents[i]));
  tbody.appendChild(frag);
  if (incr) while (tbody.childNodes.length > MAX_TABLE_ROWS) tbody.removeChild(tbody.firstChild);
  lastTablePos = pos;
  document.getElementById('eventTableWrap').scrollTop = 1e9;
}

function updateWorkerPanel(pos) {
  const incr = pos > lastWorkerPos && pos - lastWorkerPos <= 100 && lastWorkerPos >= 0;
  const start = incr ? lastWorkerPos : Math.max(0, pos - 200);
  if (!incr) workerLogs = {};
  let needCols = !incr;
  for (let i = start; i < pos && i < allEvents.length; i++) {
    const ev = allEvents[i];
    if (!ev.thread) continue;
    if (!workerLogs[ev.thread]) { workerLogs[ev.thread] = []; needCols = true; }
    workerLogs[ev.thread].push(fmtWorker(ev));
    if (workerLogs[ev.thread].length > 50) workerLogs[ev.thread].shift();
  }
  if (needCols) rebuildWorkerColumns();
  for (const t of Object.keys(workerLogs)) updateWorkerColumn(t);
  lastWorkerPos = pos;
}

// ── Replay engine ────────────────────────────────────────────────
let replayRafId = 0, playTimer = null;

function stopPlayback() {
  if (playTimer !== null) {
    clearInterval(playTimer); playTimer = null;
    document.getElementById('btnPlay').innerHTML = '&#9654;';
    document.getElementById('btnPlay').title = 'Play';
  }
}

function startPlayback() {
  stopPlayback();
  if (replayCursor >= allEvents.length) return;
  const speed = parseInt(document.getElementById('speedInput').value) || 100;
  document.getElementById('btnPlay').innerHTML = '&#9208;';
  document.getElementById('btnPlay').title = 'Pause';
  playTimer = setInterval(() => {
    if (replayCursor >= allEvents.length) { stopPlayback(); return; }
    replayTo(replayCursor + 1);
  }, speed);
}

function replayTo(pos) {
  replayCursor = pos;
  if (replayRafId) return;
  replayRafId = requestAnimationFrame(() => { replayRafId = 0; replayToImpl(replayCursor); });
}

function replayToImpl(pos) {
  flashNode = -1; flashSrc = -1; flashDst = -1;

  updateVisibleEdges(pos);
  updateEventTable(pos);
  updateWorkerPanel(pos);

  // Flash last event
  if (pos > 0 && pos <= allEvents.length) {
    const ev = allEvents[pos - 1];
    flashNode = nodeIdx.get(ev.node) ?? -1;
    flashNodeT = performance.now();
    if (ev.kind === 'GossipRoundStarted' && ev.detail) {
      const t = ev.detail.target_name || ev.detail.target;
      if (t) { flashSrc = nodeIdx.get(ev.node) ?? -1; flashDst = nodeIdx.get(t) ?? -1; flashEdgeT = performance.now(); }
    }
    if (ev.kind === 'PushReceived' && ev.detail) {
      const f = ev.detail.from_name || ev.detail.from;
      if (f) { flashSrc = nodeIdx.get(f) ?? -1; flashDst = nodeIdx.get(ev.node) ?? -1; flashEdgeT = performance.now(); }
    }
  }

  document.getElementById('replaySlider').value = pos;
  document.getElementById('replayPos').textContent = replayCursor + '/' + allEvents.length;
  drawGraph();
  // Update analytics charts if visible
  if (document.getElementById('tab-analytics').classList.contains('active')) {
    drawConvergenceChart();
    drawHeatmap();
    drawLoadHistogram();
  }
}

// ── Trace loading ────────────────────────────────────────────────
function resetState() {
  stopPlayback();
  layoutAbortFlag = true;
  nodeNames = []; nodeIdx.clear(); N = 0;
  posX = new Float64Array(0); posY = new Float64Array(0);
  totalEdges = 0; visEdges = 0;
  edgeSrc = new Int32Array(0); edgeDst = new Int32Array(0); edgeAppear = new Int32Array(0);
  flashNode = -1; flashSrc = -1; flashDst = -1;
  allEvents = []; replayCursor = 0; workerLogs = {};
  lastTablePos = -1; lastWorkerPos = -1;
  vx = 0; vy = 0; vs = 1;
  snapRounds = []; snapEntries = []; snapPeerCount = []; totalKeys = 0; numRounds = 0;
  metricPushesSent = new Int32Array(0); metricPushesRecv = new Int32Array(0);
  metricRedundant = 0; metricTotalPushes = 0; metricNoPeers = 0;
  cumulPushRecv = []; eventRoundMap = [];
  community = new Int32Array(0); numCommunities = 0; communityHulls = [];
  document.getElementById('eventTableBody').textContent = '';
  document.getElementById('workerColumns').innerHTML = '';
  document.getElementById('layoutProgress').style.display = 'none';
  document.getElementById('nodeDetail').style.display = 'none';
  document.getElementById('badgeGrid').innerHTML = '';
}

async function loadTrace(file) {
  const dot = document.getElementById('statusDot');
  const statusText = document.getElementById('statusText');
  dot.className = 'status-dot loading';
  statusText.textContent = 'Loading...';
  resetState();

  const resp = await fetch('/trace.json?file=' + encodeURIComponent(file));
  if (!resp.ok) { dot.className = 'status-dot error'; statusText.textContent = 'Failed to load trace'; return; }
  const trace = await resp.json();

  // Build node index
  nodeNames = trace.node_names;
  N = nodeNames.length;
  nodeIdx = new Map();
  for (let i = 0; i < N; i++) nodeIdx.set(nodeNames[i], i);
  posX = new Float64Array(N);
  posY = new Float64Array(N);

  // Build edges (initial topology)
  const edgeKeys = new Set();
  const tempEdges = []; // flat triples: [src, dst, appear, ...]
  for (const [a, b] of trace.topology_edges) {
    const si = nodeIdx.get(a), di = nodeIdx.get(b);
    if (si === undefined || di === undefined) continue;
    const key = si + ',' + di;
    if (!edgeKeys.has(key)) { edgeKeys.add(key); tempEdges.push(si, di, -1); }
  }

  // Build events + extract snapshots
  let seq = 0;
  allEvents = [];
  numRounds = trace.num_rounds || 0;

  // First pass: collect snapshots grouped by tick
  const snapByTick = new Map(); // tick -> Map(nodeIdx -> {entries, peer_count})
  for (const ev of trace.events) {
    const isObj = typeof ev.kind === 'object';
    if (isObj && 'StateSnapshot' in ev.kind) {
      const snap = ev.kind.StateSnapshot.snapshot || ev.kind.StateSnapshot;
      const ni = nodeIdx.get(ev.node_name);
      if (ni === undefined) continue;
      const entriesCount = snap.entries ? Object.keys(snap.entries).length : 0;
      const peerCount = snap.peer_count || 0;
      if (!snapByTick.has(ev.tick)) snapByTick.set(ev.tick, new Map());
      snapByTick.get(ev.tick).set(ni, { entries: entriesCount, peer_count: peerCount });
      if (entriesCount > totalKeys) totalKeys = entriesCount;
      continue;
    }
    if (!isObj && ev.kind === 'StateSnapshot') continue;
    let kind, detail;
    if (isObj) { for (kind in ev.kind) break; detail = ev.kind[kind] || null; }
    else { kind = ev.kind; detail = null; }
    allEvents.push({ seq: seq++, tick: ev.tick, node: ev.node_name, thread: ev.thread_name, kind, detail });
  }

  // Build snapshot arrays sorted by round
  snapRounds = Array.from(snapByTick.keys()).sort((a, b) => a - b);
  snapEntries = []; snapPeerCount = [];
  for (const tick of snapRounds) {
    const eArr = new Int32Array(N);
    const pArr = new Int32Array(N);
    const m = snapByTick.get(tick);
    for (const [ni, d] of m) { eArr[ni] = d.entries; pArr[ni] = d.peer_count; }
    snapEntries.push(eArr);
    snapPeerCount.push(pArr);
  }

  // Compute per-node metrics from events
  metricPushesSent = new Int32Array(N);
  metricPushesRecv = new Int32Array(N);
  metricRedundant = 0; metricTotalPushes = 0; metricNoPeers = 0;

  // Also build cumulative push-recv per round
  const roundSet = new Set(snapRounds);
  cumulPushRecv = [];
  let runningRecv = new Int32Array(N);

  for (let i = 0; i < allEvents.length; i++) {
    const ev = allEvents[i];
    const ni = nodeIdx.get(ev.node);
    if (ni === undefined) continue;
    if (ev.kind === 'GossipRoundStarted') {
      metricPushesSent[ni]++;
      metricTotalPushes++;
    }
    if (ev.kind === 'PushReceived') {
      metricPushesRecv[ni]++;
      runningRecv[ni]++;
      if (ev.detail && ev.detail.keys_updated === 0) metricRedundant++;
    }
    if (ev.kind === 'GossipRoundNoPeers') { metricNoPeers++; }
  }

  // Build cumulative recv snapshots aligned to snap rounds
  // Re-scan to build per-round cumulative
  if (snapRounds.length > 0) {
    const cumRecv = new Int32Array(N);
    let sri = 0;
    for (let i = 0; i < allEvents.length && sri < snapRounds.length; i++) {
      const ev = allEvents[i];
      if (ev.kind === 'PushReceived') {
        const ni = nodeIdx.get(ev.node);
        if (ni !== undefined) cumRecv[ni]++;
      }
      // When we pass a snapshot round boundary, save
      while (sri < snapRounds.length && ev.tick >= snapRounds[sri]) {
        cumulPushRecv.push(new Int32Array(cumRecv));
        sri++;
      }
    }
    // Fill remaining
    while (sri < snapRounds.length) { cumulPushRecv.push(new Int32Array(cumRecv)); sri++; }
  }

  // Index PeerAdded edges
  for (let i = 0; i < allEvents.length; i++) {
    const ev = allEvents[i];
    if (ev.kind !== 'PeerAdded' || !ev.detail) continue;
    const peer = ev.detail.peer_name || ev.detail.peer;
    if (!peer) continue;
    const si = nodeIdx.get(ev.node), di = nodeIdx.get(peer);
    if (si === undefined || di === undefined) continue;
    const key = si + ',' + di;
    if (!edgeKeys.has(key)) { edgeKeys.add(key); tempEdges.push(si, di, i); }
  }

  // Sort by appearance and build typed arrays
  const nEdges = tempEdges.length / 3;
  const sortIdx = new Array(nEdges);
  for (let i = 0; i < nEdges; i++) sortIdx[i] = i;
  sortIdx.sort((a, b) => tempEdges[a*3+2] - tempEdges[b*3+2]);

  totalEdges = nEdges;
  edgeSrc = new Int32Array(nEdges);
  edgeDst = new Int32Array(nEdges);
  edgeAppear = new Int32Array(nEdges);
  for (let j = 0; j < nEdges; j++) {
    const i = sortIdx[j];
    edgeSrc[j] = tempEdges[i*3]; edgeDst[j] = tempEdges[i*3+1]; edgeAppear[j] = tempEdges[i*3+2];
  }

  updateVisibleEdges(0);

  dot.className = 'status-dot loading';
  statusText.textContent = 'Computing layout...';
  updateStats(N, totalEdges, allEvents.length, trace.num_rounds, trace.num_rounds);

  resizeCanvas();
  initPositions();
  const rect = canvas.parentElement.getBoundingClientRect();
  await runForceLayout(rect.width, rect.height);

  // Community detection + hulls
  detectCommunities();
  computeCommunityHulls();

  dot.className = 'status-dot ready';
  statusText.textContent = trace.name + ' (' + allEvents.length + ' events)';
  drawGraph();
  drawAllCharts();

  // Replay controls
  document.getElementById('replayControls').style.display = 'flex';
  const slider = document.getElementById('replaySlider');
  slider.max = allEvents.length; slider.value = 0;
  replayCursor = 0;
  document.getElementById('replayPos').textContent = '0/' + allEvents.length;

  document.getElementById('btnFirst').onclick = () => { stopPlayback(); replayTo(0); };
  document.getElementById('btnPrev').onclick = () => { stopPlayback(); replayTo(Math.max(0, replayCursor-1)); };
  document.getElementById('btnNext').onclick = () => { stopPlayback(); replayTo(Math.min(allEvents.length, replayCursor+1)); };
  document.getElementById('btnLast').onclick = () => { stopPlayback(); replayTo(allEvents.length); };
  slider.oninput = () => { stopPlayback(); replayTo(parseInt(slider.value)); };
  document.getElementById('btnPlay').onclick = () => { playTimer !== null ? stopPlayback() : startPlayback(); };
  document.getElementById('speedInput').onchange = () => { if (playTimer !== null) startPlayback(); };
}

// ── Boot ─────────────────────────────────────────────────────────
resizeCanvas();
(async function() {
  const select = document.getElementById('traceSelect');
  const dot = document.getElementById('statusDot');
  const statusText = document.getElementById('statusText');
  let traces;
  try { traces = await (await fetch('/traces')).json(); }
  catch { dot.className='status-dot error'; statusText.textContent='Failed to fetch trace list'; select.innerHTML='<option value="">Error</option>'; return; }
  if (!traces.length) { dot.className='status-dot error'; statusText.textContent='No traces found'; select.innerHTML='<option value="">No traces found</option>'; return; }
  select.innerHTML = '';
  traces.forEach(t => { const o = document.createElement('option'); o.value = t.file; o.textContent = t.name+' ('+t.nodes+' nodes, '+t.events+' events)'; select.appendChild(o); });
  select.disabled = false;
  select.onchange = () => { if (select.value) loadTrace(select.value); };
  loadTrace(traces[0].file);
})();
</script>
</body>
</html>
"##;
