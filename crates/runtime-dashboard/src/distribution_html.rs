pub const DISTRIBUTION_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Swactor Runtime – Distribution</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'Menlo', 'Consolas', 'Monaco', monospace; background: #0f1117; color: #e0e0e0; font-size: 13px; }

  .header {
    display: flex; align-items: center; justify-content: space-between;
    padding: 12px 20px; background: #161822; border-bottom: 1px solid #2a2d3e;
  }
  .header-left { display: flex; align-items: center; }
  .header h1 { font-size: 16px; font-weight: 600; color: #fff; }
  .status-dot {
    width: 10px; height: 10px; border-radius: 50%; background: #4caf50;
    display: inline-block; margin-left: 8px; vertical-align: middle;
  }
  .status-dot.disconnected { background: #f44336; }
  .status-dot.done { background: #ff9800; }

  .nav-links { display: flex; gap: 4px; margin-left: 20px; }
  .nav-link {
    color: #888; text-decoration: none; font-size: 12px;
    padding: 4px 10px; border-radius: 3px; transition: color 0.2s;
  }
  .nav-link:hover { color: #e0e0e0; }
  .nav-link.active { color: #fff; background: #2a2d3e; }

  .header-right { display: flex; align-items: center; gap: 12px; }

  .main {
    display: grid;
    grid-template-columns: 1fr 1fr;
    grid-template-rows: auto 1fr auto;
    height: calc(100vh - 48px);
  }

  .graph-panel {
    grid-row: 1 / 3; border-right: 1px solid #2a2d3e; position: relative;
    min-height: 0; overflow: hidden;
  }
  .graph-panel canvas { position: absolute; top: 0; left: 0; width: 100%; height: 100%; display: block; }

  .side-panel { display: flex; flex-direction: column; overflow: hidden; min-height: 0; }

  .stats-panel {
    flex-shrink: 0; padding: 12px 16px; border-bottom: 1px solid #2a2d3e; background: #161822;
  }
  .stats-panel h2 { font-size: 12px; color: #888; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 8px; }

  .stats-cards {
    display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px;
  }
  .stat-card {
    background: #1c1f2e; border-radius: 4px; padding: 10px; text-align: center;
  }
  .stat-card .value { font-size: 22px; font-weight: 700; color: #fff; }
  .stat-card .label { font-size: 10px; color: #888; text-transform: uppercase; margin-top: 2px; }

  .table-panel {
    flex: 1; min-height: 0; display: flex; flex-direction: column; overflow: hidden;
  }
  .table-panel h2 {
    font-size: 12px; color: #888; text-transform: uppercase; letter-spacing: 1px;
    padding: 10px 16px 6px; flex-shrink: 0;
  }
  .table-scroll {
    flex: 1; overflow-y: auto; padding: 0 16px 8px;
  }
  .table-scroll table { width: 100%; border-collapse: collapse; }
  .table-scroll th, .table-scroll td {
    padding: 3px 8px; text-align: left; border-bottom: 1px solid #1c1f2e; font-size: 11px;
    white-space: nowrap;
  }
  .table-scroll th { color: #888; font-weight: 500; position: sticky; top: 0; background: #161822; }

  .state-alive { color: #4caf50; }
  .state-suspect { color: #ff9800; }
  .state-dead { color: #f44336; }

  .bottom-panel {
    grid-column: 1 / -1; border-top: 1px solid #2a2d3e; background: #161822;
    display: flex; gap: 12px; padding: 12px 16px; height: 200px;
  }
  .bottom-section { flex: 1; display: flex; flex-direction: column; min-width: 0; }
  .bottom-section h2 {
    font-size: 12px; color: #888; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 6px;
  }
  .bottom-section canvas { flex: 1; width: 100%; }
  .bottom-section .scroll-wrap {
    flex: 1; overflow-y: auto; font-size: 11px;
  }

  .ego-hint {
    position: absolute; bottom: 12px; left: 12px; font-size: 10px;
    color: #555; pointer-events: none;
  }

  .node-id-label {
    position: absolute; top: 12px; left: 12px; font-size: 11px;
    color: #888; max-width: 50%; overflow: hidden; text-overflow: ellipsis;
    white-space: nowrap;
  }

  ::-webkit-scrollbar { width: 6px; }
  ::-webkit-scrollbar-track { background: #0f1117; }
  ::-webkit-scrollbar-thumb { background: #2a2d3e; border-radius: 3px; }
</style>
</head>
<body>
<div class="header">
  <div class="header-left">
    <h1>
      Swactor Runtime Dashboard
      <span id="statusDot" class="status-dot disconnected"></span>
    </h1>
    <nav class="nav-links">
      <a href="/" class="nav-link">Overview</a>
      <a href="/actors" class="nav-link">Actors</a>
      <a href="/distribution" class="nav-link active">Distribution</a>
      <a href="/datastore" class="nav-link">Datastore</a>
    </nav>
  </div>
  <div class="header-right">
    <span id="nodeLabel" style="color:#888;font-size:12px;">Waiting for data...</span>
  </div>
</div>

<div class="main">
  <!-- Left: Graph -->
  <div class="graph-panel">
    <canvas id="graphCanvas"></canvas>
    <div class="ego-hint" id="egoHint">Click a node for ego-centric view. Double-click to reset.</div>
    <div class="node-id-label" id="selfLabel"></div>
  </div>

  <!-- Right: Stats + Tables -->
  <div class="side-panel">
    <div class="stats-panel">
      <h2>Distribution Stats</h2>
      <div class="stats-cards">
        <div class="stat-card"><div class="value" id="statMembers">0</div><div class="label">Members</div></div>
        <div class="stat-card"><div class="value" id="statAlive">0</div><div class="label">Alive</div></div>
        <div class="stat-card"><div class="value" id="statSuspect">0</div><div class="label">Suspect</div></div>
        <div class="stat-card"><div class="value" id="statDead">0</div><div class="label">Dead</div></div>
        <div class="stat-card"><div class="value" id="statCache">0</div><div class="label">Cache</div></div>
        <div class="stat-card"><div class="value" id="statRT">0</div><div class="label">RT Size</div></div>
        <div class="stat-card"><div class="value" id="statDir">0</div><div class="label">Directory</div></div>
        <div class="stat-card"><div class="value" id="statRepair">0</div><div class="label">Repair Q</div></div>
        <div class="stat-card"><div class="value" id="statProbes">0</div><div class="label">Probes</div></div>
      </div>
    </div>

    <div class="table-panel">
      <h2>Members <span id="memberCount" style="color:#555;font-weight:400;"></span></h2>
      <div class="table-scroll">
        <table>
          <thead><tr><th>State</th><th>Node ID</th><th>Address</th><th>Inc</th></tr></thead>
          <tbody id="membersBody"></tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- Bottom: Cache, Gossip Pairs, Routing Histogram -->
  <div class="bottom-panel">
    <div class="bottom-section">
      <h2>LRU Cache <span id="cacheCount" style="color:#555;font-weight:400;"></span></h2>
      <div class="scroll-wrap">
        <table style="width:100%;border-collapse:collapse;">
          <thead><tr><th style="color:#888;font-weight:500;font-size:11px;">Actor</th><th style="color:#888;font-weight:500;font-size:11px;">Node</th></tr></thead>
          <tbody id="cacheBody"></tbody>
        </table>
      </div>
    </div>
    <div class="bottom-section">
      <h2>Recent Probes <span id="probeCount" style="color:#555;font-weight:400;"></span></h2>
      <div class="scroll-wrap">
        <table style="width:100%;border-collapse:collapse;">
          <thead><tr>
            <th style="color:#888;font-weight:500;font-size:11px;">State</th>
            <th style="color:#888;font-weight:500;font-size:11px;">Node</th>
            <th style="color:#888;font-weight:500;font-size:11px;">Address</th>
          </tr></thead>
          <tbody id="probesBody"></tbody>
        </table>
      </div>
    </div>
    <div class="bottom-section">
      <h2>Routing Buckets</h2>
      <canvas id="bucketChart"></canvas>
    </div>
  </div>
</div>

<script>
(function() {
  var DASHBOARD_MODE = '__DASHBOARD_MODE__';
  var dot = document.getElementById('statusDot');

  // ── State ───────────────────────────────────────────────────────
  var data = null;        // latest DistributionNodeSnapshot
  var selfNodeId = '';    // this node's hex id
  var focusIdx = -1;      // ego-centric focus (-1 = none, 0 = self)

  // Graph state
  var N = 0;
  var posX = new Float64Array(0);
  var posY = new Float64Array(0);
  var nodeIds = [];       // hex strings
  var nodeStates = [];    // 'alive' | 'suspect' | 'dead' | 'self'
  var nodeAddrs = [];
  var layoutDone = false;
  var layoutIter = 0;

  // View transform
  var vx = 0, vy = 0, vs = 1;
  var isDragging = false, dragX = 0, dragY = 0, dragVx = 0, dragVy = 0;
  var clickStartX = 0, clickStartY = 0;

  // Routing neighbor set (for ego highlight)
  var routingSet = {};

  // ── Quadtree (Barnes-Hut) ──────────────────────────────────────
  var QF = 11;
  var qt = new Float64Array(256 * QF);
  var qtN = 0;
  var _fx = 0, _fy = 0;

  function qtAlloc(ox, oy, sz) {
    if (qtN * QF >= qt.length) {
      var nq = new Float64Array(Math.max(qt.length * 2, 256 * QF));
      nq.set(qt); qt = nq;
    }
    var i = qtN++, o = i * QF;
    qt[o]=ox; qt[o+1]=oy; qt[o+2]=sz;
    qt[o+3]=0; qt[o+4]=0; qt[o+5]=0;
    qt[o+6]=-1; qt[o+7]=-1; qt[o+8]=-1; qt[o+9]=-1;
    qt[o+10]=-1;
    return i;
  }

  function qtBuild() {
    var x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
    for (var i = 0; i < N; i++) {
      if (posX[i] < x0) x0 = posX[i]; if (posY[i] < y0) y0 = posY[i];
      if (posX[i] > x1) x1 = posX[i]; if (posY[i] > y1) y1 = posY[i];
    }
    var sz = Math.max(x1 - x0, y1 - y0, 1) + 2;
    qtN = 0;
    qtAlloc(x0 - 1, y0 - 1, sz);
    for (var i = 0; i < N; i++) qtIns(0, i, posX[i], posY[i]);
  }

  function qtIns(ni, bi, bx, by) {
    var o = ni * QF;
    if (qt[o+5] === 0) { qt[o+3] = bx; qt[o+4] = by; qt[o+5] = 1; qt[o+10] = bi; return; }
    if (qt[o+10] >= 0) {
      if (qt[o+2] < 0.001) { qt[o+5]++; return; }
      var eb = qt[o+10], ex = qt[o+3], ey = qt[o+4];
      qt[o+10] = -1;
      qtInsChild(ni, eb, ex, ey);
    }
    var m = qt[o+5];
    qt[o+3] = (qt[o+3]*m + bx) / (m+1);
    qt[o+4] = (qt[o+4]*m + by) / (m+1);
    qt[o+5] = m + 1;
    qtInsChild(ni, bi, bx, by);
  }

  function qtInsChild(ni, bi, bx, by) {
    var o = ni * QF, hs = qt[o+2] / 2;
    var mx = qt[o] + hs, my = qt[o+1] + hs;
    var qx = bx < mx ? 0 : 1, qy = by < my ? 0 : 1;
    var ci = o + 6 + qy * 2 + qx;
    if (qt[ci] < 0) qt[ci] = qtAlloc(qx ? mx : qt[o], qy ? my : qt[o+1], hs);
    qtIns(qt[ci], bi, bx, by);
  }

  function qtCalc(ni, px, py, k2, th2) {
    if (ni < 0) return;
    var o = ni * QF;
    if (qt[o+5] === 0) return;
    var dx = qt[o+3] - px, dy = qt[o+4] - py;
    var d2 = dx*dx + dy*dy;
    if (d2 < 0.0001) d2 = 0.0001;
    if (qt[o+10] >= 0 || qt[o+2]*qt[o+2]/d2 < th2) {
      var d = Math.sqrt(d2), f = -(k2 * qt[o+5]) / d2;
      _fx += (dx/d)*f; _fy += (dy/d)*f;
      return;
    }
    for (var c = 6; c < 10; c++) if (qt[o+c] >= 0) qtCalc(qt[o+c], px, py, k2, th2);
  }

  // ── Canvas setup ───────────────────────────────────────────────
  var canvas = document.getElementById('graphCanvas');
  var ctx = canvas.getContext('2d');

  function resizeCanvas() {
    var r = canvas.parentElement.getBoundingClientRect();
    canvas.width = r.width * devicePixelRatio;
    canvas.height = r.height * devicePixelRatio;
    canvas.style.width = r.width + 'px';
    canvas.style.height = r.height + 'px';
  }
  window.addEventListener('resize', function() { resizeCanvas(); drawGraph(); });

  // ── Zoom & pan ─────────────────────────────────────────────────
  canvas.addEventListener('wheel', function(e) {
    e.preventDefault();
    var r = canvas.getBoundingClientRect();
    var mx = e.clientX - r.left, my = e.clientY - r.top;
    var f = e.deltaY < 0 ? 1.1 : 1 / 1.1;
    var ns = Math.max(0.05, Math.min(5, vs * f));
    var ratio = ns / vs;
    vx = mx - (mx - vx) * ratio;
    vy = my - (my - vy) * ratio;
    vs = ns;
    drawGraph();
  }, { passive: false });

  canvas.addEventListener('mousedown', function(e) {
    if (e.button !== 0) return;
    isDragging = true;
    dragX = e.clientX; dragY = e.clientY;
    clickStartX = e.clientX; clickStartY = e.clientY;
    dragVx = vx; dragVy = vy;
    canvas.style.cursor = 'grabbing';
  });
  window.addEventListener('mousemove', function(e) {
    if (!isDragging) return;
    vx = dragVx + (e.clientX - dragX);
    vy = dragVy + (e.clientY - dragY);
    drawGraph();
  });
  window.addEventListener('mouseup', function(e) {
    if (isDragging) {
      var wasDrag = Math.abs(e.clientX - clickStartX) > 3 || Math.abs(e.clientY - clickStartY) > 3;
      isDragging = false; canvas.style.cursor = '';
      if (!wasDrag) handleClick(e);
    }
  });
  canvas.addEventListener('dblclick', function() { focusIdx = -1; resetView(); drawGraph(); });

  function resetView() {
    if (N === 0) { vx = 0; vy = 0; vs = 1; return; }
    var r = canvas.parentElement.getBoundingClientRect();
    var w = r.width, h = r.height;
    var x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
    for (var i = 0; i < N; i++) {
      if (posX[i] < x0) x0 = posX[i]; if (posY[i] < y0) y0 = posY[i];
      if (posX[i] > x1) x1 = posX[i]; if (posY[i] > y1) y1 = posY[i];
    }
    if (!isFinite(x0)) { vx = 0; vy = 0; vs = 1; return; }
    var pad = 40;
    vs = Math.min(w / (x1 - x0 + pad * 2), h / (y1 - y0 + pad * 2), 2);
    vx = (w - (x0 + x1) * vs) / 2;
    vy = (h - (y0 + y1) * vs) / 2;
  }

  // ── Click handler (ego-centric selection) ──────────────────────
  function handleClick(e) {
    var r = canvas.getBoundingClientRect();
    var mx = e.clientX - r.left, my = e.clientY - r.top;
    var wx = (mx - vx) / vs, wy = (my - vy) / vs;
    var baseR = Math.max(4, Math.min(12, 400 / Math.sqrt(Math.max(N, 1))));
    var hitR = baseR * 2;
    var best = -1, bestD = hitR * hitR;
    for (var i = 0; i < N; i++) {
      var dx = posX[i] - wx, dy = posY[i] - wy;
      var d2 = dx*dx + dy*dy;
      if (d2 < bestD) { bestD = d2; best = i; }
    }
    if (best < 0) { focusIdx = -1; }
    else { focusIdx = best; }
    drawGraph();
  }

  // ── Force layout ───────────────────────────────────────────────
  function initPositions(w, h) {
    var cx = w / 2, cy = h / 2;
    var rad = Math.min(cx, cy) * 0.6;
    for (var i = 0; i < N; i++) {
      var a = (2 * Math.PI * i) / N - Math.PI / 2;
      posX[i] = cx + rad * Math.cos(a);
      posY[i] = cy + rad * Math.sin(a);
    }
  }

  function runLayout() {
    var r = canvas.parentElement.getBoundingClientRect();
    var w = r.width, h = r.height;
    if (w === 0 || h === 0) return;
    var cx = w/2, cy = h/2;
    var k = Math.sqrt(w*h / Math.max(N, 1)), k2 = k*k;
    var totalIters = Math.min(200, Math.max(30, Math.floor(20000 / Math.max(N, 1))));
    var useBH = N > 60, th2 = 0.64;
    var dx = new Float64Array(N), dy = new Float64Array(N);

    // Edges: self→each member (star topology from this node's perspective)
    var selfIdx = 0; // node 0 is always 'self'
    var edges = [];
    for (var i = 1; i < N; i++) edges.push([selfIdx, i]);

    for (var iter = 0; iter < totalIters; iter++) {
      dx.fill(0); dy.fill(0);

      if (useBH) {
        qtBuild();
        for (var i = 0; i < N; i++) {
          _fx = 0; _fy = 0;
          qtCalc(0, posX[i], posY[i], k2, th2);
          dx[i] += _fx; dy[i] += _fy;
        }
      } else {
        for (var i = 0; i < N; i++) for (var j = i+1; j < N; j++) {
          var ddx = posX[i]-posX[j], ddy = posY[i]-posY[j];
          var dist = Math.sqrt(ddx*ddx + ddy*ddy) || 0.01;
          var f = k2/dist, fx = (ddx/dist)*f, fy = (ddy/dist)*f;
          dx[i] += fx; dy[i] += fy; dx[j] -= fx; dy[j] -= fy;
        }
      }

      for (var e = 0; e < edges.length; e++) {
        var si = edges[e][0], di = edges[e][1];
        var ddx = posX[si]-posX[di], ddy = posY[si]-posY[di];
        var dist = Math.sqrt(ddx*ddx + ddy*ddy) || 0.01;
        var f = (dist*dist)/k, fx = (ddx/dist)*f, fy = (ddy/dist)*f;
        dx[si] -= fx; dy[si] -= fy; dx[di] += fx; dy[di] += fy;
      }

      for (var i = 0; i < N; i++) {
        dx[i] -= (posX[i]-cx)*0.01; dy[i] -= (posY[i]-cy)*0.01;
      }

      var temp = Math.max(0.1, 1 - iter/totalIters);
      for (var i = 0; i < N; i++) {
        var dist = Math.sqrt(dx[i]*dx[i] + dy[i]*dy[i]) || 0.01;
        var cap = Math.min(dist, 10*temp);
        posX[i] += (dx[i]/dist)*cap; posY[i] += (dy[i]/dist)*cap;
        posX[i] = Math.max(40, Math.min(w-40, posX[i]));
        posY[i] = Math.max(40, Math.min(h-40, posY[i]));
      }
    }

    layoutDone = true;
    resetView();
  }

  // ── Draw graph ─────────────────────────────────────────────────
  var stateColors = { 'alive': '#4caf50', 'suspect': '#ff9800', 'dead': '#f44336', 'self': '#6366f1' };

  function drawGraph() {
    var dpr = devicePixelRatio || 1;
    var r = canvas.parentElement.getBoundingClientRect();
    if (canvas.width !== Math.round(r.width * dpr)) resizeCanvas();
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, r.width, r.height);

    if (N === 0) {
      ctx.fillStyle = '#555';
      ctx.font = '13px monospace';
      ctx.textAlign = 'center';
      ctx.fillText('No distribution data yet', r.width / 2, r.height / 2);
      return;
    }

    ctx.save();
    ctx.translate(vx, vy);
    ctx.scale(vs, vs);

    var baseR = Math.max(4, Math.min(12, 400 / Math.sqrt(Math.max(N, 1))));

    // Determine highlighted set for ego mode
    var highlighted = null;
    if (focusIdx >= 0) {
      highlighted = {};
      highlighted[focusIdx] = true;
      // If focus is self (0), highlight all connected members
      // If focus is a member, highlight self and that member
      if (focusIdx === 0) {
        for (var i = 1; i < N; i++) highlighted[i] = true;
      } else {
        highlighted[0] = true; // always show self
      }
    }

    // Draw edges (self → each member)
    for (var i = 1; i < N; i++) {
      var alpha = 1;
      var dash = false;
      if (highlighted) {
        if (!highlighted[i]) { alpha = 0.08; }
        else {
          // Check if this node is a routing neighbor
          var nid = nodeIds[i];
          if (routingSet[nid]) { dash = true; alpha = 0.7; }
          else { alpha = 0.5; }
        }
      } else {
        alpha = 0.2;
      }

      ctx.beginPath();
      ctx.moveTo(posX[0], posY[0]);
      ctx.lineTo(posX[i], posY[i]);
      ctx.strokeStyle = 'rgba(99,102,241,' + alpha + ')';
      ctx.lineWidth = 1 / vs;
      if (dash) { ctx.setLineDash([4/vs, 4/vs]); }
      else { ctx.setLineDash([]); }
      ctx.stroke();
    }
    ctx.setLineDash([]);

    // Draw nodes
    for (var i = 0; i < N; i++) {
      var opacity = 1;
      if (highlighted && !highlighted[i]) opacity = 0.15;

      var col = stateColors[nodeStates[i]] || '#888';
      var isSelf = (nodeStates[i] === 'self');

      ctx.beginPath();
      var nr = isSelf ? baseR * 1.5 : baseR;
      ctx.arc(posX[i], posY[i], nr, 0, Math.PI * 2);
      ctx.globalAlpha = opacity * 0.85;
      ctx.fillStyle = col;
      ctx.fill();

      if (isSelf) {
        ctx.lineWidth = 2 / vs;
        ctx.strokeStyle = '#fff';
        ctx.globalAlpha = opacity * 0.6;
        ctx.stroke();
      }

      if (focusIdx === i) {
        ctx.lineWidth = 2 / vs;
        ctx.strokeStyle = '#fff';
        ctx.globalAlpha = opacity;
        ctx.stroke();
      }

      ctx.globalAlpha = 1;

      // Label (short ID)
      if (vs > 0.5) {
        ctx.fillStyle = '#ccc';
        ctx.globalAlpha = opacity;
        ctx.font = Math.round(9 / vs) + 'px monospace';
        ctx.textAlign = 'center';
        var label = nodeIds[i] ? nodeIds[i].substring(0, 8) : '';
        ctx.fillText(label, posX[i], posY[i] - nr - 3/vs);
        ctx.globalAlpha = 1;
      }
    }

    ctx.restore();
  }

  // ── Update from snapshot ───────────────────────────────────────
  function updateFromSnapshot(d) {
    data = d;

    // Build node arrays: index 0 = self, then members
    var oldN = N;
    var newIds = [d.node_id];
    var newStates = ['self'];
    var newAddrs = [d.listen_addr];
    for (var i = 0; i < d.members.length; i++) {
      newIds.push(d.members[i].node_id);
      newStates.push(d.members[i].state);
      newAddrs.push(d.members[i].addr);
    }

    // Build routing neighbor set
    routingSet = {};
    for (var i = 0; i < d.routing_neighbors.length; i++) {
      routingSet[d.routing_neighbors[i].node_id] = true;
    }

    selfNodeId = d.node_id;

    // Check if topology changed
    var changed = newIds.length !== N;
    if (!changed) {
      for (var i = 0; i < newIds.length; i++) {
        if (newIds[i] !== nodeIds[i]) { changed = true; break; }
      }
    }

    nodeIds = newIds;
    nodeStates = newStates;
    nodeAddrs = newAddrs;
    N = nodeIds.length;

    if (changed || !layoutDone) {
      posX = new Float64Array(N);
      posY = new Float64Array(N);
      resizeCanvas();
      initPositions(canvas.parentElement.getBoundingClientRect().width,
                    canvas.parentElement.getBoundingClientRect().height);
      runLayout();
    } else {
      // Just update states, redraw
      drawGraph();
    }

    updateUI(d);
  }

  function updateUI(d) {
    // Self label
    document.getElementById('selfLabel').textContent = 'Node: ' + d.node_id.substring(0, 16) + '\u2026';
    document.getElementById('nodeLabel').textContent = d.listen_addr;

    // Stats cards
    document.getElementById('statMembers').textContent = d.members.length;
    document.getElementById('statAlive').textContent = d.alive_count;
    document.getElementById('statSuspect').textContent = d.suspect_count;
    document.getElementById('statDead').textContent = d.dead_count;
    document.getElementById('statCache').textContent = d.cache_size;
    document.getElementById('statRT').textContent = d.routing_table_size;
    document.getElementById('statDir').textContent = d.directory_entry_count;
    document.getElementById('statRepair').textContent = d.repair_queue_size;
    document.getElementById('statProbes').textContent = d.recent_probe_targets.length;

    // Color alive/suspect/dead
    document.getElementById('statAlive').style.color = '#4caf50';
    document.getElementById('statSuspect').style.color = d.suspect_count > 0 ? '#ff9800' : '#fff';
    document.getElementById('statDead').style.color = d.dead_count > 0 ? '#f44336' : '#fff';

    // Members table
    var body = document.getElementById('membersBody');
    body.innerHTML = '';
    document.getElementById('memberCount').textContent = '(' + d.members.length + ')';
    for (var i = 0; i < d.members.length; i++) {
      var m = d.members[i];
      var cls = 'state-' + m.state;
      var tr = document.createElement('tr');
      tr.innerHTML =
        '<td class="' + cls + '">' + m.state + '</td>' +
        '<td style="color:#aaa;font-size:10px;">' + m.node_id.substring(0, 16) + '\u2026</td>' +
        '<td>' + m.addr + '</td>' +
        '<td>' + m.incarnation + '</td>';
      body.appendChild(tr);
    }

    // Cache table
    var cacheBody = document.getElementById('cacheBody');
    cacheBody.innerHTML = '';
    document.getElementById('cacheCount').textContent = '(' + d.cache_entries.length + ')';
    var cacheMax = Math.min(d.cache_entries.length, 200);
    for (var i = 0; i < cacheMax; i++) {
      var e = d.cache_entries[i];
      var tr = document.createElement('tr');
      tr.innerHTML =
        '<td style="color:#aaa;font-size:10px;max-width:120px;overflow:hidden;text-overflow:ellipsis;">' + e.actor_addr + '</td>' +
        '<td style="color:#aaa;font-size:10px;">' + e.node_id.substring(0, 12) + '\u2026</td>';
      cacheBody.appendChild(tr);
    }

    // Recent probes — cross-reference with members for state + address
    var memberMap = {};
    for (var i = 0; i < d.members.length; i++) {
      memberMap[d.members[i].node_id] = d.members[i];
    }
    var probesBody = document.getElementById('probesBody');
    probesBody.innerHTML = '';
    document.getElementById('probeCount').textContent = '(' + d.recent_probe_targets.length + ')';
    for (var i = d.recent_probe_targets.length - 1; i >= 0; i--) {
      var pid = d.recent_probe_targets[i];
      var mem = memberMap[pid];
      var state = mem ? mem.state : 'unknown';
      var addr = mem ? mem.addr : '\u2014';
      var cls = 'state-' + state;
      var tr = document.createElement('tr');
      tr.innerHTML =
        '<td class="' + cls + '" style="font-size:10px;">' + state + '</td>' +
        '<td style="color:#aaa;font-size:10px;">' + pid.substring(0, 12) + '\u2026</td>' +
        '<td style="font-size:10px;">' + addr + '</td>';
      probesBody.appendChild(tr);
    }

    // Routing bucket histogram
    drawBucketChart(d.routing_buckets);
  }

  // ── Bucket histogram ───────────────────────────────────────────
  function drawBucketChart(buckets) {
    var cv = document.getElementById('bucketChart');
    var bctx = cv.getContext('2d');
    var dpr = devicePixelRatio || 1;
    var rect = cv.parentElement.getBoundingClientRect();
    var rw = rect.width, rh = cv.parentElement.clientHeight - 20;
    cv.width = rw * dpr;
    cv.height = rh * dpr;
    cv.style.width = rw + 'px';
    cv.style.height = rh + 'px';
    bctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    bctx.clearRect(0, 0, rw, rh);

    if (!buckets || buckets.length === 0) {
      bctx.fillStyle = '#555';
      bctx.font = '11px monospace';
      bctx.textAlign = 'center';
      bctx.fillText('No buckets', rw / 2, rh / 2);
      return;
    }

    var maxCount = 1;
    for (var i = 0; i < buckets.length; i++) {
      if (buckets[i][1] > maxCount) maxCount = buckets[i][1];
    }

    var barW = Math.max(4, Math.floor((rw - 20) / buckets.length) - 2);
    var chartH = rh - 20;

    for (var i = 0; i < buckets.length; i++) {
      var x = 10 + i * (barW + 2);
      var h = (buckets[i][1] / maxCount) * (chartH - 4);
      bctx.fillStyle = '#6366f1';
      bctx.globalAlpha = 0.8;
      bctx.fillRect(x, chartH - h, barW, h);

      if (buckets.length <= 30) {
        bctx.globalAlpha = 1;
        bctx.fillStyle = '#888';
        bctx.font = '8px monospace';
        bctx.textAlign = 'center';
        bctx.fillText(buckets[i][0], x + barW / 2, rh - 2);
      }
    }
    bctx.globalAlpha = 1;
  }

  // ── SSE connection ─────────────────────────────────────────────
  var es = new EventSource('/events');

  es.addEventListener('distribution', function(e) {
    try {
      var d = JSON.parse(e.data);
      updateFromSnapshot(d);
    } catch(err) { console.error('distribution parse error', err); }
  });

  es.addEventListener('done', function() {
    dot.className = 'status-dot done';
    es.close();
  });

  es.onerror = function() {
    dot.className = 'status-dot disconnected';
  };

  es.onopen = function() {
    dot.className = 'status-dot';
  };

  resizeCanvas();
  drawGraph();
})();
</script>
</body>
</html>
"##;
