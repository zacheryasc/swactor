pub const ACTORS_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Swactor Runtime – Actors</title>
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
  .status-dot.replaying { background: #2196f3; animation: pulse 1.5s infinite; }

  @keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.4; }
  }

  .replay-badge {
    display: none; background: #2196f3; color: #fff; font-size: 10px; font-weight: 700;
    padding: 2px 8px; border-radius: 3px; margin-left: 10px; letter-spacing: 1px;
    vertical-align: middle;
  }
  .replay-badge.visible { display: inline-block; }

  .nav-links { display: flex; gap: 4px; margin-left: 20px; }
  .nav-link {
    color: #888; text-decoration: none; font-size: 12px;
    padding: 4px 10px; border-radius: 3px; transition: color 0.2s;
  }
  .nav-link:hover { color: #e0e0e0; }
  .nav-link.active { color: #fff; background: #2a2d3e; }

  .header-right { display: flex; align-items: center; gap: 12px; }

  .progress-bar-wrap {
    display: none; width: 100%; height: 3px; background: #2a2d3e;
  }
  .progress-bar-wrap.visible { display: block; }
  .progress-fill {
    height: 100%; width: 0%; background: #2196f3; transition: width 0.3s;
  }

  .grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 12px; padding: 12px;
  }

  .panel {
    background: #161822; border: 1px solid #2a2d3e; border-radius: 6px;
    padding: 14px; overflow: visible;
  }
  .panel h2 { font-size: 12px; color: #888; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 10px; }

  .full-width { grid-column: 1 / -1; }

  .stats-cards {
    display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px;
  }
  .stat-card {
    background: #1c1f2e; border-radius: 4px; padding: 10px; text-align: center;
  }
  .stat-card .value { font-size: 22px; font-weight: 700; color: #fff; }
  .stat-card .label { font-size: 10px; color: #888; text-transform: uppercase; margin-top: 2px; }

  canvas { width: 100%; height: 220px; }

  .search-wrap { margin-bottom: 10px; display: flex; align-items: center; gap: 12px; }
  .search-input {
    background: #1c1f2e; border: 1px solid #2a2d3e; color: #e0e0e0;
    padding: 6px 10px; border-radius: 4px; font-family: inherit;
    font-size: 12px; width: 300px; outline: none;
  }
  .search-input:focus { border-color: #6366f1; }
  .search-info { color: #555; font-size: 11px; }

  .actor-list-wrap { max-height: 500px; overflow-y: auto; }
  .actor-list-wrap table { width: 100%; border-collapse: collapse; }
  .actor-list-wrap th, .actor-list-wrap td {
    padding: 4px 8px; text-align: left; border-bottom: 1px solid #2a2d3e; font-size: 12px;
  }
  .actor-list-wrap th {
    color: #888; font-weight: 500; position: sticky; top: 0; background: #161822;
  }
  .sortable { cursor: pointer; user-select: none; }
  .sortable:hover { color: #e0e0e0; }
  .sort-arrow { font-size: 10px; margin-left: 4px; color: #6366f1; }

  .depth-bar {
    height: 8px; border-radius: 2px; max-width: 120px; min-width: 2px;
  }

  .msg-type { color: #4caf50; }
  .msg-type.none { color: #555; font-style: italic; }

  tr.focused { background: #1c1f2e; }
  tr.clickable { cursor: pointer; }
  tr.clickable:hover { background: #1a1d2c; }

  .detail-panel {
    display: none; position: fixed; bottom: 12px; right: 12px;
    width: 420px; max-height: 320px; overflow-y: auto;
    background: #161822; border: 1px solid #2a2d3e; border-radius: 6px;
    padding: 14px; z-index: 100; box-shadow: 0 4px 24px rgba(0,0,0,0.5);
  }
  .detail-panel.visible { display: block; }
  .detail-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; }
  .detail-close {
    background: none; border: 1px solid #2a2d3e; color: #888; border-radius: 3px;
    padding: 2px 8px; cursor: pointer; font-family: inherit; font-size: 11px;
  }
  .detail-close:hover { color: #e0e0e0; border-color: #555; }
  .detail-grid {
    display: grid; grid-template-columns: repeat(2, 1fr); gap: 8px; margin-bottom: 10px;
  }
  .detail-item { background: #1c1f2e; border-radius: 4px; padding: 6px 8px; }
  .detail-item .d-label { font-size: 10px; color: #888; text-transform: uppercase; }
  .detail-item .d-value { font-size: 13px; font-weight: 700; color: #fff; margin-top: 2px; word-break: break-all; }
  .poisoned-badge {
    background: #f44336; color: #fff; font-size: 10px; font-weight: 700;
    padding: 2px 6px; border-radius: 3px; letter-spacing: 0.5px;
  }
  .healthy-badge { color: #4caf50; font-size: 12px; }
  canvas.sparkline { width: 100%; height: 50px; }

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
      <span id="statusDot" class="status-dot"></span>
      <span id="replayBadge" class="replay-badge">REPLAY</span>
    </h1>
    <nav class="nav-links">
      <a href="/" class="nav-link">Overview</a>
      <a href="/actors" class="nav-link active">Actors</a>
      <a href="/distribution" class="nav-link">Distribution</a>
      <a href="/datastore" class="nav-link">Datastore</a>
    </nav>
  </div>
  <div class="header-right">
    <span id="replaySpeed" style="color:#2196f3;font-size:12px;display:none;"></span>
    <span id="replayPct" style="color:#888;font-size:12px;display:none;"></span>
    <span id="uptimeLabel" style="color:#888;font-size:12px;"></span>
  </div>
</div>
<div id="progressBarWrap" class="progress-bar-wrap">
  <div id="progressFill" class="progress-fill"></div>
</div>

<div class="grid">
  <!-- Stat cards -->
  <div class="panel full-width">
    <h2>Actor Stats</h2>
    <div class="stats-cards">
      <div class="stat-card"><div class="value" id="statTotal">0</div><div class="label">Total Actors</div></div>
      <div class="stat-card"><div class="value" id="statAvgMbox">0</div><div class="label">Avg Mailbox</div></div>
      <div class="stat-card"><div class="value" id="statMaxMbox">0</div><div class="label">Max Mailbox</div></div>
      <div class="stat-card"><div class="value" id="statActiveWorkers">0</div><div class="label">Active Workers</div></div>
    </div>
  </div>

  <!-- Mailbox depth distribution -->
  <div class="panel">
    <h2>Mailbox Depth Distribution</h2>
    <canvas id="depthChart"></canvas>
  </div>

  <!-- Actors per worker -->
  <div class="panel">
    <h2>Actors per Worker</h2>
    <canvas id="workerChart"></canvas>
  </div>

  <!-- Actor table -->
  <div class="panel full-width">
    <h2>All Actors <span id="actorCount" style="color:#555;font-weight:400;"></span></h2>
    <div class="search-wrap">
      <input type="text" id="actorSearch" class="search-input" placeholder="Filter by address, type, or worker..." />
      <select id="workerFilter" class="search-input" style="width:120px;">
        <option value="">All Workers</option>
      </select>
      <select id="statusFilter" class="search-input" style="width:120px;">
        <option value="">All Status</option>
        <option value="healthy">Healthy</option>
        <option value="poisoned">Poisoned</option>
      </select>
      <input type="number" id="minDepth" class="search-input" style="width:100px;" placeholder="Min depth" min="0" />
      <span id="searchInfo" class="search-info"></span>
    </div>
    <div class="actor-list-wrap">
      <table>
        <thead>
          <tr>
            <th class="sortable" data-sort="address">Address <span id="sortArrowAddress" class="sort-arrow"></span></th>
            <th class="sortable" data-sort="worker">Worker <span id="sortArrowWorker" class="sort-arrow"></span></th>
            <th class="sortable" data-sort="mailbox">Mailbox <span id="sortArrowMailbox" class="sort-arrow"></span></th>
            <th class="sortable" data-sort="msgs">Msgs <span id="sortArrowMsgs" class="sort-arrow"></span></th>
            <th>Last Msg</th>
            <th>Depth</th>
          </tr>
        </thead>
        <tbody id="actorTableBody"></tbody>
      </table>
    </div>
  </div>


</div>

<!-- Actor detail overlay (fixed position, doesn't affect grid layout) -->
<div id="detailPanel" class="detail-panel">
  <div class="detail-header">
    <h2 style="font-size:12px;color:#888;text-transform:uppercase;letter-spacing:1px;">Actor <span id="detailAddr" style="color:#aaa;font-weight:400;"></span></h2>
    <button class="detail-close" id="detailClose">&times;</button>
  </div>
  <div class="detail-grid">
    <div class="detail-item"><div class="d-label">Address</div><div class="d-value" id="detailFullAddr" style="font-size:10px;"></div></div>
    <div class="detail-item"><div class="d-label">Worker</div><div class="d-value" id="detailWorker"></div></div>
    <div class="detail-item"><div class="d-label">Mailbox</div><div class="d-value" id="detailMailbox"></div></div>
    <div class="detail-item"><div class="d-label">Messages</div><div class="d-value" id="detailMsgCount"></div></div>
    <div class="detail-item"><div class="d-label">Last Type</div><div class="d-value" id="detailLastMsg"></div></div>
    <div class="detail-item"><div class="d-label">Status</div><div class="d-value" id="detailStatus"></div></div>
  </div>
  <div style="font-size:10px;color:#888;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px;">Mailbox History</div>
  <canvas id="sparkline" class="sparkline"></canvas>
</div>

<script>
(function() {
  var DASHBOARD_MODE = '__DASHBOARD_MODE__';
  var isReplay = (DASHBOARD_MODE === 'replay');
  var lastUptimeMs = null;
  var lastStatsTime = null;

  var dot = document.getElementById('statusDot');
  var uptimeLabel = document.getElementById('uptimeLabel');
  var replayBadge = document.getElementById('replayBadge');
  var replaySpeed = document.getElementById('replaySpeed');
  var replayPct = document.getElementById('replayPct');
  var progressBarWrap = document.getElementById('progressBarWrap');
  var progressFill = document.getElementById('progressFill');

  if (isReplay) {
    replayBadge.className = 'replay-badge visible';
    progressBarWrap.className = 'progress-bar-wrap visible';
    dot.className = 'status-dot replaying';
    uptimeLabel.style.display = 'none';
  }

  function updateUptime() {
    if (isReplay) return;
    var up = lastUptimeMs;
    if (up !== null && lastStatsTime !== null) {
      up += (Date.now() - lastStatsTime);
    }
    if (up === null) { uptimeLabel.textContent = ''; return; }
    var s = Math.floor(up / 1000);
    var d = Math.floor(s / 86400);
    var h = Math.floor((s % 86400) / 3600);
    var m = Math.floor((s % 3600) / 60);
    var sec = s % 60;
    var parts = [];
    if (d > 0) parts.push(d + 'd');
    if (h > 0 || d > 0) parts.push(h + 'h');
    parts.push(m + 'm');
    parts.push(sec + 's');
    uptimeLabel.textContent = parts.join(' ');
  }
  setInterval(updateUptime, 1000);

  // ── State ──────────────────────────────────────────────
  var currentActors = [];
  var sortCol = 'worker';
  var sortAsc = true;
  var searchTimer = null;
  var focusedAddrHex = null;
  var depthHistory = {};
  var HISTORY_MAX = 120;

  var colors = ['#4caf50','#2196f3','#ff9800','#f44336','#9c27b0','#00bcd4','#ffeb3b','#e91e63'];

  // ── Helpers ────────────────────────────────────────────
  function addrToHex(addr) {
    var bytes = Array.isArray(addr) ? addr : Object.values(addr);
    var hex = '';
    for (var j = 0; j < Math.min(8, bytes.length); j++) {
      hex += ('0' + bytes[j].toString(16)).slice(-2);
    }
    return hex + '\u2026';
  }

  function addrToFullHex(addr) {
    var bytes = Array.isArray(addr) ? addr : Object.values(addr);
    var hex = '';
    for (var j = 0; j < bytes.length; j++) {
      hex += ('0' + bytes[j].toString(16)).slice(-2);
    }
    return hex;
  }

  function shortTypeName(full) {
    if (!full) return '';
    var parts = full.split('::');
    return parts[parts.length - 1];
  }

  function escapeHtml(s) {
    if (!s) return '';
    return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }

  function depthColor(d) {
    if (d === 0) return '#4caf50';
    if (d <= 2) return '#8bc34a';
    if (d <= 5) return '#cddc39';
    if (d <= 10) return '#ff9800';
    if (d <= 20) return '#ff5722';
    return '#f44336';
  }

  // ── Stat cards ─────────────────────────────────────────
  function updateStatCards(actors) {
    var total = actors.length;
    var sum = 0, max = 0;
    var workerSet = {};
    for (var i = 0; i < actors.length; i++) {
      sum += actors[i].mailbox_depth;
      if (actors[i].mailbox_depth > max) max = actors[i].mailbox_depth;
      workerSet[actors[i].worker_id] = true;
    }
    var avg = total > 0 ? (sum / total).toFixed(1) : '0';
    document.getElementById('statTotal').textContent = total;
    document.getElementById('statAvgMbox').textContent = avg;
    document.getElementById('statMaxMbox').textContent = max;
    document.getElementById('statActiveWorkers').textContent = Object.keys(workerSet).length;
  }

  // ── Mailbox depth distribution chart ───────────────────
  var depthCanvas = document.getElementById('depthChart');
  var depthCtx = depthCanvas.getContext('2d');

  var buckets = [
    {label: '0', min: 0, max: 0},
    {label: '1-2', min: 1, max: 2},
    {label: '3-5', min: 3, max: 5},
    {label: '6-10', min: 6, max: 10},
    {label: '11-20', min: 11, max: 20},
    {label: '21-50', min: 21, max: 50},
    {label: '50+', min: 51, max: Infinity}
  ];

  var bucketColors = ['#4caf50','#8bc34a','#cddc39','#ff9800','#ff5722','#f44336','#d32f2f'];

  function drawDepthChart(actors) {
    var dpr = window.devicePixelRatio || 1;
    var rect = depthCanvas.getBoundingClientRect();
    depthCanvas.width = rect.width * dpr;
    depthCanvas.height = rect.height * dpr;
    depthCtx.scale(dpr, dpr);
    var W = rect.width, H = rect.height;
    depthCtx.clearRect(0, 0, W, H);

    var counts = buckets.map(function() { return 0; });
    for (var i = 0; i < actors.length; i++) {
      var d = actors[i].mailbox_depth;
      for (var b = 0; b < buckets.length; b++) {
        if (d >= buckets[b].min && d <= buckets[b].max) {
          counts[b]++;
          break;
        }
      }
    }

    var maxCount = Math.max(1, Math.max.apply(null, counts));
    var barW = Math.max(12, Math.floor((W - 40) / buckets.length) - 8);
    var topPad = 20;
    var chartH = H - 35;

    for (var i = 0; i < buckets.length; i++) {
      var x = 20 + i * (barW + 8);
      var h = (counts[i] / maxCount) * (chartH - topPad);
      depthCtx.fillStyle = bucketColors[i];
      depthCtx.globalAlpha = 0.85;
      depthCtx.fillRect(x, chartH - h, barW, h);

      if (counts[i] > 0) {
        depthCtx.globalAlpha = 1;
        depthCtx.fillStyle = '#e0e0e0';
        depthCtx.font = '10px monospace';
        depthCtx.textAlign = 'center';
        depthCtx.fillText(counts[i], x + barW / 2, chartH - h - 4);
      }

      depthCtx.globalAlpha = 1;
      depthCtx.fillStyle = '#888';
      depthCtx.font = '10px monospace';
      depthCtx.textAlign = 'center';
      depthCtx.fillText(buckets[i].label, x + barW / 2, H - 4);
    }
  }

  // ── Actors per worker chart ────────────────────────────
  var workerCanvas = document.getElementById('workerChart');
  var workerCtx = workerCanvas.getContext('2d');

  function drawWorkerChart(actors) {
    var dpr = window.devicePixelRatio || 1;
    var rect = workerCanvas.getBoundingClientRect();
    workerCanvas.width = rect.width * dpr;
    workerCanvas.height = rect.height * dpr;
    workerCtx.scale(dpr, dpr);
    var W = rect.width, H = rect.height;
    workerCtx.clearRect(0, 0, W, H);

    var workerCounts = {};
    for (var i = 0; i < actors.length; i++) {
      var wid = actors[i].worker_id;
      workerCounts[wid] = (workerCounts[wid] || 0) + 1;
    }
    var entries = Object.keys(workerCounts).sort(function(a, b) { return +a - +b; })
      .map(function(wid) { return {id: +wid, count: workerCounts[wid]}; });

    if (entries.length === 0) return;

    var maxCount = Math.max(1, Math.max.apply(null, entries.map(function(e) { return e.count; })));
    var barW = Math.max(12, Math.floor((W - 40) / entries.length) - 8);
    var topPad = 20;
    var chartH = H - 35;

    for (var i = 0; i < entries.length; i++) {
      var x = 20 + i * (barW + 8);
      var h = (entries[i].count / maxCount) * (chartH - topPad);
      workerCtx.fillStyle = colors[entries[i].id % colors.length];
      workerCtx.globalAlpha = 0.85;
      workerCtx.fillRect(x, chartH - h, barW, h);

      workerCtx.globalAlpha = 1;
      workerCtx.fillStyle = '#e0e0e0';
      workerCtx.font = '10px monospace';
      workerCtx.textAlign = 'center';
      workerCtx.fillText(entries[i].count, x + barW / 2, chartH - h - 4);

      workerCtx.fillStyle = '#888';
      workerCtx.fillText('W' + entries[i].id, x + barW / 2, H - 4);
    }
  }

  // ── Actor table ────────────────────────────────────────
  var MAX_TABLE_ROWS = 2000;

  function renderActorTable() {
    var filter = document.getElementById('actorSearch').value.toLowerCase();
    var workerFilter = document.getElementById('workerFilter').value;
    var statusFilter = document.getElementById('statusFilter').value;
    var minDepthVal = document.getElementById('minDepth').value;
    var minDepth = minDepthVal ? parseInt(minDepthVal, 10) : 0;

    var filtered = currentActors.filter(function(a) {
      // Text search
      if (filter) {
        var hex = addrToHex(a.address).toLowerCase();
        var msgType = (a.last_msg_type || '').toLowerCase();
        if (hex.indexOf(filter) < 0 && ('w' + a.worker_id).indexOf(filter) < 0 && msgType.indexOf(filter) < 0) {
          return false;
        }
      }
      // Worker filter
      if (workerFilter && a.worker_id !== parseInt(workerFilter, 10)) return false;
      // Status filter
      if (statusFilter === 'healthy' && a.poisoned) return false;
      if (statusFilter === 'poisoned' && !a.poisoned) return false;
      // Min depth
      if (minDepth > 0 && a.mailbox_depth < minDepth) return false;
      return true;
    });

    // Sort
    filtered.sort(function(a, b) {
      var va, vb;
      if (sortCol === 'address') {
        va = addrToHex(a.address);
        vb = addrToHex(b.address);
        return sortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
      } else if (sortCol === 'worker') {
        va = a.worker_id; vb = b.worker_id;
      } else if (sortCol === 'msgs') {
        va = a.messages_processed || 0; vb = b.messages_processed || 0;
      } else {
        va = a.mailbox_depth; vb = b.mailbox_depth;
      }
      return sortAsc ? va - vb : vb - va;
    });

    // Update sort arrows
    ['address', 'worker', 'mailbox', 'msgs'].forEach(function(col) {
      var key = col.charAt(0).toUpperCase() + col.slice(1);
      var el = document.getElementById('sortArrow' + key);
      if (col === sortCol) {
        el.textContent = sortAsc ? '\u25B2' : '\u25BC';
      } else {
        el.textContent = '';
      }
    });

    var maxDepth = 1;
    for (var i = 0; i < currentActors.length; i++) {
      if (currentActors[i].mailbox_depth > maxDepth) maxDepth = currentActors[i].mailbox_depth;
    }

    var tbody = document.getElementById('actorTableBody');
    tbody.innerHTML = '';
    var count = Math.min(filtered.length, MAX_TABLE_ROWS);
    for (var i = 0; i < count; i++) {
      var a = filtered[i];
      var hex = addrToHex(a.address);
      var fullHex = addrToFullHex(a.address);
      var pct = Math.round((a.mailbox_depth / maxDepth) * 120);
      var bc = depthColor(a.mailbox_depth);
      var hasMsg = !!a.last_msg_type;
      var msgShort = hasMsg ? shortTypeName(a.last_msg_type) : '\u2014';
      var msgClass = hasMsg ? 'msg-type' : 'msg-type none';
      var msgTitle = hasMsg ? ' title="' + escapeHtml(a.last_msg_type) + '"' : '';
      var tr = document.createElement('tr');
      tr.className = 'clickable' + (fullHex === focusedAddrHex ? ' focused' : '');
      if (a.poisoned) tr.style.opacity = '0.6';
      tr.setAttribute('data-addr', fullHex);
      tr.innerHTML =
        '<td style="color:#aaa;font-size:11px;">' + escapeHtml(hex) + (a.poisoned ? ' <span style="color:#f44336;font-size:9px;">DEAD</span>' : '') + '</td>' +
        '<td>W' + a.worker_id + '</td>' +
        '<td>' + a.mailbox_depth + '</td>' +
        '<td>' + (a.messages_processed || 0).toLocaleString() + '</td>' +
        '<td class="' + msgClass + '"' + msgTitle + '>' + escapeHtml(msgShort) + '</td>' +
        '<td><div class="depth-bar" style="width:' + pct + 'px;background:' + bc + ';"></div></td>';
      tr.addEventListener('click', (function(fh) { return function() { focusActor(fh); }; })(fullHex));
      tbody.appendChild(tr);
    }

    if (filtered.length > MAX_TABLE_ROWS) {
      var tr2 = document.createElement('tr');
      tr2.innerHTML = '<td colspan="6" style="color:#555;">... and ' + (filtered.length - MAX_TABLE_ROWS) + ' more</td>';
      tbody.appendChild(tr2);
    }

    var info = '(' + filtered.length + (filtered.length !== currentActors.length ? ' of ' + currentActors.length : '') + ')';
    document.getElementById('actorCount').textContent = info;
  }

  // ── Focus / detail panel ───────────────────────────────
  function focusActor(fullHex) {
    focusedAddrHex = fullHex;
    document.getElementById('detailPanel').className = 'detail-panel visible';
    updateDetailPanel();
    renderActorTable();
  }

  function clearFocus() {
    focusedAddrHex = null;
    document.getElementById('detailPanel').className = 'detail-panel';
    renderActorTable();
  }

  document.getElementById('detailClose').addEventListener('click', clearFocus);

  function updateDetailPanel() {
    if (!focusedAddrHex) return;
    var actor = null;
    for (var i = 0; i < currentActors.length; i++) {
      if (addrToFullHex(currentActors[i].address) === focusedAddrHex) {
        actor = currentActors[i];
        break;
      }
    }
    if (!actor) {
      document.getElementById('detailAddr').textContent = focusedAddrHex.substring(0, 16) + '\u2026 (gone)';
      return;
    }

    document.getElementById('detailAddr').innerHTML = '<a href="/actor/' + focusedAddrHex + '" style="color:#aaa;text-decoration:none;">' + escapeHtml(addrToHex(actor.address)) + '</a>';
    document.getElementById('detailFullAddr').innerHTML = '<a href="/actor/' + focusedAddrHex + '" style="color:#fff;text-decoration:none;">' + escapeHtml(focusedAddrHex) + '</a>';
    document.getElementById('detailWorker').textContent = 'W' + actor.worker_id;
    document.getElementById('detailMailbox').textContent = actor.mailbox_depth;
    document.getElementById('detailMsgCount').textContent = (actor.messages_processed || 0).toLocaleString();

    var hasMsg = !!actor.last_msg_type;
    document.getElementById('detailLastMsg').innerHTML = hasMsg
      ? '<span class="msg-type" title="' + escapeHtml(actor.last_msg_type) + '">' + escapeHtml(shortTypeName(actor.last_msg_type)) + '</span>'
      : '<span class="msg-type none">\u2014</span>';

    var isPoisoned = !!actor.poisoned;
    document.getElementById('detailStatus').innerHTML = isPoisoned
      ? '<span class="poisoned-badge">POISONED</span>'
      : '<span class="healthy-badge">Healthy</span>';

    drawSparkline();
  }

  // ── Sparkline ──────────────────────────────────────────
  function drawSparkline() {
    var canvas = document.getElementById('sparkline');
    var ctx = canvas.getContext('2d');
    var dpr = window.devicePixelRatio || 1;
    var rect = canvas.getBoundingClientRect();
    canvas.width = rect.width * dpr;
    canvas.height = rect.height * dpr;
    ctx.scale(dpr, dpr);
    var W = rect.width, H = rect.height;
    ctx.clearRect(0, 0, W, H);

    var hist = depthHistory[focusedAddrHex];
    if (!hist || hist.length < 2) {
      ctx.fillStyle = '#555';
      ctx.font = '11px monospace';
      ctx.textAlign = 'center';
      ctx.fillText('Collecting data\u2026', W / 2, H / 2);
      return;
    }

    var max = Math.max(1, Math.max.apply(null, hist));
    var padY = 6, padX = 4;
    var drawW = W - padX * 2;
    var drawH = H - padY * 2;

    // Fill area
    ctx.beginPath();
    ctx.moveTo(padX, H - padY);
    for (var i = 0; i < hist.length; i++) {
      var x = padX + (i / (hist.length - 1)) * drawW;
      var y = (H - padY) - (hist[i] / max) * drawH;
      ctx.lineTo(x, y);
    }
    ctx.lineTo(padX + drawW, H - padY);
    ctx.closePath();
    ctx.fillStyle = 'rgba(99, 102, 241, 0.15)';
    ctx.fill();

    // Line
    ctx.beginPath();
    for (var i = 0; i < hist.length; i++) {
      var x = padX + (i / (hist.length - 1)) * drawW;
      var y = (H - padY) - (hist[i] / max) * drawH;
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.strokeStyle = '#6366f1';
    ctx.lineWidth = 1.5;
    ctx.stroke();

    // Label
    var last = hist[hist.length - 1];
    ctx.fillStyle = '#e0e0e0';
    ctx.font = '10px monospace';
    ctx.textAlign = 'right';
    ctx.fillText('depth: ' + last + '  max: ' + max, W - padX, padY + 8);
  }

  // ── Sort click handlers ────────────────────────────────
  var sortHeaders = document.querySelectorAll('.sortable');
  for (var i = 0; i < sortHeaders.length; i++) {
    sortHeaders[i].addEventListener('click', function() {
      var col = this.getAttribute('data-sort');
      if (sortCol === col) { sortAsc = !sortAsc; }
      else { sortCol = col; sortAsc = true; }
      renderActorTable();
    });
  }

  // ── Search/filter handlers ─────────────────────────────
  document.getElementById('actorSearch').addEventListener('keyup', function() {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(renderActorTable, 150);
  });
  document.getElementById('workerFilter').addEventListener('change', renderActorTable);
  document.getElementById('statusFilter').addEventListener('change', renderActorTable);
  document.getElementById('minDepth').addEventListener('input', function() {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(renderActorTable, 150);
  });

  // ── Status helpers ─────────────────────────────────────
  function setStatus(s) {
    if (s === 'done') {
      dot.className = 'status-dot done';
      if (isReplay) {
        replayPct.textContent = '100%';
        progressFill.style.width = '100%';
      }
    } else if (s === 'disconnected') {
      dot.className = 'status-dot disconnected';
    } else {
      dot.className = isReplay ? 'status-dot replaying' : 'status-dot';
    }
  }

  // ── SSE connection ─────────────────────────────────────
  var es = new EventSource('/events');

  es.addEventListener('stats', function(e) {
    try {
      var data = JSON.parse(e.data);
      if (typeof data.uptime_ms === 'number') {
        lastUptimeMs = data.uptime_ms;
        lastStatsTime = Date.now();
      }
      currentActors = data.actor_details || [];
      updateStatCards(currentActors);
      drawDepthChart(currentActors);
      drawWorkerChart(currentActors);

      // Accumulate per-actor depth history
      var liveAddrs = {};
      for (var i = 0; i < currentActors.length; i++) {
        var fh = addrToFullHex(currentActors[i].address);
        liveAddrs[fh] = true;
        if (!depthHistory[fh]) depthHistory[fh] = [];
        depthHistory[fh].push(currentActors[i].mailbox_depth);
        if (depthHistory[fh].length > HISTORY_MAX) depthHistory[fh].shift();
      }
      // Clean up stale entries for removed actors
      for (var key in depthHistory) {
        if (!liveAddrs[key]) delete depthHistory[key];
      }

      // Update worker filter dropdown
      var wSelect = document.getElementById('workerFilter');
      var curVal = wSelect.value;
      var workerIds = {};
      for (var i = 0; i < currentActors.length; i++) workerIds[currentActors[i].worker_id] = true;
      var wids = Object.keys(workerIds).sort(function(a,b) { return +a - +b; });
      wSelect.innerHTML = '<option value="">All Workers</option>';
      wids.forEach(function(wid) {
        var opt = document.createElement('option');
        opt.value = wid;
        opt.textContent = 'W' + wid;
        wSelect.appendChild(opt);
      });
      wSelect.value = curVal;

      renderActorTable();
      if (focusedAddrHex) updateDetailPanel();
    } catch(err) { console.error('stats parse error', err); }
  });

  es.addEventListener('replay_meta', function(e) {
    try {
      var meta = JSON.parse(e.data);
      replaySpeed.textContent = meta.speed + 'x';
      replaySpeed.style.display = 'inline';
      replayPct.style.display = 'inline';
      replayPct.textContent = '0%';
    } catch(err) { console.error('replay_meta parse error', err); }
  });

  es.addEventListener('replay_progress', function(e) {
    try {
      var data = JSON.parse(e.data);
      var pct = Math.round(data.progress * 100);
      replayPct.textContent = pct + '%';
      progressFill.style.width = pct + '%';
    } catch(err) { console.error('replay_progress parse error', err); }
  });

  es.addEventListener('done', function() {
    setStatus('done');
    es.close();
  });

  es.onerror = function() {
    setStatus('disconnected');
  };

  es.onopen = function() {
    setStatus('connected');
  };
})();
</script>
</body>
</html>
"##;
