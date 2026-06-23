pub const ACTOR_DETAIL_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Actor Detail — Swactor Dashboard</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'Menlo', 'Consolas', 'Monaco', monospace; background: #0f1117; color: #e0e0e0; font-size: 13px; }

  .header {
    display: flex; align-items: center; justify-content: space-between;
    padding: 12px 20px; background: #161822; border-bottom: 1px solid #2a2d3e;
  }
  .header h1 { font-size: 16px; font-weight: 600; color: #fff; }
  .status-dot {
    width: 10px; height: 10px; border-radius: 50%; background: #4caf50;
    display: inline-block; margin-left: 8px; vertical-align: middle;
  }
  .status-dot.disconnected { background: #f44336; }

  .header-left { display: flex; align-items: center; }
  .nav-links { display: flex; gap: 4px; margin-left: 20px; }
  .nav-link {
    color: #888; text-decoration: none; font-size: 12px;
    padding: 4px 10px; border-radius: 3px; transition: color 0.2s;
  }
  .nav-link:hover { color: #e0e0e0; }
  .nav-link.active { color: #fff; background: #2a2d3e; }

  .content { padding: 16px 20px; max-width: 900px; }

  .breadcrumb { color: #555; font-size: 12px; margin-bottom: 12px; }
  .breadcrumb a { color: #888; text-decoration: none; }
  .breadcrumb a:hover { color: #e0e0e0; }

  .info-card {
    background: #161822; border: 1px solid #2a2d3e; border-radius: 6px;
    padding: 16px; margin-bottom: 12px;
  }
  .info-row { display: flex; gap: 24px; margin-bottom: 6px; flex-wrap: wrap; }
  .info-label { color: #888; font-size: 11px; text-transform: uppercase; }
  .info-value { color: #fff; font-weight: 700; font-size: 15px; }
  .info-value.healthy { color: #4caf50; }
  .info-value.poisoned { color: #f44336; }

  .stats-cards {
    display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px;
    margin-bottom: 12px;
  }
  .stat-card {
    background: #161822; border: 1px solid #2a2d3e; border-radius: 4px;
    padding: 12px; text-align: center;
  }
  .stat-card .value { font-size: 22px; font-weight: 700; color: #fff; }
  .stat-card .label { font-size: 10px; color: #888; text-transform: uppercase; margin-top: 2px; }

  .sparkline-panel {
    background: #161822; border: 1px solid #2a2d3e; border-radius: 6px;
    padding: 14px; margin-bottom: 12px;
  }
  .sparkline-panel h3 { font-size: 11px; color: #888; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 8px; }
  .sparkline-panel svg { width: 100%; height: 50px; }

  .type-breakdown {
    background: #161822; border: 1px solid #2a2d3e; border-radius: 6px;
    padding: 14px; margin-bottom: 12px;
  }
  .type-breakdown h3 { font-size: 11px; color: #888; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 8px; }
  .type-row { display: flex; align-items: center; gap: 8px; margin-bottom: 4px; font-size: 11px; }
  .type-name { color: #e0e0e0; min-width: 180px; text-overflow: ellipsis; overflow: hidden; white-space: nowrap; }
  .type-bar-bg { flex: 1; background: #1e2030; height: 14px; border-radius: 2px; overflow: hidden; }
  .type-bar-fill { height: 100%; border-radius: 2px; }
  .type-count { color: #888; min-width: 60px; text-align: right; }
  .type-pct { color: #555; min-width: 40px; text-align: right; }

  .logs-panel {
    background: #161822; border: 1px solid #2a2d3e; border-radius: 6px;
    padding: 14px; margin-bottom: 12px;
  }
  .logs-panel h3 {
    font-size: 11px; color: #888; text-transform: uppercase; letter-spacing: 1px;
    margin-bottom: 8px; display: flex; align-items: center; gap: 12px;
  }
  .level-filter { display: flex; gap: 4px; }
  .level-btn {
    background: #1e2030; border: 1px solid #2a2d3e; border-radius: 3px;
    color: #888; font-size: 10px; padding: 1px 6px; cursor: pointer;
    font-family: inherit;
  }
  .level-btn.active { border-color: #555; color: #fff; }
  .level-btn.error { color: #f44336; }
  .level-btn.warn { color: #ff9800; }
  .level-btn.info { color: #2196f3; }
  .level-btn.debug { color: #888; }
  .level-btn.trace { color: #555; }

  .log-list {
    max-height: 400px; overflow-y: auto; font-size: 11px; line-height: 1.6;
  }
  .log-entry { display: flex; gap: 8px; padding: 1px 0; border-bottom: 1px solid #1a1c2e; }
  .log-time { color: #555; white-space: nowrap; min-width: 80px; }
  .log-level { font-weight: 700; min-width: 50px; }
  .log-level.ERROR { color: #f44336; }
  .log-level.WARN { color: #ff9800; }
  .log-level.INFO { color: #2196f3; }
  .log-level.DEBUG { color: #888; }
  .log-level.TRACE { color: #555; }
  .log-msg { color: #e0e0e0; word-break: break-all; }

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
    </h1>
    <nav class="nav-links">
      <a href="/" class="nav-link">Overview</a>
      <a href="/actors" class="nav-link">Actors</a>
      <a href="/plugin/distribution" class="nav-link">Distribution</a>
      <a href="/plugin/vastai" class="nav-link">Fleet</a>
    </nav>
  </div>
</div>

<div class="content">
  <div class="breadcrumb">
    <a href="/">Overview</a> / <a href="/actors">Actors</a> / <span id="addrBreadcrumb">—</span>
  </div>

  <div class="info-card">
    <div class="info-row" id="nameRow" style="display:none;">
      <div><div class="info-label">Name</div><div class="info-value" id="addrName" style="color:#00bcd4;">—</div></div>
    </div>
    <div class="info-row">
      <div><div class="info-label">Address</div><div class="info-value" id="addrFull" style="font-size:12px;color:#aaa;font-weight:400;">—</div></div>
      <div><div class="info-label">Worker</div><div class="info-value" id="addrWorker">—</div></div>
      <div><div class="info-label">Status</div><div class="info-value" id="addrStatus">—</div></div>
    </div>
    <div class="info-row">
      <div><div class="info-label">Last Message Type</div><div class="info-value" id="addrLastMsg" style="color:#4caf50;font-size:13px;">—</div></div>
    </div>
  </div>

  <div class="stats-cards">
    <div class="stat-card"><div class="value" id="addrMsgs">0</div><div class="label">Messages</div></div>
    <div class="stat-card"><div class="value" id="addrMailbox">0</div><div class="label">Mailbox</div></div>
    <div class="stat-card"><div class="value" id="addrRate">0</div><div class="label">Msg/s</div></div>
    <div class="stat-card"><div class="value" id="addrWorkerLoad">—</div><div class="label">Worker Load</div></div>
  </div>

  <div class="sparkline-panel">
    <h3>Message Rate</h3>
    <svg id="rateSpark" viewBox="0 0 400 50" preserveAspectRatio="none"></svg>
  </div>

  <div class="sparkline-panel">
    <h3>Mailbox Depth</h3>
    <svg id="mboxSpark" viewBox="0 0 400 50" preserveAspectRatio="none"></svg>
  </div>

  <div class="type-breakdown" id="typeBreakdown" style="display:none;">
    <h3>Message Types</h3>
    <div id="typeRows"></div>
  </div>

  <div class="logs-panel">
    <h3>
      <span>Logs</span>
      <span id="logCount" style="color:#555;">(0)</span>
      <div class="level-filter">
        <button class="level-btn error active" data-level="ERROR" onclick="toggleLevel(this)">ERR</button>
        <button class="level-btn warn active" data-level="WARN" onclick="toggleLevel(this)">WARN</button>
        <button class="level-btn info active" data-level="INFO" onclick="toggleLevel(this)">INFO</button>
        <button class="level-btn debug active" data-level="DEBUG" onclick="toggleLevel(this)">DBG</button>
        <button class="level-btn trace active" data-level="TRACE" onclick="toggleLevel(this)">TRC</button>
      </div>
    </h3>
    <div class="log-list" id="logList"></div>
  </div>
</div>

<script>
(function() {
  var targetAddr = '__ACTOR_ADDR__';
  var dot = document.getElementById('statusDot');

  var history = { rates: [], mailbox: [], prev_msgs: 0 };

  function formatAddr(addr) {
    if (!addr) return '';
    var bytes = Array.isArray(addr) ? addr : Object.values(addr);
    var hex = '';
    for (var i = 0; i < bytes.length; i++) {
      hex += ('0' + bytes[i].toString(16)).slice(-2);
    }
    return hex;
  }

  function shortAddr(hex) {
    return hex.length > 16 ? hex.substring(0, 16) + '\u2026' : hex;
  }

  function shortTypeName(full) {
    if (!full) return '\u2014';
    var parts = full.split('::');
    return parts[parts.length - 1];
  }

  function updateSparklineSvg(svgEl, data, color) {
    if (!data || data.length < 2) { svgEl.innerHTML = ''; return; }
    var max = Math.max.apply(null, data);
    if (max === 0) max = 1;
    var w = 400, h = 50;
    var step = w / (data.length - 1);
    var points = data.map(function(v, i) {
      return (i * step).toFixed(1) + ',' + (h - (v / max) * (h - 4) - 2).toFixed(1);
    }).join(' ');
    svgEl.innerHTML = '<polyline fill="none" stroke="' + color + '" stroke-width="2" points="' + points + '"/>';
  }

  function findActor(stats) {
    if (!stats.actor_details) return null;
    for (var i = 0; i < stats.actor_details.length; i++) {
      var a = stats.actor_details[i];
      var hex = formatAddr(a.address);
      if (hex === targetAddr || hex.indexOf(targetAddr) === 0) return a;
    }
    return null;
  }

  function updateDetail(stats) {
    var actor = findActor(stats);
    if (!actor) return;

    var hex = formatAddr(actor.address);
    if (actor.name) {
      document.getElementById('addrBreadcrumb').textContent = actor.name;
      document.getElementById('nameRow').style.display = '';
      document.getElementById('addrName').textContent = actor.name;
    } else {
      document.getElementById('addrBreadcrumb').textContent = shortAddr(hex);
      document.getElementById('nameRow').style.display = 'none';
    }
    document.getElementById('addrFull').textContent = hex;
    document.getElementById('addrWorker').textContent = 'W' + actor.worker_id;

    var statusEl = document.getElementById('addrStatus');
    if (actor.poisoned) {
      statusEl.textContent = 'POISONED';
      statusEl.className = 'info-value poisoned';
    } else {
      statusEl.textContent = 'Healthy';
      statusEl.className = 'info-value healthy';
    }

    document.getElementById('addrLastMsg').textContent = shortTypeName(actor.last_msg_type);
    document.getElementById('addrMsgs').textContent = actor.messages_processed.toLocaleString();
    document.getElementById('addrMailbox').textContent = actor.mailbox_depth;

    // Compute rate
    var rate = actor.messages_processed - history.prev_msgs;
    if (rate < 0) rate = 0;
    history.prev_msgs = actor.messages_processed;
    history.rates.push(rate);
    history.mailbox.push(actor.mailbox_depth);
    if (history.rates.length > 300) { history.rates.shift(); history.mailbox.shift(); }

    // Rate per second (SSE interval is ~200ms, so multiply by 5)
    document.getElementById('addrRate').textContent = (rate * 5).toLocaleString();

    // Worker load
    if (stats.workers) {
      var w = stats.workers.find(function(w) { return w.id === actor.worker_id; });
      if (w) {
        document.getElementById('addrWorkerLoad').textContent =
          w.num_actors + ' actors, mbox ' + w.mailbox_depth;
      }
    }

    updateSparklineSvg(document.getElementById('rateSpark'), history.rates, '#4caf50');
    updateSparklineSvg(document.getElementById('mboxSpark'), history.mailbox, '#2196f3');

    // Update message type breakdown
    updateTypeBreakdown(actor.message_type_counts);
  }

  var typeColors = ['#4caf50','#2196f3','#ff9800','#9c27b0','#00bcd4','#f44336','#ffeb3b','#e91e63'];

  function updateTypeBreakdown(types) {
    var panel = document.getElementById('typeBreakdown');
    var container = document.getElementById('typeRows');
    if (!types || types.length === 0) { panel.style.display = 'none'; return; }
    panel.style.display = '';
    var total = 0;
    for (var i = 0; i < types.length; i++) total += types[i][1];
    if (total === 0) { panel.style.display = 'none'; return; }

    var html = '';
    for (var i = 0; i < types.length; i++) {
      var name = types[i][0];
      var count = types[i][1];
      var pct = (count / total * 100).toFixed(1);
      var barPct = (count / types[0][1] * 100).toFixed(1);
      var color = typeColors[i % typeColors.length];
      var shortName = name.split('::').pop();
      html += '<div class="type-row">' +
        '<span class="type-name" title="' + escapeHtml(name) + '">' + escapeHtml(shortName) + '</span>' +
        '<div class="type-bar-bg"><div class="type-bar-fill" style="width:' + barPct + '%;background:' + color + ';"></div></div>' +
        '<span class="type-count">' + count.toLocaleString() + '</span>' +
        '<span class="type-pct">' + pct + '%</span>' +
        '</div>';
    }
    container.innerHTML = html;
  }

  // ─── Logging ─────────────────────────────────────────────────
  var activeLevels = { ERROR: true, WARN: true, INFO: true, DEBUG: true, TRACE: true };
  var allLogs = [];
  var logList = document.getElementById('logList');
  var logCount = document.getElementById('logCount');
  var autoScroll = true;

  window.toggleLevel = function(btn) {
    var lvl = btn.getAttribute('data-level');
    activeLevels[lvl] = !activeLevels[lvl];
    btn.classList.toggle('active');
    renderLogs();
  };

  function formatLogTime(ms) {
    var d = new Date(ms);
    return ('0' + d.getHours()).slice(-2) + ':' +
           ('0' + d.getMinutes()).slice(-2) + ':' +
           ('0' + d.getSeconds()).slice(-2) + '.' +
           ('00' + d.getMilliseconds()).slice(-3);
  }

  function renderLogs() {
    var visible = allLogs.filter(function(e) { return activeLevels[e.level]; });
    logCount.textContent = '(' + visible.length + ')';
    var html = '';
    for (var i = 0; i < visible.length; i++) {
      var e = visible[i];
      html += '<div class="log-entry">' +
        '<span class="log-time">' + formatLogTime(e.timestamp_ms) + '</span>' +
        '<span class="log-level ' + e.level + '">' + e.level + '</span>' +
        '<span class="log-msg">' + escapeHtml(e.message) + '</span>' +
        '</div>';
    }
    logList.innerHTML = html;
    if (autoScroll) {
      logList.scrollTop = logList.scrollHeight;
    }
  }

  function escapeHtml(s) {
    if (!s) return '';
    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  function addLogEntries(events) {
    for (var i = 0; i < events.length; i++) {
      var e = events[i];
      if (!e.actor_addr) continue;
      // Match if event's actor_addr starts with or contains target
      var a = e.actor_addr.toLowerCase();
      if (a.indexOf(targetAddr.toLowerCase()) === 0 || a === targetAddr.toLowerCase()) {
        allLogs.push(e);
      }
    }
    // Keep bounded
    while (allLogs.length > 500) allLogs.shift();
    renderLogs();
  }

  // Fetch initial logs
  fetch('/api/logs?actor=' + targetAddr + '&limit=200')
    .then(function(r) { return r.json(); })
    .then(function(data) { if (Array.isArray(data)) addLogEntries(data); })
    .catch(function() {});

  logList.addEventListener('scroll', function() {
    autoScroll = (logList.scrollTop + logList.clientHeight >= logList.scrollHeight - 20);
  });

  // ─── SSE ────────────────────────────────────────────────────
  var es = new EventSource('/events');
  window.addEventListener('beforeunload', function() { es.close(); });

  es.addEventListener('stats', function(e) {
    try { updateDetail(JSON.parse(e.data)); } catch(err) { console.error(err); }
  });

  es.addEventListener('activity', function(e) {
    try { addLogEntries(JSON.parse(e.data)); } catch(err) { console.error(err); }
  });

  es.addEventListener('done', function() {
    dot.className = 'status-dot disconnected';
    es.close();
  });

  es.onerror = function() { dot.className = 'status-dot disconnected'; };
  es.onopen = function() { dot.className = 'status-dot'; };
})();
</script>
</body>
</html>
"##;
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
      <a href="/plugin/distribution" class="nav-link">Distribution</a>
      <a href="/plugin/vastai" class="nav-link">Fleet</a>
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
        var actorName = (a.name || '').toLowerCase();
        if (hex.indexOf(filter) < 0 && ('w' + a.worker_id).indexOf(filter) < 0 && msgType.indexOf(filter) < 0 && actorName.indexOf(filter) < 0) {
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
      var addrCell = a.name
        ? '<td><span style="color:#00bcd4;font-weight:600;">' + escapeHtml(a.name) + '</span> <span style="color:#555;font-size:10px;">' + escapeHtml(hex) + '</span>' + (a.poisoned ? ' <span style="color:#f44336;font-size:9px;">DEAD</span>' : '') + '</td>'
        : '<td style="color:#aaa;font-size:11px;">' + escapeHtml(hex) + (a.poisoned ? ' <span style="color:#f44336;font-size:9px;">DEAD</span>' : '') + '</td>';
      tr.innerHTML =
        addrCell +
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

    var detailAddrText = actor.name
      ? '<a href="/actor/' + focusedAddrHex + '" style="color:#00bcd4;text-decoration:none;font-weight:600;">' + escapeHtml(actor.name) + '</a>'
      : '<a href="/actor/' + focusedAddrHex + '" style="color:#aaa;text-decoration:none;">' + escapeHtml(addrToHex(actor.address)) + '</a>';
    document.getElementById('detailAddr').innerHTML = detailAddrText;
    var detailFullText = actor.name
      ? '<span style="color:#00bcd4;font-weight:600;">' + escapeHtml(actor.name) + '</span> <span style="color:#555;font-size:9px;">' + escapeHtml(focusedAddrHex) + '</span>'
      : '<a href="/actor/' + focusedAddrHex + '" style="color:#fff;text-decoration:none;">' + escapeHtml(focusedAddrHex) + '</a>';
    document.getElementById('detailFullAddr').innerHTML = detailFullText;
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
  window.addEventListener('beforeunload', function() { es.close(); });

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
pub const DASHBOARD_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Swactor Runtime Dashboard</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'Menlo', 'Consolas', 'Monaco', monospace; background: #0f1117; color: #e0e0e0; font-size: 13px; }

  .header {
    display: flex; align-items: center; justify-content: space-between;
    padding: 12px 20px; background: #161822; border-bottom: 1px solid #2a2d3e;
  }
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

  .header-left { display: flex; align-items: center; }
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
    grid-template-rows: auto auto;
    gap: 12px; padding: 12px;
  }

  .panel {
    background: #161822; border: 1px solid #2a2d3e; border-radius: 6px;
    padding: 14px; overflow: hidden;
  }
  .panel h2 { font-size: 12px; color: #888; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 10px; }

  .stats-cards {
    display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px;
  }
  .stat-card {
    background: #1c1f2e; border-radius: 4px; padding: 10px; text-align: center;
  }
  .stat-card .value { font-size: 22px; font-weight: 700; color: #fff; }
  .stat-card .label { font-size: 10px; color: #888; text-transform: uppercase; margin-top: 2px; }

  .chart-panel { grid-row: span 2; }
  canvas#workerChart { width: 100%; height: 200px; }

  .worker-cards { display: flex; flex-direction: column; gap: 6px; }
  .worker-card {
    display: flex; align-items: center; gap: 10px;
    background: #1c1f2e; border-radius: 4px; padding: 6px 10px;
  }
  .worker-card .wc-id { font-weight: 700; min-width: 32px; }
  .worker-card .wc-bar-wrap { flex: 1; height: 14px; background: #0f1117; border-radius: 2px; overflow: hidden; display: flex; }
  .worker-card .wc-bar-seg { height: 100%; }
  .worker-card .wc-stats { font-size: 11px; color: #888; min-width: 200px; text-align: right; }
  .worker-card .wc-spark { display: inline-flex; gap: 4px; margin-left: 6px; }

  .actor-table-wrap { max-height: 200px; overflow-y: auto; }
  .actor-table-wrap table { width: 100%; border-collapse: collapse; }
  .actor-table-wrap th, .actor-table-wrap td {
    padding: 4px 8px; text-align: left; border-bottom: 1px solid #2a2d3e; font-size: 12px;
  }
  .actor-table-wrap th { color: #888; font-weight: 500; position: sticky; top: 0; background: #161822; }

  .log-panel {
    grid-column: 1 / -1;
  }
  .log-wrap { max-height: 300px; overflow-y: auto; }
  .log-wrap table { width: 100%; border-collapse: collapse; }
  .log-wrap th, .log-wrap td {
    padding: 3px 8px; text-align: left; border-bottom: 1px solid #1c1f2e; font-size: 11px;
    white-space: nowrap;
  }
  .log-wrap th { color: #888; font-weight: 500; position: sticky; top: 0; background: #161822; }
  .log-wrap td.msg { white-space: normal; word-break: break-all; max-width: 400px; }

  .level-ERROR { color: #f44336; font-weight: 700; }
  .level-WARN { color: #ff9800; }
  .level-INFO { color: #4caf50; }
  .level-DEBUG { color: #2196f3; }
  .level-TRACE { color: #666; }

  .worker-detail-panel { grid-column: 1 / -1; }
  .worker-detail-scroll { max-height: 360px; overflow-y: auto; }
  .worker-group { margin-bottom: 8px; border: 1px solid #2a2d3e; border-radius: 4px; overflow: hidden; }
  .worker-group-header {
    display: flex; align-items: center; justify-content: space-between;
    padding: 7px 12px; background: #1c1f2e; cursor: pointer; user-select: none;
  }
  .worker-group-header:hover { background: #22253a; }
  .worker-group-header .wid { font-weight: 700; }
  .worker-group-header .summary { color: #888; font-size: 11px; }
  .worker-group-header .toggle { color: #555; font-size: 14px; }
  .worker-group-header .sparkline-wrap { display: inline-flex; gap: 8px; margin-left: 12px; }
  .worker-group-header .sparkline-wrap svg { vertical-align: middle; }
  .worker-group-body { display: none; }
  .worker-group.open .worker-group-body { display: block; }
  .worker-group-body table { width: 100%; border-collapse: collapse; }
  .worker-group-body th, .worker-group-body td {
    padding: 3px 10px; text-align: left; border-bottom: 1px solid #1c1f2e; font-size: 11px;
  }
  .worker-group-body th { color: #888; font-weight: 500; background: #161822; }
  .msg-type { color: #4caf50; }
  .msg-type.none { color: #555; font-style: italic; }

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
      <a href="/" class="nav-link active">Overview</a>
      <a href="/actors" class="nav-link">Actors</a>
      <a href="/plugin/distribution" class="nav-link">Distribution</a>
      <a href="/plugin/vastai" class="nav-link">Fleet</a>
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

<div id="warningBanner" style="display:none;padding:8px 20px;background:#1c1f2e;border-bottom:1px solid #2a2d3e;font-size:12px;"></div>
<div class="grid">
  <div class="panel chart-panel">
    <h2>Worker Utilization</h2>
    <div id="workerCards" class="worker-cards"></div>
    <div style="margin-top:6px;font-size:10px;color:#555;">
      <span style="color:#4caf50;">\u25A0</span> processing
      <span style="color:#2196f3;">\u25A0</span> delivery
      <span style="color:#00bcd4;">\u25A0</span> spawns
      <span style="color:#f44336;">\u25A0</span> overhead
    </div>
  </div>

  <div class="panel">
    <h2>Stats</h2>
    <div class="stats-cards">
      <div class="stat-card"><div class="value" id="statActors">0</div><div class="label">Actors</div></div>
      <div class="stat-card"><div class="value" id="statMessages">0</div><div class="label">Messages</div></div>
      <div class="stat-card"><div class="value" id="statWorkers">0</div><div class="label">Workers</div></div>
      <div class="stat-card"><div class="value" id="statMailbox">0</div><div class="label">Mailbox</div></div>
    </div>
  </div>

  <div class="panel">
    <h2>Actors</h2>
    <div class="actor-table-wrap">
      <table>
        <thead><tr><th>Address</th><th>Worker</th></tr></thead>
        <tbody id="actorTableBody"></tbody>
      </table>
    </div>
  </div>

  <div class="panel worker-detail-panel">
    <h2>Worker Details</h2>
    <div class="worker-detail-scroll" id="workerDetailContainer"></div>
  </div>

  <div class="panel log-panel">
    <h2>Activity Log <span id="logCount" style="color:#555;font-weight:400;"></span></h2>
    <div class="log-wrap" id="logWrap">
      <table>
        <thead><tr><th>Seq</th><th>Time</th><th>Level</th><th>Worker</th><th>Message</th><th>Fields</th></tr></thead>
        <tbody id="logTableBody"></tbody>
      </table>
    </div>
  </div>
</div>

<script>
(function() {
  var DASHBOARD_MODE = '__DASHBOARD_MODE__';
  var logRowCount = 0;
  var MAX_LOG_ROWS = 2000;
  var isReplay = (DASHBOARD_MODE === 'replay');
  var lastUptimeMs = null;
  var lastStatsTime = null;
  var workerHistory = {}; // { id: { message_rates: [], mailbox_depths: [] } }

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

  var colors = ['#4caf50','#2196f3','#ff9800','#f44336','#9c27b0','#00bcd4','#ffeb3b','#e91e63'];

  var phaseColors = ['#4caf50', '#2196f3', '#00bcd4', '#f44336'];
  // Group tick phases: processing=2, delivery=1+4, spawns=0+3, overhead=5

  function computePhases(timings) {
    if (!timings || timings.length === 0) return [0.25, 0.25, 0.25, 0.25];
    var sums = [0,0,0,0,0,0];
    var active = 0;
    for (var i = 0; i < timings.length; i++) {
      var t = timings[i];
      if (t.did_work) active++;
      for (var p = 0; p < 6 && p < t.phase_us.length; p++) sums[p] += t.phase_us[p];
    }
    var total = sums.reduce(function(a,b) { return a+b; }, 0);
    if (total === 0) return [0.25, 0.25, 0.25, 0.25];
    var processing = sums[2] / total;
    var delivery = (sums[1] + sums[4]) / total;
    var spawns = (sums[0] + sums[3]) / total;
    var overhead = sums[5] / total;
    var load = timings.length > 0 ? active / timings.length : 0;
    return { fracs: [processing, delivery, spawns, overhead], load: load };
  }

  function renderWorkerCards(data) {
    var container = document.getElementById('workerCards');
    if (!data.workers) return;
    container.innerHTML = '';

    data.workers.forEach(function(w, idx) {
      var timings = data.tick_timings ? data.tick_timings[idx] : null;
      var phases = computePhases(timings);
      var load = phases.load || 0;
      var fracs = phases.fracs || [0.25, 0.25, 0.25, 0.25];

      var card = document.createElement('div');
      card.className = 'worker-card';

      // ID
      var idSpan = document.createElement('span');
      idSpan.className = 'wc-id';
      idSpan.style.color = colors[w.id % colors.length];
      idSpan.textContent = 'W' + w.id;
      card.appendChild(idSpan);

      // Phase bar
      var barWrap = document.createElement('span');
      barWrap.className = 'wc-bar-wrap';
      var filledPct = Math.round(load * 100);
      for (var p = 0; p < 4; p++) {
        var seg = document.createElement('span');
        seg.className = 'wc-bar-seg';
        seg.style.width = (fracs[p] * filledPct) + '%';
        seg.style.background = phaseColors[p];
        barWrap.appendChild(seg);
      }
      card.appendChild(barWrap);

      // Sparklines
      var sparkWrap = document.createElement('span');
      sparkWrap.className = 'wc-spark';
      var wh = workerHistory[w.id];
      if (wh) {
        sparkWrap.innerHTML = renderSparklineSvg(wh.message_rates, 60, 14, '#4caf50');
      }
      card.appendChild(sparkWrap);

      // Stats
      var statsSpan = document.createElement('span');
      statsSpan.className = 'wc-stats';
      statsSpan.textContent = w.num_actors + ' actors  ' +
        w.messages_processed.toLocaleString() + ' msgs  mbox ' + w.mailbox_depth +
        '  ' + Math.round(load * 100) + '%';
      card.appendChild(statsSpan);

      container.appendChild(card);
    });
  }

  function updateStats(data) {
    if (typeof data.uptime_ms === 'number') {
      lastUptimeMs = data.uptime_ms;
      lastStatsTime = Date.now();
    }
    var totalActors = data.actors ? data.actors.length : 0;
    var totalMsgs = data.workers ? data.workers.reduce(function(s, w) { return s + w.messages_processed; }, 0) : 0;
    var totalMailbox = data.workers ? data.workers.reduce(function(s, w) { return s + w.mailbox_depth; }, 0) : 0;

    document.getElementById('statActors').textContent = totalActors;
    document.getElementById('statMessages').textContent = totalMsgs.toLocaleString();
    document.getElementById('statWorkers').textContent = data.num_workers || 0;
    document.getElementById('statMailbox').textContent = totalMailbox;

    renderWorkerCards(data);

    var nameMap = {};
    if (data.actor_details) {
      data.actor_details.forEach(function(a) {
        var h = formatAddr(a.address);
        if (a.name) nameMap[h] = a.name;
      });
    }

    var tbody = document.getElementById('actorTableBody');
    tbody.innerHTML = '';
    if (data.actors) {
      var shown = data.actors.slice(0, 200);
      shown.forEach(function(entry) {
        var addr = entry[0];
        var wid = entry[1];
        var hex = '';
        if (addr && addr.length > 0) {
          var bytes = Array.isArray(addr) ? addr : Object.values(addr);
          for (var j = 0; j < Math.min(8, bytes.length); j++) {
            hex += ('0' + bytes[j].toString(16)).slice(-2);
          }
          hex += '\u2026';
        }
        var tr = document.createElement('tr');
        var addrCell;
        if (nameMap[hex]) {
          addrCell = '<td><a href="/actor/' + hex + '" style="text-decoration:none;"><span style="color:#00bcd4;font-weight:600;">' + escapeHtml(nameMap[hex]) + '</span> <span style="color:#555;font-size:10px;">' + hex + '</span></a></td>';
        } else {
          addrCell = '<td style="color:#aaa;font-size:11px;"><a href="/actor/' + hex + '" style="color:#aaa;text-decoration:none;">' + hex + '</a></td>';
        }
        tr.innerHTML = addrCell + '<td>W' + wid + '</td>';
        tbody.appendChild(tr);
      });
      if (data.actors.length > 200) {
        var tr2 = document.createElement('tr');
        tr2.innerHTML = '<td colspan="2" style="color:#555;">... and ' + (data.actors.length - 200) + ' more</td>';
        tbody.appendChild(tr2);
      }
    }

    updateWorkerDetails(data);
  }

  function formatAddr(addr) {
    var hex = '';
    if (addr && addr.length > 0) {
      var bytes = Array.isArray(addr) ? addr : Object.values(addr);
      for (var j = 0; j < Math.min(8, bytes.length); j++) {
        hex += ('0' + bytes[j].toString(16)).slice(-2);
      }
      hex += '\u2026';
    }
    return hex;
  }

  function shortTypeName(full) {
    if (!full) return '';
    var parts = full.split('::');
    return parts[parts.length - 1];
  }

  function renderSparklineSvg(data, w, h, color) {
    if (!data || data.length < 2) return '';
    var max = Math.max.apply(null, data);
    if (max === 0) max = 1;
    var step = w / (data.length - 1);
    var points = data.map(function(v, i) {
      return (i * step).toFixed(1) + ',' + (h - (v / max) * (h - 2) - 1).toFixed(1);
    }).join(' ');
    return '<svg width="' + w + '" height="' + h + '" style="vertical-align:middle">' +
      '<polyline fill="none" stroke="' + color + '" stroke-width="1.5" points="' + points + '"/></svg>';
  }

  function pushHistorySample(stats) {
    if (!stats.workers) return;
    stats.workers.forEach(function(w) {
      if (!workerHistory[w.id]) {
        workerHistory[w.id] = { message_rates: [], mailbox_depths: [], prev_msgs: w.messages_processed };
      }
      var wh = workerHistory[w.id];
      var rate = w.messages_processed - wh.prev_msgs;
      if (rate < 0) rate = 0;
      wh.prev_msgs = w.messages_processed;
      wh.message_rates.push(rate);
      wh.mailbox_depths.push(w.mailbox_depth);
      if (wh.message_rates.length > 300) { wh.message_rates.shift(); wh.mailbox_depths.shift(); }
    });
  }

  function updateWorkerDetails(data) {
    var container = document.getElementById('workerDetailContainer');
    if (!data.workers) return;

    var groups = {};
    data.workers.forEach(function(w) { groups[w.id] = { info: w, actors: [] }; });
    if (data.actor_details) {
      data.actor_details.forEach(function(a) {
        if (groups[a.worker_id]) groups[a.worker_id].actors.push(a);
      });
    }

    var openState = {};
    container.querySelectorAll('.worker-group').forEach(function(g) {
      openState[g.dataset.wid] = g.classList.contains('open');
    });

    container.innerHTML = '';
    var wids = Object.keys(groups).sort(function(a, b) { return a - b; });

    wids.forEach(function(wid) {
      var g = groups[wid];
      var div = document.createElement('div');
      var isOpen = openState[wid] || false;
      div.className = 'worker-group' + (isOpen ? ' open' : '');
      div.dataset.wid = wid;

      var hdr = document.createElement('div');
      hdr.className = 'worker-group-header';
      var panicHtml = g.info.panics > 0 ? ', <span style="color:#f44336">' + g.info.panics + ' panics</span>' : '';
      var wh = workerHistory[wid];
      var sparkHtml = '';
      if (wh) {
        sparkHtml = '<span class="sparkline-wrap">' +
          renderSparklineSvg(wh.message_rates, 80, 16, '#4caf50') +
          renderSparklineSvg(wh.mailbox_depths, 80, 16, '#2196f3') +
          '</span>';
      }
      hdr.innerHTML =
        '<span class="wid" style="color:' + colors[wid % colors.length] + '">W' + wid + '</span>' +
        sparkHtml +
        '<span class="summary">' + g.actors.length + ' actors, ' +
          g.info.messages_processed.toLocaleString() + ' msgs, mbox ' + g.info.mailbox_depth + panicHtml + '</span>' +
        '<span class="toggle">' + (isOpen ? '\u25BC' : '\u25B6') + '</span>';
      hdr.onclick = function() {
        div.classList.toggle('open');
        hdr.querySelector('.toggle').textContent = div.classList.contains('open') ? '\u25BC' : '\u25B6';
      };
      div.appendChild(hdr);

      var body = document.createElement('div');
      body.className = 'worker-group-body';
      if (g.actors.length === 0) {
        body.innerHTML = '<div style="padding:6px 12px;color:#555;">No actors</div>';
      } else {
        var rows = '';
        g.actors.forEach(function(a) {
          var hex = formatAddr(a.address);
          var hasMsg = !!a.last_msg_type;
          var msgShort = hasMsg ? shortTypeName(a.last_msg_type) : 'none';
          var msgClass = hasMsg ? 'msg-type' : 'msg-type none';
          var title = hasMsg ? ' title="' + escapeHtml(a.last_msg_type) + '"' : '';
          var addrHtml;
          if (a.name) {
            addrHtml = '<td><a href="/actor/' + hex + '" style="text-decoration:none;"><span style="color:#00bcd4;font-weight:600;">' + escapeHtml(a.name) + '</span> <span style="color:#555;font-size:10px;">' + hex + '</span></a></td>';
          } else {
            addrHtml = '<td style="color:#aaa;"><a href="/actor/' + hex + '" style="color:#aaa;text-decoration:none;">' + hex + '</a></td>';
          }
          rows += '<tr>' + addrHtml +
            '<td>' + a.mailbox_depth + '</td>' +
            '<td class="' + msgClass + '"' + title + '>' + escapeHtml(msgShort) + '</td></tr>';
        });
        body.innerHTML = '<table><thead><tr><th>Address</th><th>Mailbox</th><th>Last Message</th></tr></thead><tbody>' + rows + '</tbody></table>';
      }
      div.appendChild(body);
      container.appendChild(div);
    });
  }

  function addLogEvents(events) {
    var tbody = document.getElementById('logTableBody');
    var wrap = document.getElementById('logWrap');
    var wasAtBottom = wrap.scrollTop + wrap.clientHeight >= wrap.scrollHeight - 20;

    events.forEach(function(ev) {
      if (logRowCount >= MAX_LOG_ROWS) {
        tbody.removeChild(tbody.firstChild);
        logRowCount--;
      }
      var tr = document.createElement('tr');
      var ts = new Date(ev.timestamp_ms);
      var timeStr = ts.toLocaleTimeString() + '.' + String(ts.getMilliseconds()).padStart(3, '0');
      var wid = ev.worker_id !== null && ev.worker_id !== undefined ? 'W' + ev.worker_id : '-';
      var fieldsStr = Object.keys(ev.fields).length > 0 ? JSON.stringify(ev.fields) : '';

      tr.innerHTML =
        '<td>' + ev.seq + '</td>' +
        '<td>' + timeStr + '</td>' +
        '<td class="level-' + ev.level + '">' + ev.level + '</td>' +
        '<td>' + wid + '</td>' +
        '<td class="msg">' + escapeHtml(ev.message) + '</td>' +
        '<td style="color:#555;max-width:300px;overflow:hidden;text-overflow:ellipsis;">' + escapeHtml(fieldsStr) + '</td>';
      tbody.appendChild(tr);
      logRowCount++;
    });

    document.getElementById('logCount').textContent = '(' + logRowCount + ')';

    if (wasAtBottom) {
      wrap.scrollTop = wrap.scrollHeight;
    }
  }

  function escapeHtml(s) {
    if (!s) return '';
    return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }

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

  var es = new EventSource('/events');
  window.addEventListener('beforeunload', function() { es.close(); });

  es.addEventListener('stats', function(e) {
    try {
      var data = JSON.parse(e.data);
      pushHistorySample(data);
      updateStats(data);
    } catch(err) { console.error('stats parse error', err); }
  });

  es.addEventListener('history', function(e) {
    try {
      var data = JSON.parse(e.data);
      if (data.workers) {
        data.workers.forEach(function(w) {
          workerHistory[w.id] = {
            message_rates: w.message_rates || [],
            mailbox_depths: w.mailbox_depths || [],
            prev_msgs: 0
          };
        });
      }
    } catch(err) { console.error('history parse error', err); }
  });

  es.addEventListener('warnings', function(e) {
    try {
      var warnings = JSON.parse(e.data);
      var banner = document.getElementById('warningBanner');
      if (warnings.length === 0) {
        banner.style.display = 'none';
        return;
      }
      banner.style.display = 'block';
      var sevColors = {critical:'#f44336',high:'#ff5722',medium:'#ff9800',low:'#888'};
      var html = warnings.map(function(w) {
        var c = sevColors[w.severity] || '#888';
        return '<span style="color:' + c + ';">\u26A0 ' + w.description + '</span>';
      }).join(' &nbsp; ');
      banner.innerHTML = '<span style="color:#ff9800;font-weight:700;">WARNINGS (' + warnings.length + ')</span> &nbsp; ' + html;
    } catch(err) { console.error('warnings parse error', err); }
  });

  es.addEventListener('activity', function(e) {
    try { addLogEvents(JSON.parse(e.data)); } catch(err) { console.error('activity parse error', err); }
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
pub const TOPOLOGY_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Topology — Swactor Dashboard</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'Menlo', 'Consolas', 'Monaco', monospace; background: #0f1117; color: #e0e0e0; font-size: 13px; }

  .header {
    display: flex; align-items: center; justify-content: space-between;
    padding: 12px 20px; background: #161822; border-bottom: 1px solid #2a2d3e;
  }
  .header h1 { font-size: 16px; font-weight: 600; color: #fff; }
  .status-dot {
    width: 10px; height: 10px; border-radius: 50%; background: #4caf50;
    display: inline-block; margin-left: 8px; vertical-align: middle;
  }
  .status-dot.disconnected { background: #f44336; }

  .header-left { display: flex; align-items: center; }
  .nav-links { display: flex; gap: 4px; margin-left: 20px; }
  .nav-link {
    color: #888; text-decoration: none; font-size: 12px;
    padding: 4px 10px; border-radius: 3px;
  }
  .nav-link:hover { color: #e0e0e0; }
  .nav-link.active { color: #fff; background: #2a2d3e; }

  .content { padding: 0; display: flex; flex-direction: column; height: calc(100vh - 49px); }
  canvas#topoCanvas { flex: 1; width: 100%; cursor: grab; }
  canvas#topoCanvas:active { cursor: grabbing; }

  .legend {
    padding: 8px 20px; background: #161822; border-top: 1px solid #2a2d3e;
    font-size: 11px; color: #888;
  }
</style>
</head>
<body>
<div class="header">
  <div class="header-left">
    <h1>Swactor Runtime Dashboard <span id="statusDot" class="status-dot"></span></h1>
    <nav class="nav-links">
      <a href="/" class="nav-link">Overview</a>
      <a href="/actors" class="nav-link">Actors</a>
      <a href="/topology" class="nav-link active">Topology</a>
      <a href="/plugin/distribution" class="nav-link">Distribution</a>
      <a href="/plugin/vastai" class="nav-link">Fleet</a>
    </nav>
  </div>
</div>

<div class="content">
  <canvas id="topoCanvas"></canvas>
  <div class="legend">
    Node size = actor count. Edge thickness = message volume. Green = local sends. Blue = cross-worker sends.
  </div>
</div>

<script>
(function() {
  var canvas = document.getElementById('topoCanvas');
  var ctx = canvas.getContext('2d');
  var dot = document.getElementById('statusDot');

  var colors = ['#4caf50','#2196f3','#ff9800','#f44336','#9c27b0','#00bcd4','#ffeb3b','#e91e63'];
  var nodes = [];
  var edges = [];
  var positions = {};

  function resize() {
    var dpr = window.devicePixelRatio || 1;
    var rect = canvas.getBoundingClientRect();
    canvas.width = rect.width * dpr;
    canvas.height = rect.height * dpr;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  }
  window.addEventListener('resize', resize);
  resize();

  function initPositions() {
    var W = canvas.getBoundingClientRect().width;
    var H = canvas.getBoundingClientRect().height;
    var cx = W / 2, cy = H / 2;
    var r = Math.min(W, H) * 0.3;

    nodes.forEach(function(n, i) {
      if (!positions[n.id]) {
        var angle = (2 * Math.PI * i) / Math.max(1, nodes.length);
        positions[n.id] = {
          x: cx + r * Math.cos(angle),
          y: cy + r * Math.sin(angle),
          vx: 0, vy: 0
        };
      }
    });
  }

  function simulate() {
    var W = canvas.getBoundingClientRect().width;
    var H = canvas.getBoundingClientRect().height;
    var cx = W / 2, cy = H / 2;

    // Repulsion between nodes
    for (var i = 0; i < nodes.length; i++) {
      var pi = positions[nodes[i].id];
      if (!pi) continue;
      for (var j = i + 1; j < nodes.length; j++) {
        var pj = positions[nodes[j].id];
        if (!pj) continue;
        var dx = pi.x - pj.x;
        var dy = pi.y - pj.y;
        var dist = Math.sqrt(dx * dx + dy * dy) || 1;
        var force = 8000 / (dist * dist);
        pi.vx += dx / dist * force;
        pi.vy += dy / dist * force;
        pj.vx -= dx / dist * force;
        pj.vy -= dy / dist * force;
      }
    }

    // Attraction along edges
    edges.forEach(function(e) {
      if (e.source === e.target) return;
      var ps = positions[e.source];
      var pt = positions[e.target];
      if (!ps || !pt) return;
      var dx = pt.x - ps.x;
      var dy = pt.y - ps.y;
      var dist = Math.sqrt(dx * dx + dy * dy) || 1;
      var force = (dist - 150) * 0.01;
      ps.vx += dx / dist * force;
      ps.vy += dy / dist * force;
      pt.vx -= dx / dist * force;
      pt.vy -= dy / dist * force;
    });

    // Gravity toward center
    for (var i = 0; i < nodes.length; i++) {
      var p = positions[nodes[i].id];
      if (!p) continue;
      p.vx += (cx - p.x) * 0.002;
      p.vy += (cy - p.y) * 0.002;
    }

    // Apply velocity with damping
    for (var i = 0; i < nodes.length; i++) {
      var p = positions[nodes[i].id];
      if (!p) continue;
      p.vx *= 0.85;
      p.vy *= 0.85;
      p.x += p.vx;
      p.y += p.vy;
      p.x = Math.max(30, Math.min(W - 30, p.x));
      p.y = Math.max(30, Math.min(H - 30, p.y));
    }
  }

  function draw() {
    var W = canvas.getBoundingClientRect().width;
    var H = canvas.getBoundingClientRect().height;
    ctx.clearRect(0, 0, W, H);

    if (nodes.length === 0) {
      ctx.fillStyle = '#555';
      ctx.font = '14px monospace';
      ctx.textAlign = 'center';
      ctx.fillText('Waiting for topology data...', W / 2, H / 2);
      return;
    }

    // Draw edges
    var maxWeight = Math.max(1, Math.max.apply(null, edges.map(function(e) { return e.weight; })));

    edges.forEach(function(e) {
      var ps = positions[e.source];
      var pt = positions[e.target];
      if (!ps || !pt) return;

      var isSelf = e.source === e.target;
      var thickness = Math.max(1, (e.weight / maxWeight) * 6);
      var color = isSelf ? 'rgba(76, 175, 80, 0.5)' : 'rgba(33, 150, 243, 0.5)';

      if (isSelf) {
        // Self-loop: small arc above the node
        ctx.beginPath();
        ctx.arc(ps.x, ps.y - 25, 15, 0.3, Math.PI - 0.3);
        ctx.strokeStyle = color;
        ctx.lineWidth = thickness;
        ctx.stroke();
      } else {
        ctx.beginPath();
        ctx.moveTo(ps.x, ps.y);
        ctx.lineTo(pt.x, pt.y);
        ctx.strokeStyle = color;
        ctx.lineWidth = thickness;
        ctx.stroke();

        // Arrow
        var angle = Math.atan2(pt.y - ps.y, pt.x - ps.x);
        var headLen = 8;
        var mx = (ps.x + pt.x) / 2;
        var my = (ps.y + pt.y) / 2;
        ctx.beginPath();
        ctx.moveTo(mx, my);
        ctx.lineTo(mx - headLen * Math.cos(angle - 0.3), my - headLen * Math.sin(angle - 0.3));
        ctx.moveTo(mx, my);
        ctx.lineTo(mx - headLen * Math.cos(angle + 0.3), my - headLen * Math.sin(angle + 0.3));
        ctx.strokeStyle = color;
        ctx.lineWidth = 1.5;
        ctx.stroke();

        // Edge label
        ctx.fillStyle = '#666';
        ctx.font = '9px monospace';
        ctx.textAlign = 'center';
        ctx.fillText(e.label, mx, my - 6);
      }
    });

    // Draw nodes
    nodes.forEach(function(n) {
      var p = positions[n.id];
      if (!p) return;
      var r = Math.max(12, 8 + n.actor_count * 2);
      var color = colors[n.group % colors.length];

      ctx.beginPath();
      ctx.arc(p.x, p.y, r, 0, 2 * Math.PI);
      ctx.fillStyle = color;
      ctx.globalAlpha = 0.7;
      ctx.fill();
      ctx.globalAlpha = 1;
      ctx.strokeStyle = '#fff';
      ctx.lineWidth = 1.5;
      ctx.stroke();

      ctx.fillStyle = '#fff';
      ctx.font = 'bold 11px monospace';
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';
      ctx.fillText(n.label, p.x, p.y);

      ctx.fillStyle = '#888';
      ctx.font = '9px monospace';
      ctx.fillText(n.actor_count + ' actors', p.x, p.y + r + 12);
    });
  }

  function updateTopology(data) {
    nodes = data.nodes || [];
    edges = data.edges || [];
    initPositions();
  }

  function tick() {
    simulate();
    draw();
    requestAnimationFrame(tick);
  }
  tick();

  var es = new EventSource('/events');
  window.addEventListener('beforeunload', function() { es.close(); });

  es.addEventListener('topology', function(e) {
    try { updateTopology(JSON.parse(e.data)); } catch(err) { console.error(err); }
  });

  es.addEventListener('done', function() {
    dot.className = 'status-dot disconnected';
    es.close();
  });
  es.onerror = function() { dot.className = 'status-dot disconnected'; };
  es.onopen = function() { dot.className = 'status-dot'; };
})();
</script>
</body>
</html>
"##;
