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
      <a href="/distribution" class="nav-link">Distribution</a>
      <a href="/datastore" class="nav-link">Datastore</a>
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
