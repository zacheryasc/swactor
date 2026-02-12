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
      <a href="/distribution" class="nav-link">Distribution</a>
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
  <div class="panel chart-panel">
    <h2>Worker Distribution</h2>
    <canvas id="workerChart"></canvas>
    <div id="workerLegend" style="margin-top:8px;font-size:11px;color:#888;"></div>
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

  var canvas = document.getElementById('workerChart');
  var ctx = canvas.getContext('2d');
  var colors = ['#4caf50','#2196f3','#ff9800','#f44336','#9c27b0','#00bcd4','#ffeb3b','#e91e63'];

  function drawWorkerChart(workers) {
    var dpr = window.devicePixelRatio || 1;
    var rect = canvas.getBoundingClientRect();
    canvas.width = rect.width * dpr;
    canvas.height = rect.height * dpr;
    ctx.scale(dpr, dpr);
    var W = rect.width, H = rect.height;
    ctx.clearRect(0, 0, W, H);

    if (!workers || workers.length === 0) return;

    var maxActors = Math.max(1, Math.max.apply(null, workers.map(function(w) { return w.num_actors; })));
    var barW = Math.max(8, Math.floor((W - 40) / workers.length) - 6);
    var chartH = H - 30;

    workers.forEach(function(w, i) {
      var x = 20 + i * (barW + 6);
      var h = (w.num_actors / maxActors) * (chartH * 0.45);
      ctx.fillStyle = colors[i % colors.length];
      ctx.globalAlpha = 0.8;
      ctx.fillRect(x, chartH * 0.5 - h, barW, h);

      var mh = Math.min(w.mailbox_depth * 2, chartH * 0.4);
      ctx.globalAlpha = 0.4;
      ctx.fillRect(x, chartH * 0.55, barW, mh);

      ctx.globalAlpha = 1;
      ctx.fillStyle = '#888';
      ctx.font = '10px monospace';
      ctx.textAlign = 'center';
      ctx.fillText('W' + w.id, x + barW / 2, H - 2);
    });

    var legend = document.getElementById('workerLegend');
    legend.innerHTML = workers.map(function(w, i) {
      return '<span style="color:' + colors[i % colors.length] + '">W' + w.id +
        ': ' + w.num_actors + ' actors, ' + w.messages_processed + ' msgs, mbox ' + w.mailbox_depth + '</span>';
    }).join(' &nbsp;|&nbsp; ');
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

    drawWorkerChart(data.workers);

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
        tr.innerHTML = '<td style="color:#aaa;font-size:11px;">' + hex + '</td><td>W' + wid + '</td>';
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
      hdr.innerHTML =
        '<span class="wid" style="color:' + colors[wid % colors.length] + '">W' + wid + '</span>' +
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
          rows += '<tr><td style="color:#aaa;">' + hex + '</td>' +
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

  es.addEventListener('stats', function(e) {
    try { updateStats(JSON.parse(e.data)); } catch(err) { console.error('stats parse error', err); }
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
