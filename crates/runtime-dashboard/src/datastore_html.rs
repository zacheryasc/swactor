pub const DATASTORE_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Swactor Runtime – Datastore</title>
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

  .grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 12px; padding: 12px;
  }

  .panel {
    background: #161822; border: 1px solid #2a2d3e; border-radius: 6px;
    padding: 14px; overflow: hidden;
  }
  .panel h2 { font-size: 12px; color: #888; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 10px; }

  .full-width { grid-column: 1 / -1; }

  .stats-cards {
    display: grid; grid-template-columns: repeat(5, 1fr); gap: 10px;
  }
  .stat-card {
    background: #1c1f2e; border-radius: 4px; padding: 10px; text-align: center;
  }
  .stat-card .value { font-size: 22px; font-weight: 700; color: #fff; }
  .stat-card .label { font-size: 10px; color: #888; text-transform: uppercase; margin-top: 2px; }

  .event-timeline {
    max-height: 400px; overflow-y: auto;
  }
  .event-row {
    display: flex; gap: 8px; padding: 4px 0; border-bottom: 1px solid #1c1f2e;
    font-size: 11px; align-items: center;
  }
  .event-time { color: #555; min-width: 70px; }
  .event-kind {
    min-width: 48px; font-weight: 700; text-transform: uppercase; font-size: 10px;
    padding: 1px 6px; border-radius: 3px; text-align: center;
  }
  .event-kind.put { background: #1b3a2a; color: #4caf50; }
  .event-kind.get { background: #1a2a3e; color: #2196f3; }
  .event-kind.delete { background: #3a1a1a; color: #f44336; }
  .event-hash { color: #aaa; font-size: 10px; min-width: 120px; }
  .event-name { color: #e0e0e0; flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .event-size { color: #888; min-width: 70px; text-align: right; }

  .objects-table { max-height: 400px; overflow-y: auto; }
  .objects-table table { width: 100%; border-collapse: collapse; }
  .objects-table th, .objects-table td {
    padding: 4px 8px; text-align: left; border-bottom: 1px solid #1c1f2e; font-size: 11px;
    white-space: nowrap;
  }
  .objects-table th { color: #888; font-weight: 500; position: sticky; top: 0; background: #161822; }

  .transfers-section { display: none; }
  .transfers-section.visible { display: block; }
  .transfer-row {
    display: flex; align-items: center; gap: 10px; margin-bottom: 8px;
  }
  .transfer-hash { color: #aaa; font-size: 10px; min-width: 100px; }
  .transfer-bar {
    flex: 1; height: 16px; background: #1c1f2e; border-radius: 3px; overflow: hidden;
  }
  .transfer-fill {
    height: 100%; background: #6366f1; border-radius: 3px;
    transition: width 0.3s ease;
  }
  .transfer-label { color: #888; font-size: 11px; min-width: 80px; text-align: right; }

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
      <a href="/distribution" class="nav-link">Distribution</a>
      <a href="/datastore" class="nav-link active">Datastore</a>
    </nav>
  </div>
  <div class="header-right">
    <span id="nodeLabel" style="color:#888;font-size:12px;">Waiting for data...</span>
  </div>
</div>

<div class="grid">
  <!-- Stat cards -->
  <div class="panel full-width">
    <h2>Datastore Stats</h2>
    <div class="stats-cards">
      <div class="stat-card"><div class="value" id="statObjects">0</div><div class="label">Objects</div></div>
      <div class="stat-card"><div class="value" id="statSize">0</div><div class="label">Total Size</div></div>
      <div class="stat-card"><div class="value" id="statPuts">0</div><div class="label">Puts</div></div>
      <div class="stat-card"><div class="value" id="statGets">0</div><div class="label">Gets</div></div>
      <div class="stat-card"><div class="value" id="statDeletes">0</div><div class="label">Deletes</div></div>
    </div>
  </div>

  <!-- Event timeline -->
  <div class="panel">
    <h2>Event Timeline <span id="eventCount" style="color:#555;font-weight:400;"></span></h2>
    <div class="event-timeline" id="eventTimeline"></div>
  </div>

  <!-- Objects table -->
  <div class="panel">
    <h2>Objects <span id="objectCount" style="color:#555;font-weight:400;"></span></h2>
    <div class="objects-table">
      <table>
        <thead><tr><th>Hash</th><th>Name</th><th>Size</th></tr></thead>
        <tbody id="objectsBody"></tbody>
      </table>
    </div>
  </div>

  <!-- Transfers -->
  <div class="panel full-width transfers-section" id="transfersPanel">
    <h2>Active Transfers</h2>
    <div id="transfersList"></div>
  </div>
</div>

<script>
(function() {
  var DASHBOARD_MODE = '__DASHBOARD_MODE__';
  var dot = document.getElementById('statusDot');

  function formatBytes(b) {
    if (b === 0) return '0 B';
    var units = ['B', 'KB', 'MB', 'GB', 'TB'];
    var i = Math.floor(Math.log(b) / Math.log(1024));
    if (i >= units.length) i = units.length - 1;
    return (b / Math.pow(1024, i)).toFixed(i > 0 ? 1 : 0) + ' ' + units[i];
  }

  function formatTime(ms) {
    var d = new Date(ms);
    return ('0' + d.getHours()).slice(-2) + ':' +
           ('0' + d.getMinutes()).slice(-2) + ':' +
           ('0' + d.getSeconds()).slice(-2);
  }

  function escapeHtml(s) {
    if (!s) return '';
    return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }

  function updateFromSnapshot(snap) {
    // Node label
    if (snap.node_id) {
      document.getElementById('nodeLabel').textContent = 'Node: ' + snap.node_id.substring(0, 16) + '\u2026';
    }

    // Stat cards
    document.getElementById('statObjects').textContent = snap.object_count;
    document.getElementById('statSize').textContent = formatBytes(snap.total_bytes);
    document.getElementById('statPuts').textContent = snap.put_ops;
    document.getElementById('statGets').textContent = snap.get_ops;
    document.getElementById('statDeletes').textContent = snap.delete_ops;

    // Event timeline
    var timeline = document.getElementById('eventTimeline');
    var wasAtBottom = timeline.scrollTop + timeline.clientHeight >= timeline.scrollHeight - 20;
    timeline.innerHTML = '';
    document.getElementById('eventCount').textContent = '(' + snap.recent_events.length + ')';

    for (var i = snap.recent_events.length - 1; i >= 0; i--) {
      var ev = snap.recent_events[i];
      var row = document.createElement('div');
      row.className = 'event-row';
      row.innerHTML =
        '<span class="event-time">' + formatTime(ev.timestamp_ms) + '</span>' +
        '<span class="event-kind ' + ev.kind + '">' + ev.kind + '</span>' +
        '<span class="event-hash">' + ev.hash.substring(0, 16) + '\u2026</span>' +
        '<span class="event-name">' + escapeHtml(ev.name || '') + '</span>' +
        '<span class="event-size">' + (ev.size_bytes > 0 ? formatBytes(ev.size_bytes) : '') + '</span>';
      timeline.appendChild(row);
    }

    if (wasAtBottom) {
      timeline.scrollTop = timeline.scrollHeight;
    }

    // Objects table
    var tbody = document.getElementById('objectsBody');
    tbody.innerHTML = '';
    document.getElementById('objectCount').textContent = '(' + snap.objects.length + ')';

    for (var i = 0; i < snap.objects.length; i++) {
      var obj = snap.objects[i];
      var tr = document.createElement('tr');
      tr.innerHTML =
        '<td style="color:#aaa;font-size:10px;">' + obj.hash.substring(0, 16) + '\u2026</td>' +
        '<td>' + escapeHtml(obj.name || '\u2014') + '</td>' +
        '<td style="color:#888;">' + formatBytes(obj.size_bytes) + '</td>';
      tbody.appendChild(tr);
    }

    // Transfers
    var panel = document.getElementById('transfersPanel');
    var list = document.getElementById('transfersList');

    if (snap.active_transfers.length === 0) {
      panel.className = 'panel full-width transfers-section';
    } else {
      panel.className = 'panel full-width transfers-section visible';
      list.innerHTML = '';

      for (var i = 0; i < snap.active_transfers.length; i++) {
        var t = snap.active_transfers[i];
        var pct = t.chunks_total > 0 ? Math.round((t.chunks_received / t.chunks_total) * 100) : 0;
        var row = document.createElement('div');
        row.className = 'transfer-row';
        row.innerHTML =
          '<span class="transfer-hash">' + t.hash.substring(0, 16) + '\u2026</span>' +
          '<div class="transfer-bar"><div class="transfer-fill" style="width:' + pct + '%"></div></div>' +
          '<span class="transfer-label">' + t.chunks_received + ' / ' + t.chunks_total + '</span>';
        list.appendChild(row);
      }
    }
  }

  // SSE connection
  var es = new EventSource('/events');

  es.addEventListener('datastore', function(e) {
    try {
      var snap = JSON.parse(e.data);
      updateFromSnapshot(snap);
    } catch(err) { console.error('datastore parse error', err); }
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
})();
</script>
</body>
</html>
"##;
