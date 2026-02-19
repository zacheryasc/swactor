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
  .objects-table tr { cursor: pointer; }
  .objects-table tr:hover td { background: #1c1f2e; }

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

  /* Upload panel */
  .upload-row {
    display: flex; gap: 10px; align-items: center; flex-wrap: wrap;
  }
  input[type="file"] {
    background: #1c1f2e; color: #e0e0e0; border: 1px solid #2a2d3e;
    border-radius: 4px; padding: 8px; font-family: inherit; font-size: 13px;
    min-height: 38px; cursor: pointer;
  }
  input[type="file"]::file-selector-button {
    background: #1e2030; color: #e0e0e0; border: 1px solid #2a2d3e;
    border-radius: 4px; padding: 4px 10px; font-family: inherit;
    font-size: 12px; cursor: pointer; margin-right: 8px;
  }
  input[type="file"]::file-selector-button:hover { border-color: #6366f1; }
  input[type="text"] {
    background: #1c1f2e; color: #e0e0e0; border: 1px solid #2a2d3e;
    border-radius: 4px; padding: 6px 10px; font-family: inherit;
    font-size: 13px; min-height: 38px; width: 180px;
  }
  input[type="text"]:focus { outline: none; border-color: #6366f1; }

  /* Buttons */
  button {
    background: #1e2030; color: #e0e0e0; border: 1px solid #2a2d3e;
    border-radius: 4px; padding: 6px 14px; font-family: inherit;
    font-size: 13px; cursor: pointer; min-height: 38px;
    transition: border-color 0.15s;
  }
  button:hover { border-color: #6366f1; color: #fff; }
  button:disabled { opacity: 0.4; cursor: default; }
  button.danger:hover { border-color: #f44336; }
  button.primary { background: #6366f1; border-color: #6366f1; color: #fff; font-weight: 600; }
  button.primary:hover { background: #5558e6; }

  /* Actions in table */
  .actions-cell { white-space: nowrap; text-align: right; }
  .actions-cell button { min-height: 28px; padding: 2px 8px; font-size: 11px; }

  /* Origin badge */
  .origin-badge {
    display: inline-block; font-size: 10px; padding: 1px 6px;
    border-radius: 3px; font-weight: 600;
  }
  .origin-badge.local { background: #1b3a2a; color: #4caf50; }
  .origin-badge.remote { background: #1a2a3e; color: #2196f3; }

  /* Toast */
  .toast {
    position: fixed; bottom: 20px; right: 20px; padding: 10px 16px;
    border-radius: 4px; font-size: 12px; z-index: 100; opacity: 0;
    transition: opacity 0.3s; pointer-events: none;
  }
  .toast.show { opacity: 1; }
  .toast.success { background: #4caf50; color: #fff; }
  .toast.error { background: #f44336; color: #fff; }

  /* Modal */
  .modal-overlay {
    display: none; position: fixed; inset: 0;
    background: rgba(0,0,0,0.6); z-index: 50;
    align-items: center; justify-content: center;
  }
  .modal-overlay.open { display: flex; }
  .modal {
    background: #161822; border: 1px solid #2a2d3e; border-radius: 6px;
    padding: 20px; width: 90%; max-width: 560px; max-height: 80vh;
    overflow-y: auto;
  }
  .modal h2 { font-size: 14px; color: #fff; margin-bottom: 16px; text-transform: none; letter-spacing: 0; }
  .modal-close {
    float: right; background: none; border: none; color: #888;
    font-size: 18px; cursor: pointer; min-height: auto; padding: 0;
  }
  .modal-close:hover { color: #fff; border: none; }
  .detail-row { display: flex; margin-bottom: 8px; }
  .detail-label { color: #888; width: 110px; flex-shrink: 0; font-size: 11px; text-transform: uppercase; padding-top: 2px; }
  .detail-value { color: #e0e0e0; word-break: break-all; font-size: 13px; }
  .chunk-list { margin-top: 8px; }
  .chunk-item { color: #888; font-size: 11px; padding: 2px 0; }

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
    <button id="startBtn" onclick="startDatastore()" style="display:none" class="primary">Start Datastore</button>
    <button id="stopBtn" onclick="stopDatastore()" style="display:none" class="danger">Stop</button>
  </div>
</div>

<div class="grid">
  <!-- Upload panel -->
  <div class="panel full-width" id="uploadPanel" style="display:none">
    <h2>Upload</h2>
    <div class="upload-row">
      <input type="file" id="fileInput" />
      <input type="text" id="nameInput" placeholder="name (optional)" />
      <button class="primary" id="uploadBtn" onclick="upload()">Upload</button>
    </div>
  </div>

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
    <div class="objects-table" id="objectsTableWrap">
      <table>
        <thead><tr><th>Hash</th><th>Name</th><th>Origin</th><th>Size</th><th style="text-align:right">Actions</th></tr></thead>
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

<!-- Detail modal -->
<div class="modal-overlay" id="modal" onclick="if(event.target===this)closeModal()">
  <div class="modal">
    <button class="modal-close" onclick="closeModal()">&times;</button>
    <h2>Object Detail</h2>
    <div id="modalBody"></div>
  </div>
</div>

<div class="toast" id="toast"></div>

<script>
(function() {
  var DASHBOARD_MODE = '__DASHBOARD_MODE__';
  var dot = document.getElementById('statusDot');
  var dsRunning = false;
  var currentNodeId = '';

  function $(id) { return document.getElementById(id); }

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

  function escAttr(s) {
    if (!s) return '';
    return s.replace(/\\/g,'\\\\').replace(/'/g,"\\'").replace(/"/g,'&quot;');
  }

  // Toast notification
  window.toast = function(msg, type) {
    var t = $('toast');
    t.textContent = msg;
    t.className = 'toast show ' + type;
    setTimeout(function() { t.className = 'toast'; }, 2500);
  };

  function detailRow(label, value) {
    return '<div class="detail-row"><div class="detail-label">' + label +
           '</div><div class="detail-value">' + escapeHtml(String(value)) + '</div></div>';
  }

  // Update lifecycle buttons
  function updateLifecycleUI(running) {
    dsRunning = running;
    $('startBtn').style.display = running ? 'none' : 'inline-block';
    $('stopBtn').style.display = running ? 'inline-block' : 'none';
    $('uploadPanel').style.display = running ? 'block' : 'none';
  }

  function updateFromSnapshot(data) {
    // Handle envelope format: {is_running, snapshot}
    var running = data.is_running;
    var snap = data.snapshot;

    updateLifecycleUI(running);

    if (!snap) return;

    // Node label
    if (snap.node_id) {
      currentNodeId = snap.node_id;
      $('nodeLabel').textContent = 'Node: ' + snap.node_id.substring(0, 16) + '\u2026';
    }

    // Stat cards
    $('statObjects').textContent = snap.object_count;
    $('statSize').textContent = formatBytes(snap.total_bytes);
    $('statPuts').textContent = snap.put_ops;
    $('statGets').textContent = snap.get_ops;
    $('statDeletes').textContent = snap.delete_ops;

    // Event timeline
    var timeline = $('eventTimeline');
    var wasAtBottom = timeline.scrollTop + timeline.clientHeight >= timeline.scrollHeight - 20;
    timeline.innerHTML = '';
    $('eventCount').textContent = '(' + snap.recent_events.length + ')';

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
    var tbody = $('objectsBody');
    tbody.innerHTML = '';
    $('objectCount').textContent = '(' + snap.objects.length + ')';

    for (var i = 0; i < snap.objects.length; i++) {
      var obj = snap.objects[i];
      var tr = document.createElement('tr');
      var nodeHex = obj.node_id || currentNodeId || '';
      var isLocal = !obj.node_id || obj.node_id === currentNodeId;
      var badgeClass = isLocal ? 'local' : 'remote';
      var badgeText = isLocal ? 'local' : (nodeHex ? nodeHex.substring(0, 8) : 'remote');
      var h = obj.hash || obj.content_hash || '';
      var shortHash = h.substring(0, 16);
      var name = obj.name || '\u2014';
      tr.setAttribute('onclick', "showDetail('" + escAttr(h) + "')");
      tr.innerHTML =
        '<td style="color:#6366f1;font-size:11px;" title="' + escapeHtml(h) + '">' + shortHash + '\u2026</td>' +
        '<td>' + escapeHtml(name) + '</td>' +
        '<td><span class="origin-badge ' + badgeClass + '">' + escapeHtml(badgeText) + '</span></td>' +
        '<td style="color:#888;">' + formatBytes(obj.size_bytes) + '</td>' +
        '<td class="actions-cell">' +
          '<button onclick="event.stopPropagation();download(\'' + escAttr(h) + '\',\'' + escAttr(obj.name || shortHash) + '\')">download</button> ' +
          '<button class="danger" onclick="event.stopPropagation();del(\'' + escAttr(h) + '\')">delete</button>' +
        '</td>';
      tbody.appendChild(tr);
    }

    // Transfers
    var panel = $('transfersPanel');
    var list = $('transfersList');

    if (snap.active_transfers.length === 0) {
      panel.className = 'panel full-width transfers-section';
    } else {
      panel.className = 'panel full-width transfers-section visible';
      list.innerHTML = '';

      for (var i = 0; i < snap.active_transfers.length; i++) {
        var t = snap.active_transfers[i];
        var pct = t.chunks_total > 0 ? Math.round((t.chunks_received / t.chunks_total) * 100) : 0;
        var trow = document.createElement('div');
        trow.className = 'transfer-row';
        trow.innerHTML =
          '<span class="transfer-hash">' + t.hash.substring(0, 16) + '\u2026</span>' +
          '<div class="transfer-bar"><div class="transfer-fill" style="width:' + pct + '%"></div></div>' +
          '<span class="transfer-label">' + t.chunks_received + ' / ' + t.chunks_total + '</span>';
        list.appendChild(trow);
      }
    }
  }

  // ── CRUD operations ──────────────────────────────────────────────────

  window.upload = function() {
    var file = $('fileInput').files[0];
    if (!file) { toast('select a file first', 'error'); return; }
    var name = $('nameInput').value.trim();
    var btn = $('uploadBtn');
    btn.disabled = true;
    btn.textContent = 'uploading...';
    var url = '/api/datastore/put';
    if (name) url += '?name=' + encodeURIComponent(name);
    fetch(url, { method: 'POST', body: file })
      .then(function(r) {
        if (!r.ok) return r.json().then(function(j) { throw new Error(j.error || r.statusText); });
        return r.json();
      })
      .then(function(j) {
        toast('uploaded ' + j.content_hash.substring(0, 12), 'success');
        $('fileInput').value = '';
        $('nameInput').value = '';
      })
      .catch(function(e) { toast('upload failed: ' + e.message, 'error'); })
      .finally(function() { btn.disabled = false; btn.textContent = 'Upload'; });
  };

  window.download = function(hash, filename) {
    fetch('/api/datastore/data?hash=' + hash)
      .then(function(r) {
        if (!r.ok) throw new Error('not found');
        return r.blob();
      })
      .then(function(blob) {
        var a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = filename;
        a.click();
        URL.revokeObjectURL(a.href);
      })
      .catch(function(e) { toast('download failed: ' + e.message, 'error'); });
  };

  window.del = function(hash) {
    if (!confirm('Delete ' + hash.substring(0, 16) + '?')) return;
    fetch('/api/datastore/delete?hash=' + hash, { method: 'POST' })
      .then(function(r) {
        if (!r.ok) return r.json().then(function(j) { throw new Error(j.error || r.statusText); });
        toast('deleted', 'success');
      })
      .catch(function(e) { toast('delete failed: ' + e.message, 'error'); });
  };

  window.showDetail = function(hash) {
    fetch('/api/datastore/get?hash=' + hash)
      .then(function(r) {
        if (!r.ok) throw new Error('not found');
        return r.json();
      })
      .then(function(j) {
        var e = j.entry;
        var m = j.manifest;
        var html = '';
        html += detailRow('Hash', e.content_hash);
        html += detailRow('Name', e.name || '\u2014');
        html += detailRow('Size', formatBytes(e.size_bytes));
        html += detailRow('Node', e.node_id ? e.node_id.substring(0, 16) + '...' : '\u2014');
        if (e.tags && Object.keys(e.tags).length > 0) {
          html += detailRow('Tags', Object.entries(e.tags).map(function(kv) { return kv[0] + '=' + kv[1]; }).join(', '));
        }
        html += detailRow('Chunks', m.chunks.length + ' (' + formatBytes(m.chunk_size) + ' each)');
        if (m.chunks.length > 0) {
          html += '<div class="chunk-list">';
          for (var i = 0; i < m.chunks.length; i++) {
            var c = m.chunks[i];
            html += '<div class="chunk-item">#' + i + ' ' + c.hash.substring(0, 16) + ' (' + formatBytes(c.size) + ')</div>';
          }
          html += '</div>';
        }
        $('modalBody').innerHTML = html;
        $('modal').classList.add('open');
      })
      .catch(function(e) { toast('failed to load detail', 'error'); });
  };

  window.closeModal = function() { $('modal').classList.remove('open'); };
  document.addEventListener('keydown', function(e) { if (e.key === 'Escape') closeModal(); });

  // ── Lifecycle ────────────────────────────────────────────────────────

  window.startDatastore = function() {
    $('startBtn').disabled = true;
    fetch('/api/datastore/start', { method: 'POST' })
      .then(function(r) {
        if (!r.ok) return r.json().then(function(j) { throw new Error(j.error || r.statusText); });
        return r.json();
      })
      .then(function() {
        toast('datastore started', 'success');
        updateLifecycleUI(true);
      })
      .catch(function(e) { toast('start failed: ' + e.message, 'error'); })
      .finally(function() { $('startBtn').disabled = false; });
  };

  window.stopDatastore = function() {
    if (!confirm('Stop the datastore? In-memory data will be lost.')) return;
    $('stopBtn').disabled = true;
    fetch('/api/datastore/shutdown', { method: 'POST' })
      .then(function(r) {
        if (!r.ok) return r.json().then(function(j) { throw new Error(j.error || r.statusText); });
        return r.json();
      })
      .then(function() {
        toast('datastore stopped', 'success');
        updateLifecycleUI(false);
        $('objectsBody').innerHTML = '';
        $('eventTimeline').innerHTML = '';
        $('statObjects').textContent = '0';
        $('statSize').textContent = '0 B';
        $('statPuts').textContent = '0';
        $('statGets').textContent = '0';
        $('statDeletes').textContent = '0';
      })
      .catch(function(e) { toast('stop failed: ' + e.message, 'error'); })
      .finally(function() { $('stopBtn').disabled = false; });
  };

  // ── SSE connection ───────────────────────────────────────────────────

  var es = new EventSource('/events');

  es.addEventListener('datastore', function(e) {
    try {
      var data = JSON.parse(e.data);
      updateFromSnapshot(data);
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
