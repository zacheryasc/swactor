pub const POOL_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Swactor Runtime – Pool</title>
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

  .members-table { max-height: 400px; overflow-y: auto; }
  .members-table table { width: 100%; border-collapse: collapse; }
  .members-table th, .members-table td {
    padding: 4px 8px; text-align: left; border-bottom: 1px solid #1c1f2e; font-size: 11px;
    white-space: nowrap;
  }
  .members-table th { color: #888; font-weight: 500; position: sticky; top: 0; background: #161822; }

  .content-table { max-height: 400px; overflow-y: auto; }
  .content-table table { width: 100%; border-collapse: collapse; }
  .content-table th, .content-table td {
    padding: 4px 8px; text-align: left; border-bottom: 1px solid #1c1f2e; font-size: 11px;
    white-space: nowrap;
  }
  .content-table th { color: #888; font-weight: 500; position: sticky; top: 0; background: #161822; }

  /* Capacity bars */
  .cap-bar-wrap { margin-bottom: 6px; }
  .cap-bar-label {
    display: flex; justify-content: space-between; font-size: 10px; color: #888; margin-bottom: 2px;
  }
  .cap-bar {
    height: 14px; background: #1c1f2e; border-radius: 3px; overflow: hidden;
  }
  .cap-bar-fill {
    height: 100%; background: #6366f1; border-radius: 3px;
    transition: width 0.3s ease;
  }
  .cap-bar-fill.warn { background: #ff9800; }
  .cap-bar-fill.crit { background: #f44336; }

  /* Buttons */
  button {
    background: #1e2030; color: #e0e0e0; border: 1px solid #2a2d3e;
    border-radius: 4px; padding: 6px 14px; font-family: inherit;
    font-size: 13px; cursor: pointer; min-height: 38px;
    transition: border-color 0.15s;
  }
  button:hover { border-color: #6366f1; color: #fff; }
  button:disabled { opacity: 0.4; cursor: default; }
  button.primary { background: #6366f1; border-color: #6366f1; color: #fff; font-weight: 600; }
  button.primary:hover { background: #5558e6; }
  button.danger:hover { border-color: #f44336; }

  /* No-pool message */
  .no-pool {
    display: flex; align-items: center; justify-content: center;
    height: calc(100vh - 49px); color: #555; font-size: 16px;
    flex-direction: column; gap: 8px;
  }

  /* Toast */
  .toast {
    position: fixed; bottom: 20px; right: 20px; padding: 10px 16px;
    border-radius: 4px; font-size: 12px; z-index: 100; opacity: 0;
    transition: opacity 0.3s; pointer-events: none;
  }
  .toast.show { opacity: 1; }
  .toast.success { background: #4caf50; color: #fff; }
  .toast.error { background: #f44336; color: #fff; }

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
      <a href="/datastore" class="nav-link">Datastore</a>
      <a href="/pool" class="nav-link active">Pool</a>
    </nav>
  </div>
  <div class="header-right">
    <button id="joinBtn" class="primary" onclick="joinPool()" style="display:none">Join Pool</button>
    <button id="leaveBtn" class="danger" onclick="leavePool()" style="display:none">Leave Pool</button>
  </div>
</div>

<div id="noPool" class="no-pool" style="display:none">
  <div>No pool configured</div>
  <div style="font-size:12px;color:#444;">Start the node with --pool-name to enable pooled storage</div>
</div>

<div id="poolContent" style="display:none">
  <div class="grid">
    <!-- Summary cards -->
    <div class="panel full-width">
      <h2>Pool Summary</h2>
      <div class="stats-cards">
        <div class="stat-card"><div class="value" id="statPoolName">-</div><div class="label">Pool Name</div></div>
        <div class="stat-card"><div class="value" id="statMembers">0</div><div class="label">Members</div></div>
        <div class="stat-card"><div class="value" id="statContent">0</div><div class="label">Content Items</div></div>
        <div class="stat-card"><div class="value" id="statTotal">0</div><div class="label">Total Capacity</div></div>
        <div class="stat-card"><div class="value" id="statUsed">0</div><div class="label">Used</div></div>
      </div>
    </div>

    <!-- Capacity chart -->
    <div class="panel">
      <h2>Capacity by Node</h2>
      <div id="capacityChart"></div>
      <div id="capEmpty" style="color:#555;font-size:11px;">No members yet</div>
    </div>

    <!-- Members table -->
    <div class="panel">
      <h2>Members <span id="memberCount" style="color:#555;font-weight:400;"></span></h2>
      <div class="members-table">
        <table>
          <thead><tr><th>Node ID</th><th>State</th><th>Total</th><th>Used</th><th>Free</th></tr></thead>
          <tbody id="membersBody"></tbody>
        </table>
      </div>
    </div>

    <!-- Content locations -->
    <div class="panel full-width">
      <h2>Content Location Map <span id="contentCount" style="color:#555;font-weight:400;"></span></h2>
      <div class="content-table">
        <table>
          <thead><tr><th>Content Hash</th><th>Replicas</th><th>Nodes</th></tr></thead>
          <tbody id="contentBody"></tbody>
        </table>
      </div>
    </div>

    <!-- ACL panel -->
    <div class="panel full-width">
      <h2>Access Control <span id="aclMode" style="color:#555;font-weight:400;"></span></h2>
      <div id="aclOpen" style="color:#555;font-size:11px;">Open mode &mdash; any node may join the pool</div>
      <div id="aclTable" class="content-table" style="display:none">
        <table>
          <thead><tr><th>Node ID</th><th>Granted By</th><th>Status</th></tr></thead>
          <tbody id="aclBody"></tbody>
        </table>
      </div>
    </div>
  </div>
</div>

<div class="toast" id="toast"></div>

<script>
(function() {
  var dot = document.getElementById('statusDot');
  var poolConfigured = false;

  function $(id) { return document.getElementById(id); }

  function formatBytes(b) {
    if (b === 0) return '0 B';
    var units = ['B', 'KB', 'MB', 'GB', 'TB'];
    var i = Math.floor(Math.log(b) / Math.log(1024));
    if (i >= units.length) i = units.length - 1;
    return (b / Math.pow(1024, i)).toFixed(i > 0 ? 1 : 0) + ' ' + units[i];
  }

  function escapeHtml(s) {
    if (!s) return '';
    return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }

  window.toast = function(msg, type) {
    var t = $('toast');
    t.textContent = msg;
    t.className = 'toast show ' + type;
    setTimeout(function() { t.className = 'toast'; }, 2500);
  };

  function updatePool(snap) {
    if (!snap) {
      if (!poolConfigured) {
        $('noPool').style.display = 'flex';
        $('poolContent').style.display = 'none';
        $('joinBtn').style.display = 'none';
        $('leaveBtn').style.display = 'none';
      }
      return;
    }

    poolConfigured = true;
    $('noPool').style.display = 'none';
    $('poolContent').style.display = 'block';
    $('joinBtn').style.display = 'inline-block';
    $('leaveBtn').style.display = 'inline-block';

    // Summary cards
    $('statPoolName').textContent = snap.pool_name || '-';
    $('statPoolName').style.fontSize = '14px';
    $('statMembers').textContent = snap.member_count;
    $('statContent').textContent = snap.content_count;
    $('statTotal').textContent = formatBytes(snap.total_bytes);
    $('statUsed').textContent = formatBytes(snap.used_bytes);

    // Members table
    var mbody = $('membersBody');
    mbody.innerHTML = '';
    $('memberCount').textContent = '(' + snap.members.length + ')';

    for (var i = 0; i < snap.members.length; i++) {
      var m = snap.members[i];
      var free = m.total_bytes - m.used_bytes;
      var tr = document.createElement('tr');
      tr.innerHTML =
        '<td style="color:#6366f1;font-size:11px;" title="' + escapeHtml(m.node_id) + '">' + m.node_id.substring(0, 16) + '\u2026</td>' +
        '<td style="color:#4caf50;">' + escapeHtml(m.state) + '</td>' +
        '<td style="color:#888;">' + formatBytes(m.total_bytes) + '</td>' +
        '<td style="color:#888;">' + formatBytes(m.used_bytes) + '</td>' +
        '<td style="color:#888;">' + formatBytes(free) + '</td>';
      mbody.appendChild(tr);
    }

    // Capacity chart
    var chart = $('capacityChart');
    chart.innerHTML = '';
    var capEmpty = $('capEmpty');

    if (snap.members.length === 0) {
      capEmpty.style.display = 'block';
    } else {
      capEmpty.style.display = 'none';
      for (var i = 0; i < snap.members.length; i++) {
        var m = snap.members[i];
        var pct = m.total_bytes > 0 ? Math.round((m.used_bytes / m.total_bytes) * 100) : 0;
        var fillClass = 'cap-bar-fill';
        if (pct > 90) fillClass += ' crit';
        else if (pct > 70) fillClass += ' warn';

        var wrap = document.createElement('div');
        wrap.className = 'cap-bar-wrap';
        wrap.innerHTML =
          '<div class="cap-bar-label"><span>' + m.node_id.substring(0, 12) + '\u2026</span><span>' + pct + '% (' + formatBytes(m.used_bytes) + ' / ' + formatBytes(m.total_bytes) + ')</span></div>' +
          '<div class="cap-bar"><div class="' + fillClass + '" style="width:' + pct + '%"></div></div>';
        chart.appendChild(wrap);
      }
    }

    // Content locations
    var cbody = $('contentBody');
    cbody.innerHTML = '';
    $('contentCount').textContent = '(' + snap.content_locations.length + ')';

    for (var i = 0; i < snap.content_locations.length; i++) {
      var cl = snap.content_locations[i];
      var nodeList = cl.nodes.map(function(n) { return n.substring(0, 12) + '\u2026'; }).join(', ');
      var tr = document.createElement('tr');
      tr.innerHTML =
        '<td style="color:#6366f1;font-size:11px;" title="' + escapeHtml(cl.content_hash) + '">' + cl.content_hash.substring(0, 16) + '\u2026</td>' +
        '<td>' + cl.replica_count + '</td>' +
        '<td style="color:#888;font-size:10px;">' + escapeHtml(nodeList) + '</td>';
      cbody.appendChild(tr);
    }

    // ACL panel
    var aclMode = snap.acl_mode || 'open';
    var aclEntries = snap.acl || [];
    $('aclMode').textContent = '(' + aclMode + ')';

    if (aclMode === 'open' || aclEntries.length === 0) {
      $('aclOpen').style.display = 'block';
      $('aclTable').style.display = 'none';
    } else {
      $('aclOpen').style.display = 'none';
      $('aclTable').style.display = 'block';
      var abody = $('aclBody');
      abody.innerHTML = '';
      for (var i = 0; i < aclEntries.length; i++) {
        var a = aclEntries[i];
        var status = a.revoked ? 'revoked' : 'granted';
        var statusColor = a.revoked ? '#f44336' : '#4caf50';
        var tr = document.createElement('tr');
        tr.innerHTML =
          '<td style="color:#6366f1;font-size:11px;" title="' + escapeHtml(a.node_id) + '">' + a.node_id.substring(0, 16) + '\u2026</td>' +
          '<td style="color:#888;font-size:10px;" title="' + escapeHtml(a.granted_by) + '">' + a.granted_by.substring(0, 16) + '\u2026</td>' +
          '<td style="color:' + statusColor + ';font-size:10px;">' + status + '</td>';
        abody.appendChild(tr);
      }
    }
  }

  // Join/Leave actions
  window.joinPool = function() {
    $('joinBtn').disabled = true;
    fetch('/api/pool/join', { method: 'POST' })
      .then(function(r) {
        if (!r.ok) return r.json().then(function(j) { throw new Error(j.error || r.statusText); });
        return r.json();
      })
      .then(function() { toast('Joined pool', 'success'); })
      .catch(function(e) { toast('Join failed: ' + e.message, 'error'); })
      .finally(function() { $('joinBtn').disabled = false; });
  };

  window.leavePool = function() {
    $('leaveBtn').disabled = true;
    fetch('/api/pool/leave', { method: 'POST' })
      .then(function(r) {
        if (!r.ok) return r.json().then(function(j) { throw new Error(j.error || r.statusText); });
        return r.json();
      })
      .then(function() { toast('Left pool', 'success'); })
      .catch(function(e) { toast('Leave failed: ' + e.message, 'error'); })
      .finally(function() { $('leaveBtn').disabled = false; });
  };

  // SSE connection
  var es = new EventSource('/events');

  es.addEventListener('pool', function(e) {
    try {
      var snap = JSON.parse(e.data);
      updatePool(snap);
    } catch(err) { console.error('pool parse error', err); }
  });

  es.addEventListener('done', function() {
    dot.className = 'status-dot done';
    es.close();
  });

  es.onerror = function() { dot.className = 'status-dot disconnected'; };
  es.onopen = function() { dot.className = 'status-dot'; };
})();
</script>
</body>
</html>
"##;
