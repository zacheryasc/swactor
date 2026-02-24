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
      <a href="/plugin/datastore" class="nav-link">Datastore</a>
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
