pub const DATASTORE_UI_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>swactor-store</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'Menlo', 'Consolas', 'Monaco', monospace; background: #0f1117; color: #e0e0e0; font-size: 13px; }

  .header {
    display: flex; align-items: center; justify-content: space-between;
    padding: 12px 20px; background: #161822; border-bottom: 1px solid #2a2d3e;
  }
  .header h1 { font-size: 16px; font-weight: 600; color: #fff; }
  .header .node-id { font-size: 11px; color: #888; margin-left: 12px; }

  .container { max-width: 960px; margin: 0 auto; padding: 16px; }

  .panel {
    background: #161822; border: 1px solid #2a2d3e; border-radius: 6px;
    padding: 16px; margin-bottom: 16px;
  }
  .panel h2 {
    font-size: 12px; color: #888; text-transform: uppercase;
    letter-spacing: 1px; margin-bottom: 12px;
  }

  .upload-row {
    display: flex; gap: 10px; align-items: center; flex-wrap: wrap;
  }

  input[type="file"] {
    background: #1c1f2e; color: #e0e0e0; border: 1px solid #2a2d3e;
    border-radius: 4px; padding: 8px; font-family: inherit; font-size: 13px;
    min-height: 44px; cursor: pointer;
  }
  input[type="file"]::file-selector-button {
    background: #1e2030; color: #e0e0e0; border: 1px solid #2a2d3e;
    border-radius: 4px; padding: 6px 12px; font-family: inherit;
    font-size: 12px; cursor: pointer; margin-right: 8px;
  }
  input[type="file"]::file-selector-button:hover { border-color: #6366f1; }

  input[type="text"] {
    background: #1c1f2e; color: #e0e0e0; border: 1px solid #2a2d3e;
    border-radius: 4px; padding: 8px 12px; font-family: inherit;
    font-size: 13px; min-height: 44px; width: 200px;
  }
  input[type="text"]:focus { outline: none; border-color: #6366f1; }

  button {
    background: #1e2030; color: #e0e0e0; border: 1px solid #2a2d3e;
    border-radius: 4px; padding: 8px 16px; font-family: inherit;
    font-size: 13px; cursor: pointer; min-height: 44px;
    transition: border-color 0.15s;
  }
  button:hover { border-color: #6366f1; color: #fff; }
  button:disabled { opacity: 0.4; cursor: default; }
  button.danger:hover { border-color: #f44336; }
  button.primary { background: #6366f1; border-color: #6366f1; color: #fff; font-weight: 600; }
  button.primary:hover { background: #5558e6; }

  .toast {
    position: fixed; bottom: 20px; right: 20px; padding: 10px 16px;
    border-radius: 4px; font-size: 12px; z-index: 100; opacity: 0;
    transition: opacity 0.3s; pointer-events: none;
  }
  .toast.show { opacity: 1; }
  .toast.success { background: #4caf50; color: #fff; }
  .toast.error { background: #f44336; color: #fff; }

  table { width: 100%; border-collapse: collapse; }
  th {
    text-align: left; font-size: 10px; color: #888;
    text-transform: uppercase; letter-spacing: 1px;
    padding: 6px 8px; border-bottom: 1px solid #2a2d3e;
  }
  td {
    padding: 8px; border-bottom: 1px solid #1c1f2e;
    font-size: 13px; vertical-align: middle;
  }
  tr:hover td { background: #1c1f2e; }
  tr { cursor: pointer; }

  .hash-cell { color: #6366f1; font-size: 12px; }
  .size-cell { color: #888; white-space: nowrap; }
  .actions-cell { white-space: nowrap; text-align: right; }
  .actions-cell button { min-height: 32px; padding: 4px 10px; font-size: 11px; }

  .empty-state {
    text-align: center; color: #555; padding: 32px; font-size: 14px;
  }

  /* Modal overlay */
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

  @media (max-width: 640px) {
    .upload-row { flex-direction: column; align-items: stretch; }
    input[type="text"] { width: 100%; }
    .header { flex-direction: column; align-items: flex-start; gap: 4px; }
    .header .node-id { margin-left: 0; }
    .actions-cell { display: flex; gap: 4px; justify-content: flex-end; }
  }
</style>
</head>
<body>

<div class="header">
  <div style="display:flex;align-items:center;flex-wrap:wrap;">
    <h1>swactor-store</h1>
    <span class="node-id" id="nodeId">connecting...</span>
  </div>
</div>

<div class="container">
  <!-- Upload panel -->
  <div class="panel">
    <h2>Upload</h2>
    <div class="upload-row">
      <input type="file" id="fileInput" />
      <input type="text" id="nameInput" placeholder="name (optional)" />
      <button class="primary" id="uploadBtn" onclick="upload()">Upload</button>
    </div>
  </div>

  <!-- Object table -->
  <div class="panel">
    <h2>Objects</h2>
    <div id="tableWrap"></div>
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
const $ = id => document.getElementById(id);

function toast(msg, type) {
  const t = $('toast');
  t.textContent = msg;
  t.className = 'toast show ' + type;
  setTimeout(() => t.className = 'toast', 2500);
}

function fmtSize(b) {
  if (b < 1024) return b + ' B';
  if (b < 1048576) return (b / 1024).toFixed(1) + ' KB';
  if (b < 1073741824) return (b / 1048576).toFixed(1) + ' MB';
  return (b / 1073741824).toFixed(1) + ' GB';
}

async function fetchStatus() {
  try {
    const r = await fetch('/api/status');
    const j = await r.json();
    $('nodeId').textContent = j.node_id.substring(0, 16) + '...';
    $('nodeId').title = j.node_id;
  } catch(e) {
    $('nodeId').textContent = 'offline';
  }
}

async function refreshList() {
  try {
    const r = await fetch('/api/list');
    const j = await r.json();
    renderTable(j.entries || []);
  } catch(e) {
    $('tableWrap').innerHTML = '<div class="empty-state">failed to load</div>';
  }
}

function renderTable(entries) {
  if (entries.length === 0) {
    $('tableWrap').innerHTML = '<div class="empty-state">no objects stored</div>';
    return;
  }
  let html = '<table><thead><tr><th>Hash</th><th>Name</th><th>Size</th><th style="text-align:right">Actions</th></tr></thead><tbody>';
  for (const e of entries) {
    const h = e.content_hash;
    const short = h.substring(0, 16);
    const name = e.name || '—';
    const size = fmtSize(e.size_bytes);
    html += '<tr onclick="showDetail(\'' + h + '\')">';
    html += '<td class="hash-cell" title="' + h + '">' + short + '</td>';
    html += '<td>' + escHtml(name) + '</td>';
    html += '<td class="size-cell">' + size + '</td>';
    html += '<td class="actions-cell">';
    html += '<button onclick="event.stopPropagation();download(\'' + h + '\',\'' + escAttr(e.name || h.substring(0,12)) + '\')">download</button> ';
    html += '<button class="danger" onclick="event.stopPropagation();del(\'' + h + '\')">delete</button>';
    html += '</td></tr>';
  }
  html += '</tbody></table>';
  $('tableWrap').innerHTML = html;
}

function escHtml(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }
function escAttr(s) { return s.replace(/'/g, "\\'").replace(/"/g, '&quot;'); }

async function upload() {
  const file = $('fileInput').files[0];
  if (!file) { toast('select a file first', 'error'); return; }
  const name = $('nameInput').value.trim();
  const btn = $('uploadBtn');
  btn.disabled = true;
  btn.textContent = 'uploading...';
  try {
    let url = '/api/put';
    if (name) url += '?name=' + encodeURIComponent(name);
    const r = await fetch(url, { method: 'POST', body: file });
    if (!r.ok) throw new Error((await r.json()).error || r.statusText);
    const j = await r.json();
    toast('uploaded ' + j.content_hash.substring(0, 12), 'success');
    $('fileInput').value = '';
    $('nameInput').value = '';
    refreshList();
  } catch(e) {
    toast('upload failed: ' + e.message, 'error');
  } finally {
    btn.disabled = false;
    btn.textContent = 'Upload';
  }
}

async function download(hash, filename) {
  try {
    const r = await fetch('/api/data?hash=' + hash);
    if (!r.ok) throw new Error('not found');
    const blob = await r.blob();
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = filename;
    a.click();
    URL.revokeObjectURL(a.href);
  } catch(e) {
    toast('download failed: ' + e.message, 'error');
  }
}

async function del(hash) {
  if (!confirm('Delete ' + hash.substring(0, 16) + '?')) return;
  try {
    const r = await fetch('/api/delete?hash=' + hash, { method: 'POST' });
    if (!r.ok) throw new Error((await r.json()).error || r.statusText);
    toast('deleted', 'success');
    refreshList();
  } catch(e) {
    toast('delete failed: ' + e.message, 'error');
  }
}

async function showDetail(hash) {
  try {
    const r = await fetch('/api/get?hash=' + hash);
    if (!r.ok) throw new Error('not found');
    const j = await r.json();
    const e = j.entry;
    const m = j.manifest;
    let html = '';
    html += row('Hash', e.content_hash);
    html += row('Name', e.name || '—');
    html += row('Size', fmtSize(e.size_bytes));
    html += row('Node', e.node_id.substring(0, 16) + '...');
    if (e.tags && Object.keys(e.tags).length > 0) {
      html += row('Tags', Object.entries(e.tags).map(([k,v]) => k + '=' + v).join(', '));
    }
    html += row('Chunks', m.chunks.length + ' (' + fmtSize(m.chunk_size) + ' each)');
    if (m.chunks.length > 0) {
      html += '<div class="chunk-list">';
      for (let i = 0; i < m.chunks.length; i++) {
        const c = m.chunks[i];
        html += '<div class="chunk-item">#' + i + ' ' + c.hash.substring(0, 16) + ' (' + fmtSize(c.size) + ')</div>';
      }
      html += '</div>';
    }
    $('modalBody').innerHTML = html;
    $('modal').classList.add('open');
  } catch(e) {
    toast('failed to load detail', 'error');
  }
}

function row(label, value) {
  return '<div class="detail-row"><div class="detail-label">' + label + '</div><div class="detail-value">' + escHtml(String(value)) + '</div></div>';
}

function closeModal() { $('modal').classList.remove('open'); }
document.addEventListener('keydown', e => { if (e.key === 'Escape') closeModal(); });

// Init
fetchStatus();
refreshList();
</script>
</body>
</html>"##;
