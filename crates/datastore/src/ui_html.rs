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

  .auth-info { display: none; font-size: 11px; color: #888; margin-left: 12px; }
  .auth-info .device-key { color: #6366f1; cursor: text; user-select: all; font-family: monospace; font-size: 10px; }

  .auth-banner {
    display: none; padding: 10px 16px; font-size: 12px;
    background: #2a1a1a; border: 1px solid #f4433666; border-radius: 4px;
    color: #f88; margin-bottom: 16px;
  }
  .auth-banner.show { display: block; }

  @media (max-width: 640px) {
    .upload-row { flex-direction: column; align-items: stretch; }
    input[type="text"] { width: 100%; }
    .header { flex-direction: column; align-items: flex-start; gap: 4px; }
    .header .node-id { margin-left: 0; }
    .auth-info { margin-left: 0; }
    .actions-cell { display: flex; gap: 4px; justify-content: flex-end; }
  }
</style>
</head>
<body>

<div class="header">
  <div style="display:flex;align-items:center;flex-wrap:wrap;">
    <h1>swactor-store</h1>
    <span class="node-id" id="nodeId">connecting...</span>
    <span class="auth-info" id="authInfo">| device key: <span class="device-key" id="deviceKey"></span></span>
  </div>
</div>

<div class="container">
  <div class="auth-banner" id="authBanner">
    <div id="authRequestForm">
      <p style="margin-bottom:8px;">You are not authorized. Request access from the operator:</p>
      <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;">
        <input type="text" id="reqName" placeholder="Your name" maxlength="64" style="width:160px;" />
        <input type="text" id="reqMessage" placeholder="Why do you need access? (optional)" maxlength="256" style="width:280px;" />
        <button class="primary" onclick="submitAccessRequest()">Request Access</button>
      </div>
    </div>
    <div id="authPending" style="display:none">
      <p>Access requested — waiting for operator approval. This page will refresh automatically.</p>
    </div>
  </div>

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

// ─── WASM Ed25519 crypto ────────────────────────────────────────
let authEnabled = false;
let deviceSeed = null;
let pubKeyBytes = null;
let wasmExports = null, bufPtr = 0;

function toHex(buf) {
  return Array.from(new Uint8Array(buf)).map(b => b.toString(16).padStart(2, '0')).join('');
}

function hexToBytes(hex) {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < hex.length; i += 2) bytes[i / 2] = parseInt(hex.substr(i, 2), 16);
  return bytes;
}

function base64urlToBytes(b64) {
  const std = b64.replace(/-/g, '+').replace(/_/g, '/');
  const bin = atob(std);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  return bytes;
}

async function initCrypto() {
  const { instance } = await WebAssembly.instantiate(
    await (await fetch('/crypto.wasm')).arrayBuffer()
  );
  wasmExports = instance.exports;
  bufPtr = wasmExports.buffer_ptr();
}

function derivePublicKey(seed) {
  const mem = new Uint8Array(wasmExports.memory.buffer);
  mem.set(seed, bufPtr);
  wasmExports.get_public_key();
  return new Uint8Array(wasmExports.memory.buffer, bufPtr + 32, 32).slice();
}

function signBytes(message, seed) {
  const mem = new Uint8Array(wasmExports.memory.buffer);
  mem.set(seed, bufPtr);
  mem.set(message, bufPtr + 128);
  wasmExports.ed25519_sign(message.length);
  return new Uint8Array(wasmExports.memory.buffer, bufPtr + 64, 64).slice();
}

async function initKeys() {
  let seed;
  const storedSeed = localStorage.getItem('deviceKeySeed');
  if (storedSeed) {
    seed = hexToBytes(storedSeed);
  } else {
    const oldJwk = localStorage.getItem('deviceKey');
    if (oldJwk) {
      try {
        const jwk = JSON.parse(oldJwk);
        if (jwk.d) {
          seed = base64urlToBytes(jwk.d);
          localStorage.removeItem('deviceKey');
        }
      } catch(e) {}
    }
    if (!seed) {
      seed = new Uint8Array(32);
      crypto.getRandomValues(seed);
    }
    localStorage.setItem('deviceKeySeed', toHex(seed));
  }
  deviceSeed = seed;
  pubKeyBytes = derivePublicKey(seed);
}

async function authFetch(url, opts) {
  if (!authEnabled || !deviceSeed) return fetch(url, opts);
  const nonce = Array.from(crypto.getRandomValues(new Uint8Array(16)));
  const payload = { action: "Access", timestamp: Math.floor(Date.now() / 1000), nonce: nonce };
  const payloadBytes = new TextEncoder().encode(JSON.stringify(payload));
  const sigBytes = signBytes(payloadBytes, deviceSeed);
  const header = JSON.stringify({
    payload: payload,
    public_key: Array.from(pubKeyBytes),
    signature: Array.from(sigBytes)
  });
  opts = opts || {};
  opts.headers = Object.assign({}, opts.headers || {}, { 'X-Signed-Request': header });
  return fetch(url, opts);
}

async function detectAuth() {
  try {
    const r = await fetch('/api/list');
    if (r.status === 401) {
      authEnabled = true;
      await initCrypto();
      await initKeys();
      $('deviceKey').textContent = toHex(pubKeyBytes);
      $('authInfo').style.display = 'inline';
    }
    return r;
  } catch(e) {
    return null;
  }
}

let pollInterval = null;

function showAuthBanner() {
  $('authBanner').classList.add('show');
  const pendingKey = localStorage.getItem('accessRequestPending');
  const pendingName = localStorage.getItem('accessRequestName');
  if (pendingKey && pubKeyBytes && pendingKey === toHex(pubKeyBytes) && pendingName) {
    // Re-submit to ensure the server still has our request (survives node restart)
    resubmitAccessRequest(pendingName);
  }
}

async function resubmitAccessRequest(name) {
  try {
    const r = await authFetch('/api/auth/request', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, message: '' })
    });
    if (r.ok) {
      $('authRequestForm').style.display = 'none';
      $('authPending').style.display = 'block';
      startPolling();
      return;
    }
  } catch(e) { /* fall through */ }
  // Failed — clear stale state, show form
  localStorage.removeItem('accessRequestPending');
  localStorage.removeItem('accessRequestName');
}

async function submitAccessRequest() {
  const name = $('reqName').value.trim();
  if (!name) { toast('Name is required', 'error'); return; }
  const message = $('reqMessage').value.trim();
  try {
    const r = await authFetch('/api/auth/request', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, message })
    });
    if (!r.ok) {
      const j = await r.json().catch(() => ({}));
      throw new Error(j.error || r.statusText);
    }
    $('authRequestForm').style.display = 'none';
    $('authPending').style.display = 'block';
    localStorage.setItem('accessRequestPending', toHex(pubKeyBytes));
    localStorage.setItem('accessRequestName', name);
    startPolling();
  } catch(e) {
    toast('Request failed: ' + e.message, 'error');
  }
}

function startPolling() {
  if (pollInterval) return;
  pollInterval = setInterval(async () => {
    try {
      const r = await authFetch('/api/list');
      if (r.ok) {
        clearInterval(pollInterval);
        pollInterval = null;
        localStorage.removeItem('accessRequestPending');
        localStorage.removeItem('accessRequestName');
        $('authBanner').classList.remove('show');
        const j = await r.json();
        renderTable(j.entries || []);
      }
    } catch(e) { /* keep polling */ }
  }, 5000);
}
// ────────────────────────────────────────────────────────────────

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
    const r = await authFetch('/api/list');
    if (r.status === 401 || r.status === 403) { showAuthBanner(); return; }
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
    const r = await authFetch(url, { method: 'POST', body: file });
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
    const r = await authFetch('/api/data?hash=' + hash);
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
    const r = await authFetch('/api/delete?hash=' + hash, { method: 'POST' });
    if (!r.ok) throw new Error((await r.json()).error || r.statusText);
    toast('deleted', 'success');
    refreshList();
  } catch(e) {
    toast('delete failed: ' + e.message, 'error');
  }
}

async function showDetail(hash) {
  try {
    const r = await authFetch('/api/get?hash=' + hash);
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
$('fileInput').addEventListener('change', function() {
  if (!$('nameInput').value.trim() && this.files.length > 0) {
    $('nameInput').value = this.files[0].name;
  }
});
fetchStatus();
(async function() {
  await detectAuth();
  refreshList();
})();
</script>
</body>
</html>"##;

pub const DATASTORE_ADMIN_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>swactor-store admin</title>
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
    display: flex; align-items: center; gap: 10px;
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
    font-size: 13px; min-height: 44px;
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
  button.small { min-height: 32px; padding: 4px 10px; font-size: 11px; }

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

  .hash-cell { color: #6366f1; font-size: 12px; cursor: default; }
  .actions-cell { white-space: nowrap; text-align: right; }
  .actions-cell button { min-height: 32px; padding: 4px 10px; font-size: 11px; }

  .empty-state {
    text-align: center; color: #555; padding: 32px; font-size: 14px;
  }

  .status-badge { font-size: 11px; margin-left: 8px; }
  .status-badge.ok { color: #4caf50; }
  .status-badge.error { color: #f44336; }

  @media (max-width: 640px) {
    .header { flex-direction: column; align-items: flex-start; gap: 4px; }
    .header .node-id { margin-left: 0; }
    .actions-cell { display: flex; gap: 4px; justify-content: flex-end; }
  }
</style>
</head>
<body>

<div class="header">
  <div style="display:flex;align-items:center;flex-wrap:wrap;">
    <h1>swactor-store admin</h1>
    <span class="node-id" id="nodeId">connecting...</span>
  </div>
</div>

<div class="container">
  <!-- Key Upload Panel -->
  <div class="panel" id="keyPanel">
    <h2>Owner Authentication</h2>
    <p style="color:#888;font-size:12px;margin-bottom:10px;">
      Upload your owner <code style="color:#6366f1;">key.json</code> file to authenticate as the node owner.
    </p>
    <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;">
      <input type="file" id="keyFileInput" accept=".json" />
      <button class="primary" onclick="loadOwnerKey()">Authenticate</button>
      <span id="keyStatus"></span>
    </div>
  </div>

  <!-- Admin Content (hidden until authenticated) -->
  <div id="adminContent" style="display:none">
    <div class="panel">
      <h2>Pending Access Requests <button class="small" onclick="refreshAll()">Refresh</button></h2>
      <div id="requestsTable"></div>
    </div>
    <div class="panel">
      <h2>Authorized Keys</h2>
      <div id="keysTable"></div>
    </div>
    <div class="panel">
      <h2>Grant Key Manually</h2>
      <p style="color:#888;font-size:12px;margin-bottom:10px;">
        Authorize a public key directly, even without a pending request.
      </p>
      <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;">
        <input type="text" id="manualKeyInput" placeholder="Public key (64 hex chars)" style="width:320px;" />
        <input type="text" id="manualNameInput" placeholder="Name (optional)" style="width:160px;" />
        <button class="primary small" onclick="manualGrant()">Grant</button>
      </div>
    </div>
  </div>
</div>

<div class="toast" id="toast"></div>

<script>
const $ = id => document.getElementById(id);

let ownerSeed = null;
let ownerPubBytes = null;
let wasmExports = null, bufPtr = 0;

function toHex(buf) {
  return Array.from(new Uint8Array(buf)).map(b => b.toString(16).padStart(2, '0')).join('');
}

function hexToBytes(hex) {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < hex.length; i += 2) bytes[i / 2] = parseInt(hex.substr(i, 2), 16);
  return bytes;
}

function escHtml(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }

function toast(msg, type) {
  const t = $('toast');
  t.textContent = msg;
  t.className = 'toast show ' + type;
  setTimeout(() => t.className = 'toast', 2500);
}

async function initCrypto() {
  const { instance } = await WebAssembly.instantiate(
    await (await fetch('/crypto.wasm')).arrayBuffer()
  );
  wasmExports = instance.exports;
  bufPtr = wasmExports.buffer_ptr();
}

function derivePublicKey(seed) {
  const mem = new Uint8Array(wasmExports.memory.buffer);
  mem.set(seed, bufPtr);
  wasmExports.get_public_key();
  return new Uint8Array(wasmExports.memory.buffer, bufPtr + 32, 32).slice();
}

function signBytes(message, seed) {
  const mem = new Uint8Array(wasmExports.memory.buffer);
  mem.set(seed, bufPtr);
  mem.set(message, bufPtr + 128);
  wasmExports.ed25519_sign(message.length);
  return new Uint8Array(wasmExports.memory.buffer, bufPtr + 64, 64).slice();
}

let cryptoReady = initCrypto();

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

async function loadOwnerKey() {
  const fileInput = $('keyFileInput');
  const status = $('keyStatus');
  if (!fileInput.files.length) {
    status.innerHTML = '<span class="status-badge error">Select a key.json file</span>';
    return;
  }
  try {
    await cryptoReady;
    const text = await fileInput.files[0].text();
    const json = JSON.parse(text);
    const secretHex = json.secret_key;
    const publicHex = json.public_key;
    if (!secretHex || !publicHex) throw new Error('Missing secret_key or public_key');

    ownerSeed = hexToBytes(secretHex);
    ownerPubBytes = hexToBytes(publicHex);

    const derived = toHex(derivePublicKey(ownerSeed));
    if (derived !== publicHex) throw new Error('Key mismatch: derived public key does not match');

    // Test call to verify this is the owner key
    const r = await ownerAuthFetch('/api/auth/requests');
    if (r.ok) {
      status.innerHTML = '<span class="status-badge ok">Authenticated</span>';
      $('adminContent').style.display = 'block';
      refreshAll();
    } else {
      ownerSeed = null;
      ownerPubBytes = null;
      status.innerHTML = '<span class="status-badge error">Not the owner key (403)</span>';
    }
  } catch(e) {
    ownerSeed = null;
    ownerPubBytes = null;
    status.innerHTML = '<span class="status-badge error">Error: ' + escHtml(e.message) + '</span>';
  }
}

async function ownerAuthFetch(url, opts) {
  if (!ownerSeed) return fetch(url, opts);
  const nonce = Array.from(crypto.getRandomValues(new Uint8Array(16)));
  const payload = { action: "Access", timestamp: Math.floor(Date.now() / 1000), nonce: nonce };
  const payloadBytes = new TextEncoder().encode(JSON.stringify(payload));
  const sigBytes = signBytes(payloadBytes, ownerSeed);
  const header = JSON.stringify({
    payload: payload,
    public_key: Array.from(ownerPubBytes),
    signature: Array.from(sigBytes)
  });
  opts = opts || {};
  opts.headers = Object.assign({}, opts.headers || {}, { 'X-Signed-Request': header });
  return fetch(url, opts);
}

function addDisambiguation(items, nameField) {
  const counts = {};
  for (const item of items) {
    const name = item[nameField] || '';
    counts[name] = (counts[name] || 0) + 1;
  }
  return items.map(item => {
    const name = item[nameField] || '';
    if (counts[name] > 1) {
      const prefix = item.key.substring(0, 8);
      return { ...item, displayName: name + ' (' + prefix + ')' };
    }
    return { ...item, displayName: name };
  });
}

async function refreshAll() {
  // Fetch requests
  try {
    const r = await ownerAuthFetch('/api/auth/requests');
    if (!r.ok) { $('requestsTable').innerHTML = '<div class="empty-state">failed to load</div>'; return; }
    const requests = await r.json();
    renderRequests(requests);
  } catch(e) {
    $('requestsTable').innerHTML = '<div class="empty-state">failed to load</div>';
  }

  // Fetch keys
  try {
    const r = await ownerAuthFetch('/api/auth/keys');
    if (!r.ok) { $('keysTable').innerHTML = '<div class="empty-state">failed to load</div>'; return; }
    const keys = await r.json();
    renderKeys(keys);
  } catch(e) {
    $('keysTable').innerHTML = '<div class="empty-state">failed to load</div>';
  }
}

function renderRequests(requests) {
  if (!requests || requests.length === 0) {
    $('requestsTable').innerHTML = '<div class="empty-state">no pending requests</div>';
    return;
  }
  const items = addDisambiguation(requests, 'name');
  let html = '<table><thead><tr><th>Name</th><th>Message</th><th>Key</th><th style="text-align:right">Actions</th></tr></thead><tbody>';
  for (const item of items) {
    const short = item.key.substring(0, 16);
    html += '<tr>';
    html += '<td>' + escHtml(item.displayName) + '</td>';
    html += '<td>' + escHtml(item.message || '') + '</td>';
    html += '<td class="hash-cell" title="' + escHtml(item.key) + '">' + escHtml(short) + '</td>';
    html += '<td class="actions-cell">';
    html += '<button class="small primary" onclick="grantKey(\'' + item.key + '\')">grant</button> ';
    html += '<button class="small danger" onclick="denyKey(\'' + item.key + '\')">deny</button>';
    html += '</td></tr>';
  }
  html += '</tbody></table>';
  $('requestsTable').innerHTML = html;
}

function renderKeys(keys) {
  if (!keys || keys.length === 0) {
    $('keysTable').innerHTML = '<div class="empty-state">no authorized keys</div>';
    return;
  }
  const items = addDisambiguation(keys, 'label');
  let html = '<table><thead><tr><th>Name</th><th>Key</th><th style="text-align:right">Actions</th></tr></thead><tbody>';
  for (const item of items) {
    const short = item.key.substring(0, 16);
    html += '<tr>';
    html += '<td>' + escHtml(item.displayName) + '</td>';
    html += '<td class="hash-cell" title="' + escHtml(item.key) + '">' + escHtml(short) + '</td>';
    html += '<td class="actions-cell">';
    html += '<button class="small danger" onclick="revokeKey(\'' + item.key + '\')">revoke</button>';
    html += '</td></tr>';
  }
  html += '</tbody></table>';
  $('keysTable').innerHTML = html;
}

async function grantKey(hex) {
  try {
    const r = await ownerAuthFetch('/api/auth/grant?key=' + hex, { method: 'POST' });
    if (!r.ok) { const j = await r.json().catch(() => ({})); throw new Error(j.error || r.statusText); }
    toast('Granted ' + hex.substring(0, 12), 'success');
    refreshAll();
  } catch(e) {
    toast('Grant failed: ' + e.message, 'error');
  }
}

async function denyKey(hex) {
  try {
    const r = await ownerAuthFetch('/api/auth/deny?key=' + hex, { method: 'POST' });
    if (!r.ok) { const j = await r.json().catch(() => ({})); throw new Error(j.error || r.statusText); }
    toast('Denied ' + hex.substring(0, 12), 'success');
    refreshAll();
  } catch(e) {
    toast('Deny failed: ' + e.message, 'error');
  }
}

async function revokeKey(hex) {
  if (!confirm('Revoke ' + hex.substring(0, 16) + '?')) return;
  try {
    const r = await ownerAuthFetch('/api/auth/revoke?key=' + hex, { method: 'POST' });
    if (!r.ok) { const j = await r.json().catch(() => ({})); throw new Error(j.error || r.statusText); }
    toast('Revoked ' + hex.substring(0, 12), 'success');
    refreshAll();
  } catch(e) {
    toast('Revoke failed: ' + e.message, 'error');
  }
}

async function manualGrant() {
  const key = $('manualKeyInput').value.trim();
  if (!key || key.length !== 64) { toast('Enter a 64-char hex public key', 'error'); return; }
  const name = $('manualNameInput').value.trim();
  let url = '/api/auth/grant?key=' + key;
  if (name) url += '&name=' + encodeURIComponent(name);
  try {
    const r = await ownerAuthFetch(url, { method: 'POST' });
    if (!r.ok) { const j = await r.json().catch(() => ({})); throw new Error(j.error || r.statusText); }
    toast('Granted ' + key.substring(0, 12), 'success');
    $('manualKeyInput').value = '';
    $('manualNameInput').value = '';
    refreshAll();
  } catch(e) {
    toast('Grant failed: ' + e.message, 'error');
  }
}

// Init
fetchStatus();
</script>
</body>
</html>"##;
