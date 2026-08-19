(() => {
  const CONTROL_ID = 'myelin-fleet-control';

  async function syncControl() {
    const nodeView = document.querySelector('.node-view[data-node]');
    if (!nodeView) return;
    const rawNodeId = nodeView.getAttribute('data-node') || '';
    if (!/^\d+$/.test(rawNodeId)) return;
    const logicalNodeId = Number(rawNodeId);

    let model;
    try {
      const response = await fetch('/api/control/status', { cache: 'no-store' });
      if (!response.ok) return;
      const payload = await response.json();
      model = payload.Status;
    } catch (_) {
      return;
    }
    const node = model?.nodes?.find(candidate => candidate.logical_node_id === logicalNodeId);
    if (!node) return;

    const heading = nodeView.querySelector('section.panel > h2');
    if (!heading) return;
    let control = document.getElementById(CONTROL_ID);
    if (!control) {
      control = document.createElement('div');
      control.id = CONTROL_ID;
      control.style.cssText = 'display:flex;align-items:center;gap:10px;margin:0 0 12px';
      const button = document.createElement('button');
      button.type = 'button';
      button.dataset.action = 'kill';
      button.style.cssText = 'border:1px solid #ef4444;background:transparent;color:#ef4444;border-radius:3px;padding:6px 10px;font:600 12px ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;cursor:pointer';
      const message = document.createElement('span');
      message.dataset.message = '';
      message.style.cssText = 'font:12px ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;color:var(--muted)';
      control.append(button, message);
      heading.after(control);
    }

    const button = control.querySelector('[data-action="kill"]');
    const message = control.querySelector('[data-message]');
    const terminal = node.phase === 'stopped' || node.phase === 'orphan';
    const pending = node.phase === 'kill_requested' || node.phase === 'stopping';
    button.hidden = terminal;
    button.disabled = pending;
    button.textContent = pending ? 'Kill requested' : 'Kill';
    button.style.opacity = pending ? '.55' : '1';
    message.textContent = terminal ? `managed node ${logicalNodeId}: ${node.phase}` : '';

    button.onclick = async () => {
      if (!window.confirm(`Kill managed node ${logicalNodeId}? Vast.ai contracts are destroyed and billing stops.`)) return;
      button.disabled = true;
      message.textContent = 'submitting…';
      const commandId = `fleet-kill-${globalThis.crypto?.randomUUID?.() || Date.now()}`;
      try {
        const response = await fetch(`/api/control/nodes/${logicalNodeId}/kill`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ command_id: commandId }),
        });
        if (!response.ok) {
          const body = await response.json().catch(() => ({}));
          throw new Error(body.error || `HTTP ${response.status}`);
        }
        message.textContent = 'Kill accepted';
      } catch (error) {
        button.disabled = false;
        message.textContent = error.message;
      }
    };
  }

  const page = document.getElementById('page');
  if (page) new MutationObserver(syncControl).observe(page, { childList: true, subtree: true });
  syncControl();
  setInterval(syncControl, 1000);
})();
