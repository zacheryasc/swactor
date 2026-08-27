(() => {
  const NODE_CONTROL_ID = 'myelin-fleet-control';
  const BULK_CONTROL_ID = 'myelin-fleet-bulk-control';
  const CONFIRM_ID = 'myelin-confirm-dialog';
  const STYLE_ID = 'myelin-fleet-control-style';
  const selectedNodes = new Set();
  let lastModel = null;
  let syncing = false;
  const page = document.getElementById('page');
  if (!page) return;

  function installUi() {
    if (document.getElementById(STYLE_ID)) return;
    const style = document.createElement('style');
    style.id = STYLE_ID;
    style.textContent = `
      .myelin-control { display:flex;align-items:center;flex-wrap:wrap;gap:10px;margin:0 0 12px }
      .myelin-control button { border:1px solid var(--cyan);background:transparent;color:var(--cyan);border-radius:var(--r);padding:6px 10px;font:600 12px var(--mono);cursor:pointer }
      .myelin-control button.danger { border-color:var(--bad);color:var(--bad) }
      .myelin-control button:disabled { cursor:not-allowed;opacity:.55 }
      .myelin-control-message { color:var(--muted);font:12px var(--mono) }
      .myelin-bulk-control { width:fit-content;margin:0 0 12px auto;padding:6px 8px;border:1px solid var(--divider);background:transparent;border-radius:var(--r) }
      .node-card[data-bulk-selected="true"] { border-color:var(--bad);background:var(--selected);box-shadow:inset 3px 0 0 var(--bad) }
      .node-card[data-bulk-killable="true"] { user-select:none }
      .myelin-confirm { width:min(440px,calc(100vw - 32px));padding:0;color:var(--text);background:var(--panel);border:1px solid var(--bad);border-radius:var(--r);box-shadow:0 18px 60px rgba(0,0,0,.55) }
      .myelin-confirm::backdrop { background:rgba(0,6,12,.78) }
      .myelin-confirm form { display:grid;gap:14px;padding:18px }
      .myelin-confirm h2,.myelin-confirm p { margin:0 }
      .myelin-confirm h2 { color:var(--bad) }
      .myelin-confirm-actions { display:flex;justify-content:flex-end;gap:8px }
      .myelin-confirm button { padding:6px 12px;background:transparent;color:var(--text);border:1px solid var(--border);border-radius:var(--r);cursor:pointer;font:600 13px var(--mono) }
      .myelin-confirm button[value="confirm"] { color:var(--danger-ink);background:var(--danger-fill);border-color:var(--danger-border) }
    `;
    document.head.append(style);
  }

  function confirmTermination(title, message, action) {
    let dialog = document.getElementById(CONFIRM_ID);
    if (!dialog) {
      dialog = document.createElement('dialog');
      dialog.id = CONFIRM_ID;
      dialog.className = 'myelin-confirm';
      dialog.setAttribute('aria-labelledby', 'myelin-confirm-title');
      dialog.setAttribute('aria-describedby', 'myelin-confirm-message');
      dialog.innerHTML = `<form method="dialog">
        <h2 id="myelin-confirm-title"></h2>
        <p id="myelin-confirm-message"></p>
        <div class="myelin-confirm-actions">
          <button value="cancel" autofocus>Cancel</button>
          <button value="confirm"></button>
        </div>
      </form>`;
      document.body.append(dialog);
    }
    dialog.querySelector('#myelin-confirm-title').textContent = title;
    dialog.querySelector('#myelin-confirm-message').textContent = message;
    dialog.querySelector('[value="confirm"]').textContent = action;
    dialog.returnValue = 'cancel';
    return new Promise(resolve => {
      dialog.addEventListener('close', () => resolve(dialog.returnValue === 'confirm'), { once: true });
      dialog.showModal();
    });
  }

  function killable(node) {
    return node && !['kill_requested', 'stopping', 'stopped', 'orphan'].includes(node.phase);
  }

  function managedNodes(model) {
    return Array.isArray(model?.nodes) ? model.nodes : [];
  }

  function commandId(logicalNodeId) {
    return `fleet-kill-${logicalNodeId}-${globalThis.crypto?.randomUUID?.() || Date.now()}`;
  }

  async function submitKills(logicalNodeIds) {
    return Promise.all(logicalNodeIds.map(async logicalNodeId => {
      try {
        const response = await fetch(`/api/control/nodes/${logicalNodeId}/kill`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ command_id: commandId(logicalNodeId) }),
        });
        if (!response.ok) {
          const body = await response.json().catch(() => ({}));
          throw new Error(body.error || `HTTP ${response.status}`);
        }
        return { logicalNodeId, error: null };
      } catch (error) {
        return { logicalNodeId, error: error.message };
      }
    }));
  }

  function resultMessage(results) {
    const failed = results.filter(result => result.error);
    if (!failed.length) return `Termination accepted for ${results.length} node${results.length === 1 ? '' : 's'}`;
    const accepted = results.length - failed.length;
    const failures = failed.map(result => `${result.logicalNodeId}: ${result.error}`).join('; ');
    return `${accepted} accepted; ${failed.length} failed — ${failures}`;
  }

  function syncFleetControl(model) {
    const page = document.getElementById('page');
    const cards = [...page.querySelectorAll('a.node-card[data-node]')];
    if (!cards.length) {
      document.getElementById(BULK_CONTROL_ID)?.remove();
      return;
    }
    const byId = new Map(managedNodes(model).map(node => [node.logical_node_id, node]));
    for (const logicalNodeId of [...selectedNodes]) {
      if (!killable(byId.get(logicalNodeId))) selectedNodes.delete(logicalNodeId);
    }
    for (const card of cards) {
      const logicalNodeId = Number(card.dataset.node);
      const selectable = card.dataset.origin !== 'orchestrator'
        && Number.isSafeInteger(logicalNodeId)
        && killable(byId.get(logicalNodeId));
      card.dataset.bulkKillable = String(selectable);
      card.dataset.bulkSelected = String(selectable && selectedNodes.has(logicalNodeId));
      card.setAttribute('aria-selected', String(selectable && selectedNodes.has(logicalNodeId)));
      card.title = selectable ? 'Open node; Shift+click to select for termination' : '';
    }

    const count = selectedNodes.size;
    let control = document.getElementById(BULK_CONTROL_ID);
    if (!count) {
      control?.remove();
      return;
    }
    if (!control) {
      control = document.createElement('div');
      control.id = BULK_CONTROL_ID;
      control.className = 'myelin-control myelin-bulk-control';
      control.innerHTML = `<span data-selection></span>
        <button type="button" class="danger" data-action="kill-selected">Terminate selected</button>
        <span class="myelin-control-message" data-message role="status"></span>`;
      page.querySelector('.grid').before(control);
      control.querySelector('[data-action="kill-selected"]').onclick = killSelected;
    }
    control.querySelector('[data-selection]').textContent =
      `${count} worker node${count === 1 ? '' : 's'} selected`;
    const button = control.querySelector('[data-action="kill-selected"]');
    button.disabled = false;
    button.textContent = `Terminate ${count} selected`;
  }


  async function killSelected() {
    const control = document.getElementById(BULK_CONTROL_ID);
    const button = control?.querySelector('[data-action="kill-selected"]');
    const message = control?.querySelector('[data-message]');
    const byId = new Map(managedNodes(lastModel).map(node => [node.logical_node_id, node]));
    const ids = [...selectedNodes].filter(logicalNodeId => killable(byId.get(logicalNodeId)));
    if (!ids.length) return;
    const label = ids.join(', ');
    if (!await confirmTermination(
      `Terminate ${ids.length} managed node${ids.length === 1 ? '' : 's'}?`,
      `Terminate managed node${ids.length === 1 ? '' : 's'} ${label}? Vast.ai contracts are destroyed and billing stops.`,
      `Terminate ${ids.length}`,
    )) return;
    button.disabled = true;
    message.textContent = 'Submitting termination requests…';
    const results = await submitKills(ids);
    for (const result of results) {
      if (!result.error) selectedNodes.delete(result.logicalNodeId);
    }
    message.textContent = resultMessage(results);
    syncFleetControl(lastModel);
  }

  function ensureOrchestratorControl(nodeView, model) {
    const heading = nodeView.querySelector('section.panel > h2');
    if (!heading) return;
    let control = document.getElementById(NODE_CONTROL_ID);
    if (!control) {
      control = document.createElement('div');
      control.id = NODE_CONTROL_ID;
      control.className = 'myelin-control';
      control.innerHTML = `<button type="button" class="danger" data-action="shutdown-all">Shutdown all</button>
        <span class="myelin-control-message" data-message role="status"></span>`;
      heading.after(control);
      control.querySelector('[data-action="shutdown-all"]').onclick = shutdownAll;
    }
    const count = managedNodes(model).filter(killable).length;
    const button = control.querySelector('[data-action="shutdown-all"]');
    button.disabled = count === 0;
    button.textContent = count ? `Shutdown all (${count})` : 'No managed nodes to shut down';
  }

  async function shutdownAll() {
    const control = document.getElementById(NODE_CONTROL_ID);
    const button = control?.querySelector('[data-action="shutdown-all"]');
    const message = control?.querySelector('[data-message]');
    const ids = managedNodes(lastModel).filter(killable).map(node => node.logical_node_id);
    if (!ids.length) return;
    if (!await confirmTermination(
      `Shut down all ${ids.length} managed node${ids.length === 1 ? '' : 's'}?`,
      `This terminates managed nodes ${ids.join(', ')}. Vast.ai contracts are destroyed and billing stops.`,
      'Shutdown all',
    )) return;
    button.disabled = true;
    message.textContent = 'Submitting termination requests…';
    const results = await submitKills(ids);
    message.textContent = resultMessage(results);
  }

  async function syncManagedNodeControl(nodeView, model, logicalNodeId) {
    const node = managedNodes(model).find(candidate => candidate.logical_node_id === logicalNodeId);
    if (!node) return;

    const heading = nodeView.querySelector('section.panel > h2');
    if (!heading) return;
    let control = document.getElementById(NODE_CONTROL_ID);
    if (!control) {
      control = document.createElement('div');
      control.id = NODE_CONTROL_ID;
      control.className = 'myelin-control';
      control.innerHTML = `<button type="button" class="danger" data-action="kill">Kill</button>
        <span class="myelin-control-message" data-message role="status"></span>`;
      heading.after(control);
    }

    const killButton = control.querySelector('[data-action="kill"]');
    const message = control.querySelector('[data-message]');
    const terminal = node.phase === 'stopped' || node.phase === 'orphan';
    const pending = node.phase === 'kill_requested' || node.phase === 'stopping';
    killButton.hidden = terminal;
    killButton.disabled = pending;
    killButton.textContent = pending ? 'Kill requested' : 'Kill';
    message.textContent = `managed node ${logicalNodeId}: ${node.phase}`;

    killButton.onclick = async () => {
      if (!await confirmTermination(
        'Terminate managed node?',
        `Terminate managed node ${logicalNodeId}? Vast.ai contracts are destroyed and billing stops.`,
        'Terminate node',
      )) return;
      killButton.disabled = true;
      message.textContent = 'Submitting kill…';
      const [result] = await submitKills([logicalNodeId]);
      message.textContent = result.error ? result.error : 'Kill accepted';
      if (result.error) killButton.disabled = false;
    };
  }

  async function syncControl() {
    if (syncing) return;
    syncing = true;
    try {
      let model;
      try {
        const response = await fetch('/api/control/fleet', { cache: 'no-store' });
        if (!response.ok) return;
        model = (await response.json()).FleetStatus;
      } catch (_) {
        return;
      }
      lastModel = model;
      window.dispatchEvent(new CustomEvent('dashboard-hardware-source', {
        detail: { source: model?.provider?.provisioning_mode === 'mock' ? 'orchestrator' : 'node' },
      }));

      const nodeView = document.querySelector('.node-view[data-node]');
      if (!nodeView) {
        syncFleetControl(model);
        return;
      }
      if (nodeView.dataset.origin === 'orchestrator') {
        ensureOrchestratorControl(nodeView, model);
        return;
      }
      const rawNodeId = nodeView.dataset.node || '';
      if (/^\d+$/.test(rawNodeId)) await syncManagedNodeControl(nodeView, model, Number(rawNodeId));
    } finally {
      syncing = false;
    }
  }

  installUi();
  page.addEventListener('click', event => {
    const card = event.target.closest('a.node-card[data-node]');
    if (!card || !page.contains(card) || !event.shiftKey) return;
    const logicalNodeId = Number(card.dataset.node);
    const node = managedNodes(lastModel).find(candidate => candidate.logical_node_id === logicalNodeId);
    const selectable = card.dataset.origin !== 'orchestrator'
      && Number.isSafeInteger(logicalNodeId)
      && killable(node);
    if (!selectable) return;
    event.preventDefault();
    if (selectedNodes.has(logicalNodeId)) selectedNodes.delete(logicalNodeId);
    else selectedNodes.add(logicalNodeId);
    syncFleetControl(lastModel);
  });
  new MutationObserver(mutations => {
    const dashboardChanged = mutations.some(mutation => {
      const element = mutation.target.nodeType === Node.ELEMENT_NODE
        ? mutation.target
        : mutation.target.parentElement;
      return !element?.closest(`#${NODE_CONTROL_ID}, #${BULK_CONTROL_ID}`);
    });
    if (dashboardChanged) syncControl();
  }).observe(page, { childList: true, subtree: true });
  syncControl();
  setInterval(syncControl, 1000);
})();
