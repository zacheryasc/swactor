(() => {
  const CONTROL_ID = 'myelin-fleet-control';
  const CONFIRM_ID = 'myelin-confirm-dialog';
  const CONFIRM_STYLE_ID = 'myelin-confirm-dialog-style';
  const selectedJobs = new Map();

  function confirmKill(logicalNodeId) {
    let dialog = document.getElementById(CONFIRM_ID);
    if (!dialog) {
      const style = document.createElement('style');
      style.id = CONFIRM_STYLE_ID;
      style.textContent = `
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
      dialog = document.createElement('dialog');
      dialog.id = CONFIRM_ID;
      dialog.className = 'myelin-confirm';
      dialog.setAttribute('aria-labelledby', 'myelin-confirm-title');
      dialog.setAttribute('aria-describedby', 'myelin-confirm-message');
      dialog.innerHTML = `<form method="dialog">
        <h2 id="myelin-confirm-title">Terminate managed node?</h2>
        <p id="myelin-confirm-message"></p>
        <div class="myelin-confirm-actions">
          <button value="cancel" autofocus>Cancel</button>
          <button value="confirm">Terminate node</button>
        </div>
      </form>`;
      document.body.append(dialog);
    }
    dialog.querySelector('#myelin-confirm-message').textContent =
      `Terminate managed node ${logicalNodeId}? Vast.ai contracts are destroyed and billing stops.`;
    dialog.returnValue = 'cancel';
    return new Promise(resolve => {
      dialog.addEventListener('close', () => resolve(dialog.returnValue === 'confirm'), { once: true });
      dialog.showModal();
    });
  }

  async function syncControl() {
    let model;
    try {
      const response = await fetch('/api/control/status', { cache: 'no-store' });
      if (!response.ok) return;
      const payload = await response.json();
      model = payload.Status;
    } catch (_) {
      return;
    }
    window.dispatchEvent(new CustomEvent('dashboard-hardware-source', {
      detail: {
        source: model?.provider?.provisioning_mode === 'mock' ? 'orchestrator' : 'node',
      },
    }));

    const nodeView = document.querySelector('.node-view[data-node]');
    if (!nodeView) return;
    const rawNodeId = nodeView.getAttribute('data-node') || '';
    if (!/^\d+$/.test(rawNodeId)) return;
    const logicalNodeId = Number(rawNodeId);
    const node = model?.nodes?.find(candidate => candidate.logical_node_id === logicalNodeId);
    if (!node) return;

    let job = { state: 'idle', message: 'No job submitted' };
    try {
      const response = await fetch(`/api/control/nodes/${logicalNodeId}/job`, { cache: 'no-store' });
      if (response.ok) job = await response.json();
    } catch (_) {}

    const heading = nodeView.querySelector('section.panel > h2');
    if (!heading) return;
    let control = document.getElementById(CONTROL_ID);
    if (!control) {
      control = document.createElement('div');
      control.id = CONTROL_ID;
      control.style.cssText = 'display:flex;align-items:center;flex-wrap:wrap;gap:10px;margin:0 0 12px';

      const fileInput = document.createElement('input');
      fileInput.type = 'file';
      fileInput.accept = '.toml,text/plain,application/toml';
      fileInput.dataset.jobFile = '';
      fileInput.style.cssText = 'max-width:260px;color:var(--muted);font:12px ui-monospace,SFMono-Regular,Menlo,Consolas,monospace';

      const jobButton = document.createElement('button');
      jobButton.type = 'button';
      jobButton.dataset.action = 'job';
      jobButton.textContent = 'Submit job';
      jobButton.style.cssText = 'border:1px solid #5cd5ff;background:transparent;color:#5cd5ff;border-radius:3px;padding:6px 10px;font:600 12px ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;cursor:pointer';

      const killButton = document.createElement('button');
      killButton.type = 'button';
      killButton.dataset.action = 'kill';
      killButton.style.cssText = 'border:1px solid #ef4444;background:transparent;color:#ef4444;border-radius:3px;padding:6px 10px;font:600 12px ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;cursor:pointer';

      const message = document.createElement('span');
      message.dataset.message = '';
      message.style.cssText = 'font:12px ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;color:var(--muted)';

      control.append(fileInput, jobButton, killButton, message);
      heading.after(control);
    }

    const fileInput = control.querySelector('[data-job-file]');
    const jobButton = control.querySelector('[data-action="job"]');
    const killButton = control.querySelector('[data-action="kill"]');
    const message = control.querySelector('[data-message]');
    const terminal = node.phase === 'stopped' || node.phase === 'orphan';
    const pending = node.phase === 'kill_requested' || node.phase === 'stopping';
    const jobReady = node.phase === 'running' && Boolean(node.runtime?.job_actor);
    const jobActive = job.state === 'started' || job.state === 'running';
    const selectedJob = selectedJobs.get(logicalNodeId);

    fileInput.hidden = !jobReady;
    jobButton.hidden = !jobReady;
    jobButton.disabled = jobActive || !selectedJob;
    jobButton.textContent = jobActive ? 'Job active' : 'Submit job';
    killButton.hidden = terminal;
    killButton.disabled = pending;
    killButton.textContent = pending ? 'Kill requested' : 'Kill';
    killButton.style.opacity = pending ? '.55' : '1';

    if (job.state !== 'idle') {
      message.textContent = job.message;
    } else if (selectedJob) {
      message.textContent = `Selected ${selectedJob.name}`;
    } else if (terminal) {
      message.textContent = `managed node ${logicalNodeId}: ${node.phase}`;
    } else {
      message.textContent = 'Select a job TOML file';
    }

    fileInput.onchange = async () => {
      const file = fileInput.files?.[0];
      if (!file) {
        selectedJobs.delete(logicalNodeId);
        jobButton.disabled = true;
        message.textContent = 'Select a job TOML file';
        return;
      }
      try {
        const text = await file.text();
        selectedJobs.set(logicalNodeId, { name: file.name, text });
        jobButton.disabled = jobActive;
        message.textContent = `Selected ${file.name}`;
      } catch (error) {
        selectedJobs.delete(logicalNodeId);
        jobButton.disabled = true;
        message.textContent = `Could not read job file: ${error.message}`;
      }
    };

    jobButton.onclick = async () => {
      const selected = selectedJobs.get(logicalNodeId);
      if (!selected) return;
      jobButton.disabled = true;
      message.textContent = 'Submitting job…';
      try {
        const response = await fetch(`/api/control/nodes/${logicalNodeId}/job`, {
          method: 'POST',
          headers: { 'content-type': 'application/toml; charset=utf-8' },
          body: selected.text,
        });
        const body = await response.json().catch(() => ({}));
        if (!response.ok) throw new Error(body.error || `HTTP ${response.status}`);
        message.textContent = body.message || 'Job started';
      } catch (error) {
        jobButton.disabled = false;
        message.textContent = error.message;
      }
    };

    killButton.onclick = async () => {
      if (!await confirmKill(logicalNodeId)) return;
      killButton.disabled = true;
      message.textContent = 'Submitting kill…';
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
        killButton.disabled = false;
        message.textContent = error.message;
      }
    };
  }

  const page = document.getElementById('page');
  if (page) new MutationObserver(syncControl).observe(page, { childList: true, subtree: true });
  syncControl();
  setInterval(syncControl, 1000);
})();
