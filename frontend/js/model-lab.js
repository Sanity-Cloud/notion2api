(function () {
  const state = { models: [], controllers: [] };

  function make(tag, className, text) {
    const el = document.createElement(tag);
    if (className) el.className = className;
    if (text !== undefined) el.textContent = text;
    return el;
  }

  function headers() {
    return {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${window.NotionAI.Core.State.get('apiKey')}`,
      'X-Client-Type': window.NotionAI.Core.Constants.CLIENT_TYPE
    };
  }

  function open() {
    document.getElementById('modelLabModal')?.classList.remove('hidden');
    loadModels();
    document.getElementById('modelLabPrompt')?.focus();
  }

  function close() {
    state.controllers.forEach(controller => controller.abort());
    state.controllers = [];
    document.getElementById('modelLabModal')?.classList.add('hidden');
    document.getElementById('modelLabLauncher')?.focus();
  }

  async function loadModels() {
    if (state.models.length) return;
    const status = document.getElementById('modelLabStatus');
    try {
      const response = await window.NotionAI.API.Client.get('/v1/models');
      if (!response.ok) throw new Error(`Models request failed (${response.status})`);
      const payload = await response.json();
      state.models = Array.isArray(payload?.data) ? payload.data : [];
      const selects = ['modelLabA', 'modelLabB'].map(id => document.getElementById(id));
      selects.forEach((select, index) => {
        select.replaceChildren();
        state.models.forEach((model, modelIndex) => {
          const option = make('option', '', model.name || model.id);
          option.value = model.id;
          option.selected = modelIndex === Math.min(index, Math.max(0, state.models.length - 1));
          select.append(option);
        });
      });
      if (status) status.textContent = `${state.models.length} models available.`;
    } catch (error) {
      if (status) status.textContent = error.message;
    }
  }

  function resultText(payload) {
    const content = payload?.choices?.[0]?.message?.content;
    if (typeof content === 'string') return content;
    if (Array.isArray(content)) return content.map(item => item?.text || '').join('');
    return '';
  }

  function actualModel(payload, requested) {
    const metadata = payload?.model_metadata || payload?.choices?.[0]?.message?.model_metadata || {};
    return metadata.actual_model_display_name || metadata.actual_model || payload?.model || requested;
  }

  async function runSide(model, prompt, output, meta) {
    const controller = new AbortController();
    state.controllers.push(controller);
    const started = performance.now();
    output.textContent = 'Working…';
    meta.textContent = `Requested: ${model}`;
    try {
      const response = await fetch(`${window.NotionAI.Core.State.get('baseUrl')}/v1/chat/completions`, {
        method: 'POST', headers: headers(), signal: controller.signal,
        body: JSON.stringify({ model, messages: [{ role: 'user', content: prompt }], stream: false, metadata: { persist_remote_chat: false } })
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(payload?.error?.message || payload?.detail?.error?.message || `Request failed (${response.status})`);
      output.textContent = resultText(payload) || 'No visible response received.';
      meta.textContent = `Requested: ${model} · Actual: ${actualModel(payload, model)} · ${Math.round((performance.now() - started) / 100) / 10}s`;
      return { ok: true, model, actual: actualModel(payload, model), text: output.textContent };
    } catch (error) {
      output.textContent = error.name === 'AbortError' ? 'Cancelled.' : error.message;
      meta.textContent = `Requested: ${model} · Failed`;
      return { ok: false, model, error: output.textContent };
    }
  }

  async function compare() {
    const prompt = document.getElementById('modelLabPrompt').value.trim();
    const modelA = document.getElementById('modelLabA').value;
    const modelB = document.getElementById('modelLabB').value;
    const run = document.getElementById('modelLabRun');
    if (!prompt || !modelA || !modelB) return;
    state.controllers.forEach(controller => controller.abort());
    state.controllers = [];
    run.disabled = true;
    const results = await Promise.all([
      runSide(modelA, prompt, document.getElementById('modelLabOutputA'), document.getElementById('modelLabMetaA')),
      runSide(modelB, prompt, document.getElementById('modelLabOutputB'), document.getElementById('modelLabMetaB'))
    ]);
    document.getElementById('modelLabExport').disabled = !results.some(result => result.ok);
    document.getElementById('modelLabExport').onclick = () => {
      const text = `# Model comparison\n\n## Prompt\n${prompt}\n\n## ${modelA}\n${document.getElementById('modelLabMetaA').textContent}\n\n${document.getElementById('modelLabOutputA').textContent}\n\n## ${modelB}\n${document.getElementById('modelLabMetaB').textContent}\n\n${document.getElementById('modelLabOutputB').textContent}`;
      navigator.clipboard.writeText(text);
    };
    run.disabled = false;
    state.controllers = [];
  }

  function install() {
    const style = document.createElement('style');
    style.textContent = `
      .tool-modal .modal-content{width:min(1120px,94vw);height:min(820px,90vh);display:flex;flex-direction:column}.tool-modal .modal-body{overflow:auto;flex:1}.model-lab-controls{display:grid;grid-template-columns:1fr 1fr;gap:12px}.model-lab-prompt{grid-column:1/-1;min-height:110px;resize:vertical}.model-lab-results{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:16px}.model-lab-result{border:1px solid var(--border);border-radius:9px;padding:12px;background:var(--bg-secondary);min-width:0}.model-lab-result h3{font-size:13px;margin:0 0 5px}.model-lab-meta{font-size:11px;color:var(--text-tertiary);margin-bottom:10px;overflow-wrap:anywhere}.model-lab-output{font-size:13px;line-height:1.55;white-space:pre-wrap;overflow-wrap:anywhere}.model-lab-status{font-size:11px;color:var(--text-tertiary);grid-column:1/-1}@media(max-width:760px){.model-lab-controls,.model-lab-results{grid-template-columns:1fr}.model-lab-prompt,.model-lab-status{grid-column:1}}
    `;
    document.head.append(style);

    const footer = document.querySelector('.sidebar-footer');
    const button = make('button', 'sidebar-footer-btn', 'Model Lab');
    button.id = 'modelLabLauncher'; button.type = 'button'; button.addEventListener('click', open); footer?.prepend(button);

    const modal = make('div', 'modal-overlay hidden tool-modal'); modal.id = 'modelLabModal';
    modal.setAttribute('role', 'dialog'); modal.setAttribute('aria-modal', 'true'); modal.setAttribute('aria-labelledby', 'modelLabTitle');
    const box = make('div', 'modal-content');
    const header = make('div', 'modal-header'); const title = make('h2', '', 'Model Lab'); title.id = 'modelLabTitle'; header.append(title);
    const x = make('button', 'modal-close-btn', '×'); x.type = 'button'; x.setAttribute('aria-label', 'Close Model Lab'); x.addEventListener('click', close); header.append(x);
    const body = make('div', 'modal-body');
    const controls = make('div', 'model-lab-controls');
    const prompt = make('textarea', 'form-control model-lab-prompt'); prompt.id = 'modelLabPrompt'; prompt.placeholder = 'Enter one prompt to run against both models'; prompt.setAttribute('aria-label', 'Comparison prompt');
    const selectA = make('select', 'form-control'); selectA.id = 'modelLabA'; selectA.setAttribute('aria-label', 'First model');
    const selectB = make('select', 'form-control'); selectB.id = 'modelLabB'; selectB.setAttribute('aria-label', 'Second model');
    const status = make('div', 'model-lab-status', 'Loading models…'); status.id = 'modelLabStatus';
    controls.append(prompt, selectA, selectB, status);
    const results = make('div', 'model-lab-results');
    [['A', 'modelLabMetaA', 'modelLabOutputA'], ['B', 'modelLabMetaB', 'modelLabOutputB']].forEach(([label, metaId, outputId]) => {
      const card = make('section', 'model-lab-result'); card.append(make('h3', '', `Model ${label}`));
      const meta = make('div', 'model-lab-meta', 'Not run yet.'); meta.id = metaId;
      const output = make('div', 'model-lab-output', 'Response will appear here.'); output.id = outputId; card.append(meta, output); results.append(card);
    });
    body.append(controls, results);
    const footerActions = make('div', 'modal-footer');
    const exportButton = make('button', 'btn-secondary', 'Copy comparison'); exportButton.id = 'modelLabExport'; exportButton.type = 'button'; exportButton.disabled = true;
    const runButton = make('button', 'btn-primary', 'Run comparison'); runButton.id = 'modelLabRun'; runButton.type = 'button'; runButton.addEventListener('click', compare);
    footerActions.append(exportButton, runButton); box.append(header, body, footerActions); modal.append(box); document.body.append(modal);
    modal.addEventListener('click', event => { if (event.target === modal) close(); });
    modal.addEventListener('keydown', event => { if (event.key === 'Escape') close(); });
  }

  window.NotionAI = window.NotionAI || {};
  window.NotionAI.ModelLab = { open, close };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', install); else install();
})();
