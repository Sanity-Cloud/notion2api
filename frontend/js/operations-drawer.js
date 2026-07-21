(function () {
  const STORAGE_KEY = 'notion2api_operations_jobs_v1';
  const MAX_JOBS = 30;
  const STALL_MS = 90000;
  const state = { jobs: loadJobs(), tab: 'activity', sessionQuery: '', timer: null };

  function loadJobs() {
    try {
      const jobs = JSON.parse(localStorage.getItem(STORAGE_KEY) || '[]');
      if (!Array.isArray(jobs)) return [];
      return jobs.slice(0, MAX_JOBS).map(job =>
        ['queued', 'running', 'stalled'].includes(job.status)
          ? { ...job, status: 'interrupted', finishedAt: job.finishedAt || Date.now() }
          : job
      );
    } catch (_) {
      return [];
    }
  }

  function saveJobs() {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(state.jobs.slice(0, MAX_JOBS)));
  }

  function currentChats() {
    return window.NotionAI?.Core?.State?.get('chats') || [];
  }

  function currentChat() {
    const id = window.NotionAI?.Core?.State?.get('currentChatId');
    return currentChats().find(chat => chat.id === id) || null;
  }

  function id() {
    return globalThis.crypto?.randomUUID?.() || `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  }

  function short(value) {
    const text = String(value || '');
    return text.length > 18 ? `${text.slice(0, 8)}…${text.slice(-6)}` : text || 'Not bound';
  }

  function latestAssistant(chat) {
    return [...(chat?.messages || [])].reverse().find(message => message.role === 'assistant' && String(message.content || '').trim());
  }

  function latestBinding(chat) {
    const message = [...(chat?.messages || [])].reverse().find(item => item?.modelMetadata?.notion_thread_id);
    return message?.modelMetadata?.notion_thread_id || '';
  }

  function elapsed(job) {
    const end = job.finishedAt || Date.now();
    const seconds = Math.max(0, Math.round((end - job.startedAt) / 1000));
    return `${Math.floor(seconds / 60).toString().padStart(2, '0')}:${(seconds % 60).toString().padStart(2, '0')}`;
  }

  function setJob(jobId, changes) {
    const job = state.jobs.find(item => item.id === jobId);
    if (!job) return;
    Object.assign(job, changes);
    saveJobs();
    render();
  }

  function updateStalls() {
    let changed = false;
    state.jobs.forEach(job => {
      if (job.status === 'running' && Date.now() - job.startedAt >= STALL_MS) {
        job.status = 'stalled';
        changed = true;
      }
    });
    if (changed) saveJobs();
    if (document.getElementById('operationsDrawer')?.classList.contains('open')) render();
  }

  function make(tag, className, text) {
    const element = document.createElement(tag);
    if (className) element.className = className;
    if (text !== undefined) element.textContent = text;
    return element;
  }

  function action(label, handler, danger = false) {
    const button = make('button', `operations-action${danger ? ' danger' : ''}`, label);
    button.type = 'button';
    button.addEventListener('click', handler);
    return button;
  }

  function statusLabel(status) {
    return ({
      queued: 'Queued', running: 'Running', stalled: 'Stalled', completed: 'Completed',
      failed: 'Failed', cancelled: 'Cancelled', interrupted: 'Interrupted'
    })[status] || status;
  }

  function renderJobs(container) {
    if (!state.jobs.length) {
      const empty = make('div', 'operations-empty');
      empty.append(make('strong', '', 'No browser requests yet.'), make('span', '', 'Send a message to see progress, cancellation, and recovery controls here.'));
      container.append(empty);
      return;
    }

    state.jobs.forEach(job => {
      const card = make('article', 'operations-card');
      const header = make('div', 'operations-card-header');
      header.append(make('span', `operations-status ${job.status}`, statusLabel(job.status)), make('time', '', elapsed(job)));
      card.append(header, make('strong', 'operations-title', job.title || 'Untitled request'));
      const meta = make('div', 'operations-meta');
      meta.append(
        make('span', '', `Model: ${job.modelLabel || job.model || 'Default'}`),
        make('span', '', `Conversation: ${short(job.conversationId)}`)
      );
      if (job.threadId) meta.append(make('span', '', `Notion thread: ${short(job.threadId)}`));
      if (job.error) meta.append(make('span', 'operations-error', job.error));
      card.append(meta);

      const actions = make('div', 'operations-actions');
      if (['running', 'stalled'].includes(job.status)) {
        actions.append(action('Cancel', () => {
          if (window.NotionAI?.Core?.State?.get('currentChatId') === job.chatId) {
            window.NotionAI.Core.State.get('controller')?.abort?.();
          }
        }, true));
      }
      if (['completed', 'failed', 'cancelled', 'interrupted', 'stalled'].includes(job.status)) {
        actions.append(action('Open chat', () => {
          const chat = currentChats().find(item => item.id === job.chatId);
          if (chat) window.NotionAI.Chat.Manager.selectChat(chat.id);
          close();
        }));
      }
      if (['failed', 'cancelled', 'interrupted', 'stalled'].includes(job.status)) {
        actions.append(action('Recover local response', () => {
          const chat = currentChats().find(item => item.id === job.chatId);
          if (chat && latestAssistant(chat)) {
            window.NotionAI.Chat.Manager.selectChat(chat.id);
            close();
          } else {
            setJob(job.id, { error: 'No completed response is stored in this browser.' });
          }
        }));
      }
      if (actions.childElementCount) card.append(actions);
      container.append(card);
    });
  }

  function renderSessions(container) {
    const search = make('input', 'operations-search');
    search.type = 'search';
    search.placeholder = 'Search conversations';
    search.setAttribute('aria-label', 'Search conversations');
    search.value = state.sessionQuery;
    search.addEventListener('input', event => { state.sessionQuery = event.target.value; render(); });
    container.append(search);

    const query = state.sessionQuery.trim().toLowerCase();
    const chats = currentChats().filter(chat => !query || String(chat.title || '').toLowerCase().includes(query));
    if (!chats.length) {
      container.append(make('div', 'operations-empty', query ? 'No matching conversations.' : 'No saved conversations yet.'));
      return;
    }

    chats.forEach(chat => {
      const card = make('article', `operations-card${chat.id === window.NotionAI.Core.State.get('currentChatId') ? ' selected' : ''}`);
      card.append(make('strong', 'operations-title', chat.title || 'New chat'));
      const meta = make('div', 'operations-meta');
      meta.append(
        make('span', '', `${(chat.messages || []).length} messages`),
        make('span', '', `Local: ${short(chat.conversationId || chat.id)}`),
        make('span', '', `Notion: ${short(latestBinding(chat))}`)
      );
      card.append(meta);
      const actions = make('div', 'operations-actions');
      actions.append(
        action('Continue', () => { window.NotionAI.Chat.Manager.selectChat(chat.id); close(); }),
        action('Rename', () => { close(); window.NotionAI.UI.Modal.openRenameModal(chat.id); }),
        action('Start fresh', () => { window.NotionAI.Chat.Manager.startNewChat(); close(); })
      );
      card.append(actions);
      container.append(card);
    });
  }

  function render() {
    const content = document.getElementById('operationsContent');
    if (!content) return;
    content.replaceChildren();
    document.querySelectorAll('[data-operations-tab]').forEach(button => {
      const active = button.dataset.operationsTab === state.tab;
      button.classList.toggle('active', active);
      button.setAttribute('aria-selected', String(active));
    });
    if (state.tab === 'activity') renderJobs(content); else renderSessions(content);
    const activeCount = state.jobs.filter(job => ['queued', 'running', 'stalled'].includes(job.status)).length;
    const badge = document.getElementById('operationsBadge');
    if (badge) { badge.textContent = String(activeCount); badge.hidden = activeCount === 0; }
  }

  function open(tab = state.tab) {
    state.tab = tab;
    document.getElementById('operationsDrawer')?.classList.add('open');
    document.getElementById('operationsBackdrop')?.classList.add('open');
    document.getElementById('operationsBtn')?.setAttribute('aria-expanded', 'true');
    render();
    document.getElementById('operationsCloseBtn')?.focus();
  }

  function close() {
    document.getElementById('operationsDrawer')?.classList.remove('open');
    document.getElementById('operationsBackdrop')?.classList.remove('open');
    document.getElementById('operationsBtn')?.setAttribute('aria-expanded', 'false');
    document.getElementById('operationsBtn')?.focus();
  }

  function installStyles() {
    const style = document.createElement('style');
    style.textContent = `
      .operations-launch{position:relative}.operations-badge{min-width:17px;height:17px;padding:0 5px;border-radius:999px;background:#b42318;color:#fff;font-size:10px;display:inline-flex;align-items:center;justify-content:center;margin-left:auto}.operations-badge[hidden]{display:none}
      .operations-backdrop{position:fixed;inset:0;background:rgba(0,0,0,.28);opacity:0;pointer-events:none;transition:opacity .16s;z-index:109}.operations-backdrop.open{opacity:1;pointer-events:auto}
      .operations-drawer{position:fixed;top:0;right:0;bottom:0;width:min(430px,94vw);background:var(--bg-sidebar);border-left:1px solid var(--border);box-shadow:-18px 0 42px rgba(0,0,0,.18);transform:translateX(102%);transition:transform .18s ease-out;z-index:110;display:flex;flex-direction:column;color:var(--text)}.operations-drawer.open{transform:translateX(0)}
      .operations-header{display:flex;align-items:center;justify-content:space-between;padding:18px 18px 12px;border-bottom:1px solid var(--border)}.operations-header h2{font-size:16px;margin:0}.operations-close{border:0;background:transparent;color:var(--text-secondary);font-size:24px;line-height:1;padding:4px 7px;border-radius:6px}.operations-close:hover{background:var(--bg-hover);color:var(--text)}
      .operations-tabs{display:flex;padding:10px 18px 0;gap:6px;border-bottom:1px solid var(--border)}.operations-tab{border:0;border-bottom:2px solid transparent;background:transparent;color:var(--text-secondary);padding:8px 10px}.operations-tab.active{color:var(--text);border-bottom-color:var(--border-active)}
      .operations-content{padding:14px 18px 24px;overflow:auto;display:flex;flex-direction:column;gap:10px}.operations-card{border:1px solid var(--border);border-radius:9px;background:var(--card-bg);padding:12px;display:flex;flex-direction:column;gap:8px}.operations-card.selected{border-color:var(--border-active)}
      .operations-card-header{display:flex;align-items:center;justify-content:space-between;gap:8px;color:var(--text-tertiary);font-size:11px}.operations-status{display:inline-flex;align-items:center;gap:5px;font-weight:650}.operations-status:before{content:'';width:7px;height:7px;border-radius:50%;background:#667085}.operations-status.running:before{background:#1570ef}.operations-status.stalled:before,.operations-status.interrupted:before{background:#dc6803}.operations-status.completed:before{background:#079455}.operations-status.failed:before,.operations-status.cancelled:before{background:#d92d20}
      .operations-title{font-size:13px;line-height:1.35}.operations-meta{display:flex;flex-direction:column;gap:3px;color:var(--text-tertiary);font-size:11px;overflow-wrap:anywhere}.operations-error{color:#b42318}.operations-actions{display:flex;gap:6px;flex-wrap:wrap}.operations-action{border:1px solid var(--border);border-radius:6px;background:var(--bg-secondary);color:var(--text);padding:6px 8px;font-size:11px}.operations-action:hover{border-color:var(--border-hover);background:var(--bg-hover)}.operations-action.danger{color:#b42318}
      .operations-search{width:100%;border:1px solid var(--border);border-radius:7px;background:var(--bg-secondary);color:var(--text);padding:9px 10px;font:inherit}.operations-empty{min-height:130px;border:1px dashed var(--border);border-radius:9px;padding:22px;text-align:center;color:var(--text-secondary);display:flex;flex-direction:column;gap:7px;justify-content:center}
      @media(max-width:700px){.operations-drawer{width:100vw}.operations-content{padding-left:14px;padding-right:14px}}
      @media(prefers-reduced-motion:reduce){.operations-drawer,.operations-backdrop{transition:none}}
    `;
    document.head.append(style);
  }

  function installUI() {
    installStyles();
    const footer = document.querySelector('.sidebar-footer');
    if (!footer || document.getElementById('operationsBtn')) return;
    const button = make('button', 'sidebar-footer-btn operations-launch');
    button.id = 'operationsBtn';
    button.type = 'button';
    button.setAttribute('aria-controls', 'operationsDrawer');
    button.setAttribute('aria-expanded', 'false');
    button.append(make('span', '', 'Operations'));
    const badge = make('span', 'operations-badge'); badge.id = 'operationsBadge'; badge.hidden = true; button.append(badge);
    footer.prepend(button);

    const backdrop = make('div', 'operations-backdrop'); backdrop.id = 'operationsBackdrop'; backdrop.addEventListener('click', close);
    const drawer = make('aside', 'operations-drawer'); drawer.id = 'operationsDrawer'; drawer.setAttribute('aria-label', 'Operations');
    const header = make('div', 'operations-header'); header.append(make('h2', '', 'Operations'));
    const closeButton = make('button', 'operations-close', '×'); closeButton.id = 'operationsCloseBtn'; closeButton.type = 'button'; closeButton.setAttribute('aria-label', 'Close operations'); closeButton.addEventListener('click', close); header.append(closeButton);
    const tabs = make('div', 'operations-tabs'); tabs.setAttribute('role', 'tablist');
    [['activity', 'Activity'], ['sessions', 'Sessions']].forEach(([key, label]) => {
      const tab = make('button', 'operations-tab', label); tab.type = 'button'; tab.dataset.operationsTab = key; tab.setAttribute('role', 'tab'); tab.addEventListener('click', () => { state.tab = key; render(); }); tabs.append(tab);
    });
    const content = make('div', 'operations-content'); content.id = 'operationsContent';
    drawer.append(header, tabs, content);
    document.body.append(backdrop, drawer);
    button.addEventListener('click', () => drawer.classList.contains('open') ? close() : open());
    document.addEventListener('keydown', event => { if (event.key === 'Escape' && drawer.classList.contains('open')) close(); });
    render();
  }

  function instrumentStreaming() {
    const streaming = window.NotionAI?.Chat?.Streaming;
    if (!streaming || streaming.__operationsInstrumented) return;
    const original = streaming.streamResponse;
    streaming.streamResponse = async function (chat, model, aiWrapper, attachments) {
      const job = {
        id: id(), chatId: chat.id, conversationId: chat.conversationId || chat.id || '',
        title: chat.title || 'Untitled request', model, modelLabel: window.NotionAI.API.Models.getCurrentModelLabel(),
        status: 'running', startedAt: Date.now(), finishedAt: null, threadId: '', error: ''
      };
      state.jobs.unshift(job); state.jobs = state.jobs.slice(0, MAX_JOBS); saveJobs(); render();
      try {
        const result = await original.call(this, chat, model, aiWrapper, attachments);
        setJob(job.id, {
          status: 'completed', finishedAt: Date.now(), conversationId: chat.conversationId || job.conversationId,
          threadId: result?.modelMetadata?.notion_thread_id || '', modelLabel: result?.modelDisplayName || job.modelLabel
        });
        return result;
      } catch (error) {
        setJob(job.id, { status: error?.name === 'AbortError' ? 'cancelled' : 'failed', finishedAt: Date.now(), error: error?.name === 'AbortError' ? '' : String(error?.message || 'Request failed') });
        throw error;
      }
    };
    streaming.__operationsInstrumented = true;
  }

  function init() {
    installUI();
    instrumentStreaming();
    state.timer = setInterval(updateStalls, 1000);
  }

  window.NotionAI = window.NotionAI || {};
  window.NotionAI.Operations = { open, close, render, getJobs: () => state.jobs.map(job => ({ ...job })) };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
})();
