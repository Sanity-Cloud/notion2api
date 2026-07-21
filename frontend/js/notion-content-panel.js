(function () {
  function make(tag, className, text) {
    const el = document.createElement(tag);
    if (className) el.className = className;
    if (text !== undefined) el.textContent = text;
    return el;
  }

  function open() {
    document.getElementById('notionContentModal')?.classList.remove('hidden');
    loadAccountInfo();
    document.getElementById('notionContentBody')?.focus();
  }

  function close() {
    document.getElementById('notionContentModal')?.classList.add('hidden');
    document.getElementById('notionContentLauncher')?.focus();
  }

  async function json(response) {
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload?.detail?.error?.message || payload?.error?.message || payload?.detail || `Request failed (${response.status})`);
    return payload;
  }

  async function post(path, payload) {
    return json(await window.NotionAI.API.Client.post(path, payload));
  }

  async function loadAccountInfo() {
    const info = document.getElementById('notionContentAccount');
    try {
      const payload = await json(await window.NotionAI.API.Client.get('/v1/notion/account_info'));
      info.textContent = payload.parent_page_accessible
        ? `Default parent ready: ${payload.repo_ai_parent_page_id}`
        : 'No accessible default parent. Enter a parent page ID when creating a page.';
    } catch (error) {
      info.textContent = error.message;
    }
  }

  function blocks(text) {
    const result = [];
    text.split(/\n\s*\n/).map(value => value.trim()).filter(Boolean).forEach(paragraph => {
      for (let offset = 0; offset < paragraph.length; offset += 1900) {
        result.push({
          object: 'block', type: 'paragraph',
          paragraph: { rich_text: [{ type: 'text', text: { content: paragraph.slice(offset, offset + 1900) } }] }
        });
      }
    });
    return result;
  }

  async function sync() {
    const mode = document.getElementById('notionContentMode').value;
    const pageInput = document.getElementById('notionContentPage');
    const title = document.getElementById('notionContentTitle').value.trim();
    const content = document.getElementById('notionContentBody').value.trim();
    const filePath = document.getElementById('notionContentFile').value.trim();
    const status = document.getElementById('notionContentStatus');
    const button = document.getElementById('notionContentSync');
    let pageId = pageInput.value.trim();
    if (mode === 'create' && !title) { status.textContent = 'A title is required when creating a page.'; return; }
    if (mode !== 'create' && !pageId) { status.textContent = 'A target page ID is required.'; return; }
    if (!content && !filePath) { status.textContent = 'Add content or a local server file path.'; return; }
    if (mode === 'replace' && !confirm('Replace mode deletes existing child blocks before appending the new content. Continue?')) return;

    button.disabled = true;
    status.textContent = 'Validating target…';
    try {
      if (mode === 'create') {
        const created = await post('/v1/notion/create_page', { title, parent_page_id: pageId || null });
        pageId = created.page_id;
        pageInput.value = pageId;
        status.textContent = `Created ${created.title}.`;
      } else {
        const access = await post('/v1/notion/check_page_access', { page_id: pageId });
        if (!access.accessible) throw new Error(access.error || 'The target page is not accessible.');
      }

      if (mode === 'replace') {
        status.textContent = 'Removing existing child blocks…';
        await post('/v1/notion/delete_block_children', { page_id: pageId, preserve_types: [] });
      }

      const children = blocks(content);
      for (let offset = 0; offset < children.length; offset += 100) {
        const batch = children.slice(offset, offset + 100);
        status.textContent = `Appending blocks ${offset + 1}–${offset + batch.length} of ${children.length}…`;
        await post('/v1/notion/append_blocks', { page_id: pageId, children: batch });
      }

      if (filePath) {
        status.textContent = 'Uploading server-local file…';
        await post('/v1/notion/upload_file', { page_id: pageId, file_path: filePath });
      }
      status.textContent = `Sync complete. Page ${pageId}.`;
    } catch (error) {
      status.textContent = error.message;
    } finally {
      button.disabled = false;
    }
  }

  function install() {
    const style = document.createElement('style');
    style.textContent = `
      .notion-content-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}.notion-content-wide{grid-column:1/-1}.notion-content-grid label{display:flex;flex-direction:column;gap:6px;font-size:12px;color:var(--text-secondary)}.notion-content-grid input,.notion-content-grid select,.notion-content-grid textarea{border:1px solid var(--border);border-radius:7px;background:var(--bg-secondary);color:var(--text);padding:9px 10px;font:inherit}.notion-content-grid textarea{min-height:240px;resize:vertical}.notion-content-note,.notion-content-status{font-size:11px;line-height:1.5;color:var(--text-tertiary)}.notion-content-status{min-height:18px;color:var(--text-secondary)}@media(max-width:700px){.notion-content-grid{grid-template-columns:1fr}.notion-content-wide{grid-column:1}}
    `;
    document.head.append(style);
    const footer = document.querySelector('.sidebar-footer');
    const button = make('button', 'sidebar-footer-btn', 'Notion Content'); button.id = 'notionContentLauncher'; button.type = 'button'; button.addEventListener('click', open); footer?.prepend(button);

    const modal = make('div', 'modal-overlay hidden tool-modal'); modal.id = 'notionContentModal';
    modal.setAttribute('role', 'dialog'); modal.setAttribute('aria-modal', 'true'); modal.setAttribute('aria-labelledby', 'notionContentTitle');
    const box = make('div', 'modal-content');
    const header = make('div', 'modal-header'); const heading = make('h2', '', 'Notion Content'); heading.id = 'notionContentTitle'; header.append(heading);
    const x = make('button', 'modal-close-btn', '×'); x.type = 'button'; x.setAttribute('aria-label', 'Close Notion Content'); x.addEventListener('click', close); header.append(x);
    const body = make('div', 'modal-body');
    const grid = make('div', 'notion-content-grid');
    const modeLabel = make('label', '', 'Action'); const mode = make('select'); mode.id = 'notionContentMode'; [['append','Append to page'],['create','Create child page'],['replace','Replace page children']].forEach(([value,label]) => { const option = make('option','',label); option.value=value; mode.append(option); }); modeLabel.append(mode);
    const pageLabel = make('label', '', 'Page or parent ID'); const page = make('input'); page.id = 'notionContentPage'; page.placeholder = 'Required for append/replace; optional default for create'; pageLabel.append(page);
    const titleLabel = make('label', '', 'New page title'); const title = make('input'); title.id = 'notionContentTitle'; title.placeholder = 'Required only for create'; titleLabel.append(title);
    const fileLabel = make('label', '', 'Local server file path'); const file = make('input'); file.id = 'notionContentFile'; file.placeholder = 'Optional path readable by notion2api'; fileLabel.append(file);
    const contentLabel = make('label', 'notion-content-wide', 'Content preview'); const content = make('textarea'); content.id = 'notionContentBody'; content.placeholder = 'Paragraphs separated by blank lines'; contentLabel.append(content);
    const account = make('div', 'notion-content-note notion-content-wide', 'Checking the configured Notion account…'); account.id = 'notionContentAccount';
    const note = make('div', 'notion-content-note notion-content-wide', 'Replace is destructive and always asks for confirmation. Browser files are not sent here; the optional file path must exist on the notion2api server.');
    const status = make('div', 'notion-content-status notion-content-wide'); status.id = 'notionContentStatus'; status.setAttribute('role', 'status');
    grid.append(modeLabel, pageLabel, titleLabel, fileLabel, contentLabel, account, note, status); body.append(grid);
    const actions = make('div', 'modal-footer'); const syncButton = make('button', 'btn-primary', 'Sync to Notion'); syncButton.id = 'notionContentSync'; syncButton.type = 'button'; syncButton.addEventListener('click', sync); actions.append(syncButton);
    box.append(header, body, actions); modal.append(box); document.body.append(modal); modal.addEventListener('click', event => { if (event.target === modal) close(); });
    modal.addEventListener('keydown', event => { if (event.key === 'Escape') close(); });
  }

  window.NotionAI = window.NotionAI || {};
  window.NotionAI.NotionContent = { open, close, blocks };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', install); else install();
})();
