(function () {
  const script = document.currentScript;
  const base = String(script?.dataset.baseUrl || new URL(script.src).origin).replace(/\/$/, '');
  const title = script?.dataset.title || 'Research Assistant';
  const model = script?.dataset.model || 'terra';
  const position = script?.dataset.position === 'left' ? 'left' : 'right';
  const theme = script?.dataset.theme || 'auto';
  const root = document.createElement('div');
  const button = document.createElement('button');
  const frame = document.createElement('iframe');
  button.type = 'button'; button.textContent = script?.dataset.buttonLabel || 'Ask AI'; button.setAttribute('aria-expanded', 'false');
  button.style.cssText = `position:fixed;${position}:20px;bottom:20px;z-index:2147483646;border:0;border-radius:999px;padding:12px 17px;background:#202020;color:#fff;font:600 14px/1.2 ui-sans-serif,Segoe UI,sans-serif;box-shadow:0 8px 28px rgba(0,0,0,.24);cursor:pointer`;
  frame.title = title; frame.hidden = true; frame.allow = 'clipboard-write';
  frame.src = `${base}/embed.html?title=${encodeURIComponent(title)}&model=${encodeURIComponent(model)}&theme=${encodeURIComponent(theme)}`;
  frame.style.cssText = `position:fixed;${position}:20px;bottom:72px;width:min(390px,calc(100vw - 24px));height:min(620px,calc(100vh - 96px));z-index:2147483646;border:1px solid rgba(0,0,0,.16);border-radius:14px;background:#fff;box-shadow:0 18px 55px rgba(0,0,0,.28)`;
  button.addEventListener('click', () => { frame.hidden = !frame.hidden; button.setAttribute('aria-expanded', String(!frame.hidden)); });
  root.append(frame, button); document.body.append(root);
})();
