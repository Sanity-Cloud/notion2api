---
name: notion2api-build-ui
description: Build or modify Notion2API web UI widgets and controls. Use for chat controls, settings, modals, upload inputs, model selectors, session/history controls, status displays, responsive layout, accessibility, and wiring frontend behavior to API endpoints.
---

# Build Notion2API UI

Use the existing vanilla HTML, CSS, and JavaScript. Do not add a framework or dependency for a native control or a few lines of DOM code.

## Workflow

1. Trace the live UI path before editing. `frontend/index.html` currently contains the main markup, styles, application logic, and event binding; similarly named modular files may not be loaded by that page.
2. Reuse existing classes, CSS variables, `window.NotionAI` namespaces, state accessors, modal patterns, and API helpers.
3. Use semantic native controls first. Add labels, keyboard behavior, focus handling, ARIA only where native semantics are insufficient, and visible disabled/loading/error states.
4. Wire the control to the narrowest existing endpoint. Add backend behavior only if no endpoint already supports it.
5. Preserve light/dark themes, mobile sidebar behavior, streaming cancellation, and local/persisted chat state.
6. Sanitize rendered model content through the existing Markdown/DOMPurify path; never inject untrusted HTML directly.
7. Validate the interaction manually at narrow and desktop widths and run relevant backend tests when the API path changes.

Native Notion widget types are outside Notion2API's API surface. Build an external embeddable widget only when the user asks for one.
