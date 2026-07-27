'use strict';
const assert = require('assert');
const path = require('path');

global.STATE = { controller: null };
const updates = [];
global.window = {
  NotionAI: {
    Chat: {
      Renderer: {
        updateAIMessage: (_wrapper, text) => updates.push(text),
        updateThinkingPanel: () => {},
        updateModelLabel: () => {},
        updateSearchPanel: () => {},
        showErrorCard: () => {}
      }
    },
    API: { Models: { getResponseModelDisplayName: () => '' } },
    Utils: { Validation: { normalizeSearchPayload: value => value || { queries: [], sources: [] } } }
  }
};
require(path.resolve(__dirname, '../../frontend/js/chat/streaming.js'));
const streaming = window.NotionAI.Chat.Streaming;
const baseUrl = process.env.SMOKE_BASE_URL;
assert(baseUrl);

async function response(caseName) {
  return fetch(`${baseUrl}/v1/chat/completions`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', accept: 'text/event-stream' },
    body: JSON.stringify({ model: caseName, messages: [] })
  });
}

(async () => {
  const wrapper = { thinkingText: '' };
  const clean = await streaming.processStream(
    await response('smoke-clean'), wrapper, { queries: [], sources: [] }, '', '', 'smoke-clean'
  );
  assert.strictEqual(clean.fullAiReply, 'smoke-ok');
  assert.strictEqual(clean.terminalState.done, true);
  assert.strictEqual(clean.terminalState.finishCount, 1);

  updates.length = 0;
  await assert.rejects(
    streaming.processStream(
      await response('smoke-cleanup'), wrapper, { queries: [], sources: [] }, '', '', 'smoke-cleanup'
    ),
    error => error.errorCode === 'ERR_STREAM_SOURCE_CLEANUP'
  );
  assert.strictEqual(updates.at(-1), '');

  updates.length = 0;
  await assert.rejects(
    streaming.processStream(
      await response('smoke-interrupted'), wrapper, { queries: [], sources: [] }, '', '', 'smoke-interrupted'
    ),
    error => error.errorCode === 'ERR_STREAM_INTERRUPTED'
  );
  assert.strictEqual(updates.at(-1), '');
  console.log('isolated browser HTTP smoke: passed');
})().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
