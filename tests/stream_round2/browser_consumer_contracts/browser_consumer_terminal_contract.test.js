'use strict';
const assert = require('assert');
const path = require('path');

const updates = [];
global.STATE = { controller: null };
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
    API: {
      Models: { getResponseModelDisplayName: () => '' }
    },
    Utils: {
      Validation: { normalizeSearchPayload: value => value || { queries: [], sources: [] } }
    }
  }
};
require(path.resolve(__dirname, '../../../frontend/js/chat/streaming.js'));
const streaming = window.NotionAI.Chat.Streaming;
const wrapper = { thinkingText: '' };
const search = { queries: [], sources: [] };

function payload(content = null, finishReason = null, extra = {}) {
  return JSON.stringify({
    ...extra,
    choices: [{ delta: content === null ? {} : { content }, finish_reason: finishReason }]
  });
}

for (const finishReason of ['stop', 'length', 'tool_calls', 'function_call']) {
  const state = streaming.createTerminalState();
  let result = streaming.consumePayload(payload('ok'), wrapper, search, '', '', null, state);
  result = streaming.consumePayload(payload(null, finishReason), wrapper, search, result.thinkingText, result.fullAiReply, null, state);
  result = streaming.consumePayload('[DONE]', wrapper, search, result.thinkingText, result.fullAiReply, null, state);
  streaming.validateTerminalState(state);
  assert.strictEqual(result.fullAiReply, 'ok');
  assert.strictEqual(state.finishCount, 1);
  assert.strictEqual(state.finishReason, finishReason);
  assert.strictEqual(state.done, true);
}

for (const terminalPayload of [
  JSON.stringify({ type: 'stream_error', error: { code: 'ERR', message: 'failed' } }),
  JSON.stringify({ object: 'error', error: { code: 'ERR_OBJECT', message: 'failed' } }),
  payload(null, 'error'),
  payload(null, 'content_filter')
]) {
  const state = streaming.createTerminalState();
  streaming.consumePayload(terminalPayload, wrapper, search, '', 'partial', null, state);
  assert.throws(() => streaming.validateTerminalState(state));
}

for (const frames of [
  [payload('partial')],
  ['[DONE]'],
  [payload(null, 'stop')],
  [payload(null, 'stop'), payload(null, 'stop'), '[DONE]']
]) {
  const state = streaming.createTerminalState();
  let thinking = '';
  let content = '';
  for (const frame of frames) {
    const result = streaming.consumePayload(frame, wrapper, search, thinking, content, null, state);
    thinking = result.thinkingText;
    content = result.fullAiReply;
  }
  assert.throws(() => streaming.validateTerminalState(state));
}

{
  const state = streaming.createTerminalState();
  let result = streaming.consumePayload(payload('kept'), wrapper, search, '', '', null, state);
  result = streaming.consumePayload(payload(null, 'stop'), wrapper, search, result.thinkingText, result.fullAiReply, null, state);
  result = streaming.consumePayload('[DONE]', wrapper, search, result.thinkingText, result.fullAiReply, null, state);
  result = streaming.consumePayload(payload('ignored'), wrapper, search, result.thinkingText, result.fullAiReply, null, state);
  assert.strictEqual(result.fullAiReply, 'kept');
}

function responseFrom(textChunks, finalError = null) {
  let index = 0;
  return {
    body: {
      getReader() {
        return {
          async read() {
            if (finalError && index === textChunks.length) throw finalError;
            if (index >= textChunks.length) return { done: true };
            return { done: false, value: new TextEncoder().encode(textChunks[index++]) };
          }
        };
      }
    }
  };
}

(async () => {
  updates.length = 0;
  const clean = await streaming.processStream(
    responseFrom([`data: ${payload('clean')}\n\ndata: ${payload(null, 'stop')}\n\ndata: [DONE]\n\n`]),
    wrapper,
    { queries: [], sources: [] },
    '',
    '',
    'model'
  );
  assert.strictEqual(clean.fullAiReply, 'clean');
  assert.strictEqual(clean.terminalState.done, true);

  updates.length = 0;
  await assert.rejects(
    streaming.processStream(
      responseFrom([`data: ${payload('partial')}\n\ndata: ${JSON.stringify({ type: 'stream_error', error: { code: 'ERR_STREAM', message: 'failed' } })}\n\n`]),
      wrapper,
      { queries: [], sources: [] },
      '',
      '',
      'model'
    ),
    error => error.errorCode === 'ERR_STREAM'
  );
  assert.strictEqual(updates.at(-1), '');

  const abort = new Error('cancelled');
  abort.name = 'AbortError';
  await assert.rejects(
    streaming.processStream(responseFrom([], abort), wrapper, { queries: [], sources: [] }, '', '', 'model'),
    error => error.name === 'AbortError'
  );

  console.log('browser consumer terminal contract: real parser checks passed');
})().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
