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
    API: { Models: { getResponseModelDisplayName: () => '' } },
    Utils: {
      Validation: {
        normalizeSearchPayload: value => value || { queries: [], sources: [] }
      }
    }
  }
};

require(path.resolve(__dirname, '../frontend/js/chat/streaming.js'));
const streaming = window.NotionAI.Chat.Streaming;
const wrapper = { thinkingText: '' };
const search = { queries: [], sources: [] };

function payload(content = null, finishReason = null) {
  return JSON.stringify({
    choices: [{
      delta: content === null ? {} : { content },
      finish_reason: finishReason
    }]
  });
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
            return {
              done: false,
              value: new TextEncoder().encode(textChunks[index++])
            };
          }
        };
      }
    }
  };
}

(async () => {
  assert.strictEqual(typeof streaming.createTerminalState, 'function');
  assert.strictEqual(typeof streaming.validateTerminalState, 'function');

  const clean = await streaming.processStream(
    responseFrom([
      `data: ${payload('clean')}\n\ndata: ${payload(null, 'stop')}\n\ndata: [DONE]\n\n`
    ]),
    wrapper,
    search,
    '',
    '',
    'model'
  );
  assert.strictEqual(clean.fullAiReply, 'clean');
  assert.strictEqual(clean.terminalState.done, true);

  updates.length = 0;
  await assert.rejects(
    streaming.processStream(
      responseFrom([
        `data: ${payload('partial')}\n\ndata: ${JSON.stringify({
          type: 'stream_error',
          error: { code: 'ERR_STREAM', message: 'failed' }
        })}\n\n`
      ]),
      wrapper,
      search,
      '',
      '',
      'model'
    ),
    error => error.errorCode === 'ERR_STREAM'
  );
  assert.strictEqual(updates.at(-1), '');

  await assert.rejects(
    streaming.processStream(
      responseFrom([`data: ${payload('partial')}\n\n`]),
      wrapper,
      search,
      '',
      '',
      'model'
    ),
    error => error.errorCode === 'ERR_STREAM_INCOMPLETE'
  );

  console.log('browser consumer terminal contract: passed');
})().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
