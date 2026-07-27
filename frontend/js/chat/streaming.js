/**
 * Streaming Module
 * Handles SSE streaming responses from backend
 */

// Initialize namespace
window.NotionAI = window.NotionAI || {};
window.NotionAI.Chat = window.NotionAI.Chat || {};

window.NotionAI.Chat.Streaming = {
    /**
     * Streams chat completion response
     * @param {Object} chat - Current chat object
     * @param {string} model - Model ID to use
     * @param {Object} aiWrapper - AI message wrapper element
     * @returns {Promise<Object>} Result with full reply, thinking text, and search data
     */
    async streamResponse(chat, model, aiWrapper) {
        const searchState = { queries: [], sources: [] };
        let thinkingText = '';
        let fullAiReply = '';

        // Prepare messages
        const requestMessages = chat.messages
            .filter(msg => (
                msg &&
                typeof msg === 'object' &&
                (msg.role === 'user' || msg.role === 'assistant')
            ))
            .map(msg => ({
                role: msg.role,
                content: String(msg.content || ''),
                thinking: String(msg.thinking || '')
            }));

        // Create request
        STATE.controller = new AbortController();

        try {
            const response = await fetch(
                `${window.NotionAI.Core.State.get('baseUrl')}/v1/chat/completions`,
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': `Bearer ${window.NotionAI.Core.State.get('apiKey')}`,
                        'X-Client-Type': window.NotionAI.Core.Constants.CLIENT_TYPE
                    },
                    body: JSON.stringify({
                        model: model,
                        messages: requestMessages,
                        conversation_id: chat.conversationId || chat.id || null,
                        stream: true
                    }),
                    signal: STATE.controller.signal
                }
            );

            // Check memory status
            const isMemoryDegraded = window.NotionAI.API.Client.checkMemoryStatus(response);
            if (isMemoryDegraded) {
                this.notifyMemoryDegradedOnce();
            }

            // Extract conversation ID
            const backendConversationId = window.NotionAI.API.Client.getConversationId(response);
            if (backendConversationId && chat.conversationId !== backendConversationId) {
                window.NotionAI.Chat.Storage.updateConversationId(chat.id, backendConversationId);
                chat.conversationId = backendConversationId;
            }

            if (!response.ok) {
                // Read the response body to get structured error info
                let errorInfo = { message: `HTTP Error: ${response.status}`, code: '', suggestion: '' };
                try {
                    const errorBody = await response.json();
                    if (errorBody?.error) {
                        errorInfo.message = errorBody.error.message || errorInfo.message;
                        errorInfo.code = errorBody.error.code || '';
                        errorInfo.suggestion = errorBody.error.suggestion || '';
                        errorInfo.detail = errorBody.error.detail || '';
                    } else if (errorBody?.detail) {
                        // FastAPI HTTPException format
                        errorInfo.message = errorBody.detail;
                    }
                } catch (e) {
                    // Response body is not JSON, use status code
                }

                if (response.status === 401) {
                    errorInfo.message = errorInfo.message || "API KEY doesn't match.";
                    errorInfo.suggestion = errorInfo.suggestion || "Check your API Key in Settings.";
                }

                const err = new Error(errorInfo.message);
                err.errorCode = errorInfo.code;
                err.suggestion = errorInfo.suggestion;
                err.errorDetail = errorInfo.detail;
                err.httpStatus = response.status;
                throw err;
            }

            // Process stream
            const result = await this.processStream(response, aiWrapper, searchState, thinkingText, fullAiReply, model);
            return result;

        } catch (err) {
            if (err.name !== 'AbortError') {
                console.error('API Error:', err);
                window.NotionAI.Chat.Renderer.showErrorCard(
                    aiWrapper,
                    err.message,
                    err.errorCode || '',
                    err.suggestion || '',
                    err.errorDetail || '',
                    err.httpStatus || 0
                );
            }
            throw err;
        }
    },

    /**
     * Processes SSE stream from response
     * @param {Response} response - Fetch response
     * @param {Object} aiWrapper - AI message wrapper
     * @param {Object} searchState - Search state object
     * @param {string} thinkingText - Thinking text accumulator
     * @param {string} fullAiReply - Reply text accumulator
     * @param {string} model - Requested model ID
     * @returns {Promise<Object>} Final result object
     */
    createTerminalState() {
        return {
            finishReason: null,
            finishCount: 0,
            done: false,
            failure: null
        };
    },

    streamFailure(message, code = 'incomplete_terminal_state', detail = '') {
        const error = new Error(message);
        error.errorCode = code;
        error.errorDetail = detail;
        return error;
    },

    validateTerminalState(terminalState) {
        const successfulReasons = new Set(['stop', 'length', 'tool_calls', 'function_call']);
        if (terminalState.failure) {
            throw this.streamFailure(
                terminalState.failure.message || 'The stream reported a terminal error.',
                terminalState.failure.code || 'stream_terminal_error',
                terminalState.failure.detail || ''
            );
        }
        if (!terminalState.done || terminalState.finishCount !== 1 || !successfulReasons.has(terminalState.finishReason)) {
            throw this.streamFailure(
                'The stream ended before a valid finish reason and [DONE] receipt.',
                'incomplete_terminal_state'
            );
        }
    },

    async processStream(response, aiWrapper, searchState, thinkingText, fullAiReply, model) {
        const reader = response.body.getReader();
        const decoder = new TextDecoder('utf-8');
        const modelState = { metadata: null, displayName: null, requestedModel: model };
        const terminalState = this.createTerminalState();
        let sseBuffer = '';

        while (!terminalState.done && !terminalState.failure) {
            const { done, value } = await reader.read();
            if (done) break;

            sseBuffer += decoder.decode(value, { stream: true });
            const events = sseBuffer.split('\n\n');
            sseBuffer = events.pop() || '';

            for (const eventBlock of events) {
                if (terminalState.done || terminalState.failure) break;
                for (let line of eventBlock.split('\n')) {
                    if (terminalState.done || terminalState.failure) break;
                    line = line.trim();
                    if (!line.startsWith('data:')) continue;
                    const payload = line.slice(5).trim();
                    const result = this.consumePayload(
                        payload,
                        aiWrapper,
                        searchState,
                        thinkingText,
                        fullAiReply,
                        modelState,
                        terminalState
                    );
                    thinkingText = result.thinkingText;
                    fullAiReply = result.fullAiReply;
                }
            }
        }

        if (!terminalState.done && !terminalState.failure && sseBuffer.trim().startsWith('data:')) {
            const result = this.consumePayload(
                sseBuffer.trim().slice(5).trim(),
                aiWrapper,
                searchState,
                thinkingText,
                fullAiReply,
                modelState,
                terminalState
            );
            thinkingText = result.thinkingText;
            fullAiReply = result.fullAiReply;
        }

        try {
            this.validateTerminalState(terminalState);
        } catch (error) {
            fullAiReply = '';
            thinkingText = '';
            aiWrapper.thinkingText = '';
            window.NotionAI.Chat.Renderer.updateAIMessage(aiWrapper, '', false);
            throw error;
        }

        return {
            fullAiReply,
            thinkingText,
            searchState,
            modelMetadata: modelState.metadata,
            modelDisplayName: modelState.displayName,
            terminalState
        };
    },

    consumePayload(payload, aiWrapper, searchState, thinkingText, fullAiReply, modelState = null, terminalState = null) {
        const state = terminalState || this.createTerminalState();
        if (!payload || state.done || state.failure) {
            return { thinkingText, fullAiReply, terminalState: state };
        }
        if (payload === '[DONE]') {
            state.done = true;
            return { thinkingText, fullAiReply, terminalState: state };
        }

        let dataObj;
        try {
            dataObj = JSON.parse(payload);
        } catch (e) {
            return { thinkingText, fullAiReply, terminalState: state };
        }
        if (!dataObj || typeof dataObj !== 'object' || Array.isArray(dataObj)) {
            return { thinkingText, fullAiReply, terminalState: state };
        }

        const structuredError = dataObj.error;
        if (dataObj.object === 'error' || dataObj.type === 'stream_error' || structuredError) {
            state.failure = {
                code: structuredError?.code || dataObj.code || 'stream_error',
                message: structuredError?.message || dataObj.message || 'The stream reported an error.',
                detail: structuredError?.detail || ''
            };
            return { thinkingText, fullAiReply, terminalState: state };
        }

        const choice = Array.isArray(dataObj.choices) && dataObj.choices[0] && typeof dataObj.choices[0] === 'object'
            ? dataObj.choices[0]
            : null;
        const finishReason = choice?.finish_reason;
        if (finishReason !== undefined && finishReason !== null) {
            const normalizedReason = String(finishReason);
            if (normalizedReason === 'error' || normalizedReason === 'content_filter') {
                state.finishReason = normalizedReason;
                state.failure = {
                    code: normalizedReason,
                    message: `The stream ended with finish_reason=${normalizedReason}.`
                };
                return { thinkingText, fullAiReply, terminalState: state };
            }
            if (!['stop', 'length', 'tool_calls', 'function_call'].includes(normalizedReason)) {
                state.failure = {
                    code: 'unrecognized_finish_reason',
                    message: `Unsupported finish_reason=${normalizedReason}.`
                };
                return { thinkingText, fullAiReply, terminalState: state };
            }
            state.finishCount += 1;
            state.finishReason = normalizedReason;
            if (state.finishCount !== 1) {
                state.failure = {
                    code: 'multiple_finish_reasons',
                    message: 'The stream emitted more than one successful finish reason.'
                };
                return { thinkingText, fullAiReply, terminalState: state };
            }
        }

        if (dataObj.type === 'model_metadata') {
            const metadata = (dataObj.model_metadata && typeof dataObj.model_metadata === 'object')
                ? dataObj.model_metadata
                : ((dataObj.data && typeof dataObj.data === 'object') ? dataObj.data : {});
            const displayName = window.NotionAI.API.Models.getResponseModelDisplayName(
                metadata,
                dataObj.model || metadata.requested_model || ''
            );
            if (displayName) {
                window.NotionAI.Chat.Renderer.updateModelLabel(aiWrapper, displayName, metadata);
                if (modelState) {
                    modelState.metadata = metadata;
                    modelState.displayName = displayName;
                }
            }
            return { thinkingText, fullAiReply, terminalState: state };
        }

        const embeddedMetadata = (dataObj.model_metadata && typeof dataObj.model_metadata === 'object')
            ? dataObj.model_metadata
            : null;
        const embeddedActual = dataObj.actual_model || embeddedMetadata?.actual_model || embeddedMetadata?.notion_model_name || embeddedMetadata?.notion_step_model || '';
        if (embeddedMetadata || embeddedActual) {
            const metadata = {
                ...(embeddedMetadata || {}),
                ...(embeddedActual ? { actual_model: embeddedActual } : {}),
                requested_model: embeddedMetadata?.requested_model || modelState?.requestedModel || ''
            };
            const displayName = window.NotionAI.API.Models.getResponseModelDisplayName(metadata, modelState?.requestedModel || '');
            window.NotionAI.Chat.Renderer.updateModelLabel(aiWrapper, displayName, metadata);
            if (modelState) {
                modelState.metadata = metadata;
                modelState.displayName = displayName;
            }
        }

        if (dataObj.type === 'search_metadata') {
            this.mergeSearchState(searchState, dataObj.searches || {});
            aiWrapper.searchData = searchState;
            window.NotionAI.Chat.Renderer.updateSearchPanel(aiWrapper);
            return { thinkingText, fullAiReply, terminalState: state };
        }
        if (dataObj.type === 'thinking_chunk') {
            const chunk = typeof dataObj.text === 'string' ? dataObj.text : '';
            if (chunk) {
                thinkingText += chunk;
                aiWrapper.thinkingText = thinkingText;
                window.NotionAI.Chat.Renderer.updateThinkingPanel(aiWrapper);
            }
            return { thinkingText, fullAiReply, terminalState: state };
        }
        if (dataObj.type === 'content_replace') {
            const replacement = typeof dataObj.content === 'string' ? dataObj.content : '';
            if (replacement) {
                fullAiReply = replacement;
                window.NotionAI.Chat.Renderer.updateAIMessage(aiWrapper, fullAiReply, false);
            }
            return { thinkingText, fullAiReply, terminalState: state };
        }
        if (dataObj.type === 'thinking_replace') {
            thinkingText = typeof dataObj.thinking === 'string' ? dataObj.thinking : '';
            aiWrapper.thinkingText = thinkingText;
            window.NotionAI.Chat.Renderer.updateThinkingPanel(aiWrapper);
            return { thinkingText, fullAiReply, terminalState: state };
        }

        const deltaReasoning = choice?.delta?.reasoning_content || '';
        if (deltaReasoning) {
            thinkingText += deltaReasoning;
            aiWrapper.thinkingText = thinkingText;
            window.NotionAI.Chat.Renderer.updateThinkingPanel(aiWrapper);
        }
        const deltaContent = choice?.delta?.content || '';
        if (deltaContent) {
            fullAiReply += deltaContent;
            window.NotionAI.Chat.Renderer.updateAIMessage(aiWrapper, fullAiReply, false);
        }
        return { thinkingText, fullAiReply, terminalState: state };
    },

    /**
     * Merges search state from payload
     * @param {Object} target - Target search state
     * @param {Object} payload - Search payload
     */
    mergeSearchState(target, payload) {
        const normalized = window.NotionAI.Utils.Validation.normalizeSearchPayload(payload);

        normalized.queries.forEach(query => {
            if (!target.queries.includes(query)) {
                target.queries.push(query);
            }
        });

        normalized.sources.forEach(source => {
            const exists = target.sources.some(existing =>
                existing.title === source.title && existing.url === source.url
            );
            if (!exists) {
                target.sources.push(source);
            }
        });
    },

    /**
     * Notifies user of memory degradation (once per session)
     */
    notifyMemoryDegradedOnce() {
        if (!this._memoryNotified) {
            this._memoryNotified = true;
            const banner = document.getElementById('memoryBanner');
            if (banner) {
                banner.classList.remove('hidden');
            }
        }
    }
};
