import assert from 'node:assert/strict';
import test from 'node:test';

import type { CallbackQueryContext } from 'grammy';

import { handleCommunicationResponse } from '../handlers/communications/response.js';

import type { Context } from '@/src/bot/context.js';

interface TestContextState {
  acknowledgements: Array<{ text?: string } | undefined>;
  deleteCount: number;
  errors: unknown[][];
}

/**
 * Creates the callback context and observable state used by response-handler tests.
 */
function createTestContext() {
  const state: TestContextState = {
    acknowledgements: [],
    deleteCount: 0,
    errors: [],
  };

  // This context represents Telegram user 123 tapping the primary option of communication 12.
  const ctx = {
    callbackQuery: { data: 'comm:12:0' },
    from: { id: 123 },
    logger: {
      error: (...args: unknown[]) => state.errors.push(args),
    },
    t: (key: string) => key,
    answerCallbackQuery: async (options?: { text?: string }) => {
      state.acknowledgements.push(options);
    },
    deleteMessage: async () => {
      state.deleteCount += 1;
    },
  } as unknown as CallbackQueryContext<Context>;

  return { ctx, state };
}

// A transient API failure must leave the buttons available so the user can retry
// and must not claim that an answer was saved when persistence rejected it.
test('handleCommunicationResponse preserves the message when persistence fails', async () => {
  // The callback context tracks acknowledgements, deletion attempts, and logged errors.
  const { ctx, state } = createTestContext();
  const recordedInputs: unknown[] = [];

  // This dependency simulates the API returning a structured database failure.
  await handleCommunicationResponse(ctx, {
    recordResponse: async (input) => {
      recordedInputs.push(input);
      return { success: false, error: { message: 'database unavailable' } };
    },
  });

  // These assertions verify attribution uses the Telegram sender and failure remains recoverable.
  assert.deepEqual(recordedInputs, [{ communicationId: 12, optionIndex: 0, telegramId: '123' }]);
  assert.deepEqual(state.acknowledgements, [{ text: 'communication-response-failed' }]);
  assert.equal(state.deleteCount, 0);
  assert.equal(state.errors.length, 1);
});

// A transport exception is another persistence failure and must provide the same
// retry path as a structured API error instead of leaving the callback spinning.
test('handleCommunicationResponse preserves the message when the API request throws', async () => {
  // The callback context tracks the failure acknowledgement and confirms no deletion occurs.
  const { ctx, state } = createTestContext();

  // This dependency simulates a network failure before the API can return a response.
  await handleCommunicationResponse(ctx, {
    recordResponse: async () => {
      throw new Error('network unavailable');
    },
  });

  // These assertions verify thrown failures are visible and leave the answer buttons intact.
  assert.deepEqual(state.acknowledgements, [{ text: 'communication-response-failed' }]);
  assert.equal(state.deleteCount, 0);
  assert.equal(state.errors.length, 1);
});

// A persisted response should acknowledge success and clear the keyboard so the
// user sees that the interaction completed and cannot casually submit it again.
test('handleCommunicationResponse deletes the message after persistence succeeds', async () => {
  // The callback context tracks the success acknowledgement and message deletion.
  const { ctx, state } = createTestContext();

  // This dependency simulates the API successfully storing the selected option.
  await handleCommunicationResponse(ctx, {
    recordResponse: async () => ({ success: true }),
  });

  // These assertions verify deletion happens only on the successful branch.
  assert.deepEqual(state.acknowledgements, [{ text: 'communication-response-recorded' }]);
  assert.equal(state.deleteCount, 1);
  assert.equal(state.errors.length, 0);
});
