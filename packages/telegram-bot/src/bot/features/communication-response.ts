import { Composer } from 'grammy';

import { getRpcClientForUser } from '@/src/api/client.js';
import type { Context } from '@/src/bot/context.js';

import { COMMUNICATION_OPTION_PATTERN } from '@/src/bot/handlers/communications/options.js';
import { handleCommunicationResponse } from '@/src/bot/handlers/communications/response.js';
import { logHandle } from '@/src/bot/helpers/logging.js';

const composer = new Composer<Context>();

/**
 * Records the option a user tapped on a communication and clears the message.
 * The answer is stored first so a failed delete never loses it, and the message
 * is removed afterwards, which also prevents a second answer from the same user.
 */
composer.callbackQuery(
  COMMUNICATION_OPTION_PATTERN,
  logHandle('callback-communication-response'),
  async (ctx) => {
    await handleCommunicationResponse(ctx, {
      // Sign with the responding user's identity so the API can attribute the
      // answer from authenticated context instead of trusting request data.
      recordResponse: async ({ communicationId, optionIndex, telegramId }) => {
        const rpcClient = getRpcClientForUser(telegramId);
        return rpcClient.bot.recordCommunicationResponse({
          id: communicationId,
          optionIndex,
        });
      },
    });
  },
);

export { composer as communicationResponseFeature };
