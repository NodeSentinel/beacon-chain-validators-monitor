import { InlineKeyboard } from 'grammy';

import type { CommandContext } from 'grammy';
import { parseCommunicationIdArgument } from './command-id.js';
import { buildCommunicationOptionsKeyboard } from './options.js';
import { countBatchDeliveryResults, splitRecipientsIntoBatches } from './send-batches.js';

import { COMMON_REQUESTS_TELEGRAM_ID, getRpcClientForUser } from '@/src/api/client.js';
import type { Context } from '@/src/bot/context.js';

import { sendMessage } from '@/src/telegram/messaging.js';

/**
 * Sends one stored communication to its resolved audience.
 */
export async function sendCommunicationHandler(ctx: CommandContext<Context>) {
  const communicationId = parseCommunicationIdArgument(ctx.message?.text);

  if (!communicationId) {
    return ctx.reply('Usage: /send_communication <id>');
  }

  // Use the generic bot client because this command is not tied to any user context.
  const rpcClient = getRpcClientForUser(COMMON_REQUESTS_TELEGRAM_ID);
  const communicationResponse = await rpcClient.bot.getCommunication({
    id: communicationId,
  });

  if (!communicationResponse.success || !communicationResponse.data) {
    return ctx.reply(
      communicationResponse.error?.message ?? `Communication ${communicationId} was not found.`,
    );
  }

  // Store the loaded communication after the response guard so the rest of the handler stays typed.
  const communication = communicationResponse.data;

  if (communication.sent) {
    return ctx.reply('already sent');
  }

  // Communications with options ask for an answer; the rest keep the legacy
  // dismiss button so every user can remove the broadcast locally.
  const keyboard =
    communication.options.length > 0
      ? buildCommunicationOptionsKeyboard(communication.id, communication.options)
      : new InlineKeyboard().text('Dismiss', 'remove_message');

  let sentCount = 0;
  let failedCount = 0;

  // Send each batch concurrently while keeping the overall broadcast flow easy to follow.
  for (const recipientBatch of splitRecipientsIntoBatches(communication.recipients, 10)) {
    const deliveryResults = await Promise.allSettled(
      recipientBatch.map(async (telegramId) => {
        const messageId = await sendMessage({
          api: ctx.api,
          chatId: Number(telegramId),
          telegramId,
          text: communication.message,
          logger: ctx.logger,
          options: {
            link_preview_options: { is_disabled: true },
            reply_markup: keyboard,
          },
        });

        return messageId !== null;
      }),
    );

    // Count each settled result so rejected sends still produce a partial batch outcome.
    const batchCounts = countBatchDeliveryResults(deliveryResults);
    sentCount += batchCounts.sentCount;
    failedCount += batchCounts.failedCount;
  }

  const markSentResponse = await rpcClient.bot.markCommunicationSent({
    id: communicationId,
  });

  if (!markSentResponse.success) {
    return ctx.reply(markSentResponse.error?.message ?? 'already sent');
  }

  return ctx.reply(
    `Communication ${communicationId} sent to ${sentCount} users. Failed deliveries: ${failedCount}.`,
  );
}
