import type { CallbackQueryContext } from 'grammy';

import { parseCommunicationOptionCallbackData } from './options.js';

import type { Context } from '@/src/bot/context.js';

interface CommunicationResponseResult {
  success: boolean;
  error?: { message: string };
}

interface CommunicationResponseDependencies {
  recordResponse: (input: {
    communicationId: number;
    optionIndex: number;
    telegramId: string;
  }) => Promise<CommunicationResponseResult>;
}

/**
 * Records a callback answer and removes its message only after persistence succeeds.
 */
export async function handleCommunicationResponse(
  ctx: CallbackQueryContext<Context>,
  dependencies: CommunicationResponseDependencies,
) {
  const parsed = parseCommunicationOptionCallbackData(ctx.callbackQuery.data);
  const telegramId = ctx.from.id.toString();

  if (!parsed) {
    await ctx.answerCallbackQuery();
    return;
  }

  let response: CommunicationResponseResult;

  try {
    response = await dependencies.recordResponse({
      communicationId: parsed.communicationId,
      telegramId,
      optionIndex: parsed.optionIndex,
    });
  } catch (error) {
    ctx.logger.error(
      {
        communicationId: parsed.communicationId,
        optionIndex: parsed.optionIndex,
        error,
      },
      'Failed to record communication response',
    );
    await ctx.answerCallbackQuery({ text: ctx.t('communication-response-failed') });
    return;
  }

  if (!response.success) {
    ctx.logger.error(
      {
        communicationId: parsed.communicationId,
        optionIndex: parsed.optionIndex,
        error: response.error?.message,
      },
      'Failed to record communication response',
    );
    await ctx.answerCallbackQuery({ text: ctx.t('communication-response-failed') });
    return;
  }

  await ctx.answerCallbackQuery({ text: ctx.t('communication-response-recorded') });
  await ctx.deleteMessage();
}
