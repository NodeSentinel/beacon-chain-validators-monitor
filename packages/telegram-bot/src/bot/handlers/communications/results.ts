import type { CommandContext } from 'grammy';

import { parseCommunicationIdArgument } from './command-id.js';
import { formatCommunicationResults } from './list-format.js';

import { COMMON_REQUESTS_TELEGRAM_ID, getRpcClientForUser } from '@/src/api/client.js';
import type { Context } from '@/src/bot/context.js';

/**
 * Shows how many users picked each option of a communication.
 */
export async function communicationResultsHandler(ctx: CommandContext<Context>) {
  const communicationId = parseCommunicationIdArgument(ctx.message?.text);

  if (!communicationId) {
    return ctx.reply('Usage: /communication_results <id>');
  }

  // Use the generic bot client because this command is not tied to any user context.
  const rpcClient = getRpcClientForUser(COMMON_REQUESTS_TELEGRAM_ID);
  const response = await rpcClient.bot.getCommunication({
    id: communicationId,
  });

  if (!response.success || !response.data) {
    return ctx.reply(response.error?.message ?? `Communication ${communicationId} was not found.`);
  }

  return ctx.reply(formatCommunicationResults(response.data));
}
