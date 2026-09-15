import { ORPCError } from '@orpc/server';

import type { ApiProcedures } from '@/auth/middleware.js';
import { AuthStrategy } from '@/auth/types.js';

/**
 * Builds a bot-only procedure backed by the signed Telegram identity.
 */
export function createBotProcedure(procedures: ApiProcedures) {
  return procedures.securedProcedure.use(async ({ context, next }) => {
    if (context.authStrategy !== AuthStrategy.BOT_SIGNATURE || !context.botTelegramId) {
      throw new ORPCError('UNAUTHORIZED', {
        message: 'Bot signature authentication required',
      });
    }

    return next({ context });
  });
}
