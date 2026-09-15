import { ORPCError } from '@orpc/server';
import { describe, expect, it, vi } from 'vitest';

import { createBotProcedure } from './procedures.js';

import { AuthStrategy } from '@/auth/types.js';

interface BotProcedureMiddlewareInput {
  context: {
    authStrategy?: AuthStrategy;
    botTelegramId?: string;
  };
  next: (input: { context: BotProcedureMiddlewareInput['context'] }) => Promise<unknown>;
}

type BotProcedureMiddleware = (input: BotProcedureMiddlewareInput) => Promise<unknown>;

/**
 * Captures the authentication middleware added to a bot procedure.
 */
function createProcedureHarness() {
  let middleware: BotProcedureMiddleware | undefined;
  const procedureAfterAuthentication = { route: vi.fn() };
  const securedProcedure = {
    use(candidate: BotProcedureMiddleware) {
      middleware = candidate;
      return procedureAfterAuthentication;
    },
  };

  return {
    getMiddleware: () => middleware,
    procedureAfterAuthentication,
    securedProcedure,
  };
}

// Bot routes must reject credentials from every strategy except the signed bot
// request, otherwise callers could reach internal bot operations through user auth.
describe('createBotProcedure authentication', () => {
  // This scenario presents a valid API key context without a signed Telegram identity.
  it('rejects a non-bot authentication strategy', async () => {
    // The harness records the middleware so it can be exercised without an HTTP server.
    const { getMiddleware, securedProcedure } = createProcedureHarness();
    createBotProcedure({ securedProcedure } as never);

    // This callback represents the downstream route and must never run for an API key.
    const next = vi.fn();
    const middleware = getMiddleware();

    // This assertion verifies bot routes reject authenticated callers that lack a bot signature.
    await expect(
      middleware?.({
        context: { authStrategy: AuthStrategy.API_KEY },
        next,
      }),
    ).rejects.toBeInstanceOf(ORPCError);
    expect(next).not.toHaveBeenCalled();
  });

  // This scenario presents the Telegram identity preserved after successful HMAC validation.
  it('accepts a bot signature context with its verified Telegram identity', async () => {
    // The harness records the middleware and the procedure returned after authentication.
    const { getMiddleware, procedureAfterAuthentication, securedProcedure } =
      createProcedureHarness();
    const procedure = createBotProcedure({ securedProcedure } as never);

    // This context represents bot-signature middleware that authenticated Telegram user 123.
    const context = {
      authStrategy: AuthStrategy.BOT_SIGNATURE,
      botTelegramId: '123',
    };
    const next = vi.fn().mockResolvedValue('accepted');

    // This assertion verifies the request proceeds and retains the verified identity.
    await expect(getMiddleware()?.({ context, next })).resolves.toBe('accepted');
    expect(next).toHaveBeenCalledWith({ context });
    expect(procedure).toBe(procedureAfterAuthentication);
  });
});
