import { describe, expect, it, vi } from 'vitest';

import { createBotCommunicationsRoutes } from './communications.js';

type RouteHandler = (input: Record<string, unknown>) => Promise<unknown>;

/**
 * Builds the minimum authenticated procedure chain needed to call bot route handlers.
 */
function createProcedureHarness() {
  const handlers: Record<string, RouteHandler> = {};
  const procedure = {
    route({ path }: { path: string }) {
      const builder = {
        input() {
          return builder;
        },
        output() {
          return {
            handler(handler: RouteHandler) {
              handlers[path] = handler;
              return handler;
            },
          };
        },
      };

      return builder;
    },
  };
  const securedProcedure = {
    use() {
      return procedure;
    },
  };

  return { handlers, securedProcedure };
}

// The response endpoint receives an untrusted payload and a Telegram identity
// that has already been verified from the bot signature.
describe('recordBotCommunicationResponse', () => {
  // This scenario includes a forged telegramId in the payload to prove storage
  // attribution comes exclusively from authenticated request context.
  it('records the response under the signed Telegram identity', async () => {
    // Communication 12 has two valid answer options, and the caller selects the first one.
    const communication = { id: 12, options: ['Accept', 'Cancel'] };
    const recordResponse = vi.fn().mockResolvedValue({});
    const { handlers, securedProcedure } = createProcedureHarness();

    // These storage operations are the only database behavior exercised by this route.
    const botCommunicationsStorage = {
      findById: vi.fn().mockResolvedValue(communication),
      recordResponse,
    };

    createBotCommunicationsRoutes({
      botCommunicationsStorage: botCommunicationsStorage as never,
      procedures: { securedProcedure } as never,
    });

    // The payload attempts to claim user 999, while the validated signature belongs to user 123.
    await handlers['/bot/communications/{id}/responses']({
      context: { botTelegramId: '123' },
      input: { id: 12, optionIndex: 0, telegramId: '999' },
    });

    // This assertion verifies forged payload identity is ignored before persistence.
    expect(recordResponse).toHaveBeenCalledWith({
      communicationId: 12,
      optionIndex: 0,
      telegramId: '123',
    });
  });
});

// Communication details expose only options that received answers, with each
// count carrying its own label instead of relying on parallel array positions.
describe('getBotCommunication responses', () => {
  // This scenario has no answers for the primary option and twelve answers for
  // the secondary option, so the primary option must be absent from the result.
  it('returns only options with recorded answers', async () => {
    // Communication 12 was broadcast with accept and cancel options in semantic order.
    const communication = {
      id: 12,
      description: 'Cluster migration',
      message: 'Do we migrate your cluster?',
      exclude: [],
      onlyTo: [],
      options: ['Accept', 'Cancel'],
      sent: true,
      sentAt: new Date('2026-09-15T12:00:00.000Z'),
      createdAt: new Date('2026-09-15T11:00:00.000Z'),
    };
    const { handlers, securedProcedure } = createProcedureHarness();

    // The grouped database result contains only option index 1 because nobody selected index 0.
    const botCommunicationsStorage = {
      findById: vi.fn().mockResolvedValue(communication),
      listBroadcastTelegramIds: vi.fn().mockResolvedValue(['123']),
      countResponsesByOption: vi.fn().mockResolvedValue([{ optionIndex: 1, count: 12 }]),
    };

    createBotCommunicationsRoutes({
      botCommunicationsStorage: botCommunicationsStorage as never,
      procedures: { securedProcedure } as never,
    });

    // This call loads the API representation used by the admin results command.
    const response = await handlers['/bot/communications/{id}']({ input: { id: 12 } });

    // This assertion verifies unanswered options are omitted and the remaining row is self-contained.
    expect(response).toMatchObject({
      success: true,
      data: {
        responses: [{ optionIndex: 1, label: 'Cancel', count: 12 }],
      },
    });
  });
});
