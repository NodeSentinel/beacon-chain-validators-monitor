import { PrismaClient } from '@beacon-indexer/db';

/**
 * Stores broadcast communications used by the Telegram bot.
 */
export class BotCommunicationsStorage {
  constructor(private readonly prisma: PrismaClient) {}

  /**
   * Creates a new communication in pending state.
   */
  async create(input: {
    description: string;
    message: string;
    exclude: string[];
    onlyTo: string[];
    options: string[];
  }) {
    return this.prisma.communication.create({
      data: {
        description: input.description,
        message: input.message,
        exclude: input.exclude,
        onlyTo: input.onlyTo,
        options: input.options,
      },
    });
  }

  /**
   * Gets one communication by numeric id.
   */
  async findById(id: number) {
    return this.prisma.communication.findUnique({
      where: { id },
    });
  }

  /**
   * Lists communications that have not been sent yet.
   */
  async listPending() {
    return this.prisma.communication.findMany({
      where: {
        sent: false,
      },
      orderBy: {
        createdAt: 'asc',
      },
    });
  }

  /**
   * Lists the telegram ids eligible for a full broadcast send.
   */
  async listBroadcastTelegramIds() {
    const users = await this.prisma.user.findMany({
      where: {
        telegramId: { not: null },
        hasBlockedBot: false,
        clusters: {
          some: {
            validators: {
              some: {},
            },
          },
        },
      },
      select: {
        telegramId: true,
      },
    });

    // Convert database bigint values into telegram ids the bot can send to directly.
    return users.map((user) => user.telegramId!.toString());
  }

  /**
   * Stores the option a user picked for a communication.
   * Uses an upsert so a repeated tap, which can only happen if the delivered
   * message survives the client-side delete, never fails the button handler.
   */
  async recordResponse(input: {
    communicationId: number;
    telegramId: string;
    optionIndex: number;
  }) {
    return this.prisma.communicationResponse.upsert({
      where: {
        communicationId_telegramId: {
          communicationId: input.communicationId,
          telegramId: input.telegramId,
        },
      },
      create: {
        communicationId: input.communicationId,
        telegramId: input.telegramId,
        optionIndex: input.optionIndex,
      },
      update: {
        optionIndex: input.optionIndex,
        respondedAt: new Date(),
      },
    });
  }

  /**
   * Counts the answers of one communication grouped by the option index.
   * Options nobody picked are absent from the result, so callers must pad the
   * missing indexes themselves.
   */
  async countResponsesByOption(communicationId: number) {
    const grouped = await this.prisma.communicationResponse.groupBy({
      by: ['optionIndex'],
      where: { communicationId },
      _count: { _all: true },
    });

    return grouped.map((group) => ({
      optionIndex: group.optionIndex,
      count: group._count._all,
    }));
  }

  /**
   * Marks a communication as sent only once.
   */
  async markSent(id: number) {
    const result = await this.prisma.communication.updateMany({
      where: {
        id,
        sent: false,
      },
      data: {
        sent: true,
        sentAt: new Date(),
      },
    });

    return result.count === 1;
  }
}
