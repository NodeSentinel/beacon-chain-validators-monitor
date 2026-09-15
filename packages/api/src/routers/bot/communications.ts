/* eslint-disable @typescript-eslint/no-explicit-any */
import { ORPCError } from '@orpc/server';
import { z } from 'zod';

import type { ApiDependencies } from '@/dependencies.js';
import { createBotProcedure } from '@/routers/bot/procedures.js';
import {
  BotCommunicationDetailsSchema,
  BotCommunicationListSchema,
  BotCommunicationResponseRecordedSchema,
  BotCommunicationSchema,
  BotCommunicationSentSchema,
  CommunicationIdParamSchema,
  CreateBotCommunicationSchema,
  RecordBotCommunicationResponseSchema,
} from '@/routers/bot/schemas.js';
import { resolveCommunicationRecipients } from '@/storage/bot-communication-recipients.js';
import { ApiResponseSchema, errorResponse, successResponse } from '@/utils/response.js';

const BotCommunicationResponseSchema = ApiResponseSchema(BotCommunicationSchema);
type BotCommunicationResponse = z.infer<typeof BotCommunicationResponseSchema>;

const BotCommunicationListResponseSchema = ApiResponseSchema(BotCommunicationListSchema);
type BotCommunicationListResponse = z.infer<typeof BotCommunicationListResponseSchema>;

const BotCommunicationDetailsResponseSchema = ApiResponseSchema(BotCommunicationDetailsSchema);
type BotCommunicationDetailsResponse = z.infer<typeof BotCommunicationDetailsResponseSchema>;

const BotCommunicationSentResponseSchema = ApiResponseSchema(BotCommunicationSentSchema);
type BotCommunicationSentResponse = z.infer<typeof BotCommunicationSentResponseSchema>;

const BotCommunicationResponseRecordedResponseSchema = ApiResponseSchema(
  BotCommunicationResponseRecordedSchema,
);
type BotCommunicationResponseRecordedResponse = z.infer<
  typeof BotCommunicationResponseRecordedResponseSchema
>;

/**
 * Maps a communication row into the API response shape.
 */
function mapCommunication(communication: {
  id: number;
  description: string;
  message: string;
  exclude: string[];
  onlyTo: string[];
  options: string[];
  sent: boolean;
  sentAt: Date | null;
  createdAt: Date;
}) {
  return {
    id: communication.id,
    description: communication.description,
    message: communication.message,
    exclude: communication.exclude,
    onlyTo: communication.onlyTo,
    options: communication.options,
    sent: communication.sent,
    sentAt: communication.sentAt?.toISOString() ?? null,
    createdAt: communication.createdAt.toISOString(),
  };
}

/**
 * Creates the bot communications routes.
 */
export function createBotCommunicationsRoutes(
  params: Pick<ApiDependencies, 'botCommunicationsStorage' | 'procedures'>,
) {
  const botProcedure = createBotProcedure(params.procedures);

  const createBotCommunication = botProcedure
    .route({ method: 'POST', path: '/bot/communications' })
    .input(CreateBotCommunicationSchema)
    .output(BotCommunicationResponseSchema)
    .handler(async ({ input }: any) => {
      try {
        return successResponse(
          mapCommunication(await params.botCommunicationsStorage.create(input)),
        ) as BotCommunicationResponse;
      } catch (error) {
        return errorResponse(
          'CREATE_COMMUNICATION_ERROR',
          error instanceof Error ? error.message : 'Failed to create communication',
        ) as BotCommunicationResponse;
      }
    });

  const listBotCommunications = botProcedure
    .route({ method: 'GET', path: '/bot/communications' })
    .output(BotCommunicationListResponseSchema)
    .handler(async () => {
      try {
        return successResponse(
          (await params.botCommunicationsStorage.listPending()).map(mapCommunication),
        ) as BotCommunicationListResponse;
      } catch (error) {
        return errorResponse(
          'LIST_COMMUNICATIONS_ERROR',
          error instanceof Error ? error.message : 'Failed to list communications',
        ) as BotCommunicationListResponse;
      }
    });

  const getBotCommunication = botProcedure
    .route({ method: 'GET', path: '/bot/communications/{id}' })
    .input(CommunicationIdParamSchema)
    .output(BotCommunicationDetailsResponseSchema)
    .handler(async ({ input }: any) => {
      try {
        const communication = await params.botCommunicationsStorage.findById(input.id);

        if (!communication) {
          throw new ORPCError('NOT_FOUND', {
            message: `Communication ${input.id} was not found`,
          });
        }

        const broadcastTelegramIds =
          communication.onlyTo.length > 0
            ? []
            : await params.botCommunicationsStorage.listBroadcastTelegramIds();

        const groupedCounts = await params.botCommunicationsStorage.countResponsesByOption(
          communication.id,
        );

        return successResponse({
          ...mapCommunication(communication),
          recipients: resolveCommunicationRecipients(broadcastTelegramIds, communication),
          responses: groupedCounts
            .flatMap(({ count, optionIndex }) => {
              const label = communication.options[optionIndex];

              return label === undefined ? [] : [{ optionIndex, label, count }];
            })
            .sort((left, right) => left.optionIndex - right.optionIndex),
        }) as BotCommunicationDetailsResponse;
      } catch (error) {
        return errorResponse(
          'GET_COMMUNICATION_ERROR',
          error instanceof Error ? error.message : 'Failed to get communication',
        ) as BotCommunicationDetailsResponse;
      }
    });

  const markBotCommunicationSent = botProcedure
    .route({ method: 'PUT', path: '/bot/communications/{id}/sent' })
    .input(CommunicationIdParamSchema)
    .output(BotCommunicationSentResponseSchema)
    .handler(async ({ input }: any) => {
      try {
        const updated = await params.botCommunicationsStorage.markSent(input.id);

        if (!updated) {
          throw new ORPCError('CONFLICT', {
            message: `Communication ${input.id} was already sent`,
          });
        }

        return successResponse({
          id: input.id,
          sent: true,
        }) as BotCommunicationSentResponse;
      } catch (error) {
        return errorResponse(
          'MARK_COMMUNICATION_SENT_ERROR',
          error instanceof Error ? error.message : 'Failed to mark communication as sent',
        ) as BotCommunicationSentResponse;
      }
    });

  const recordBotCommunicationResponse = botProcedure
    .route({ method: 'POST', path: '/bot/communications/{id}/responses' })
    .input(RecordBotCommunicationResponseSchema)
    .output(BotCommunicationResponseRecordedResponseSchema)
    .handler(async ({ context, input }: any) => {
      try {
        const communication = await params.botCommunicationsStorage.findById(input.id);

        if (!communication) {
          throw new ORPCError('NOT_FOUND', {
            message: `Communication ${input.id} was not found`,
          });
        }

        // Guard the index against stale keyboards: an old message can still carry
        // an option that no longer exists on the communication.
        if (input.optionIndex >= communication.options.length) {
          throw new ORPCError('BAD_REQUEST', {
            message: `Communication ${input.id} has no option ${input.optionIndex}`,
          });
        }

        await params.botCommunicationsStorage.recordResponse({
          communicationId: input.id,
          telegramId: context.botTelegramId,
          optionIndex: input.optionIndex,
        });

        return successResponse({
          id: input.id,
          optionIndex: input.optionIndex,
          recorded: true,
        }) as BotCommunicationResponseRecordedResponse;
      } catch (error) {
        return errorResponse(
          'RECORD_COMMUNICATION_RESPONSE_ERROR',
          error instanceof Error ? error.message : 'Failed to record response',
        ) as BotCommunicationResponseRecordedResponse;
      }
    });

  return {
    createBotCommunication,
    getBotCommunication,
    listBotCommunications,
    markBotCommunicationSent,
    recordBotCommunicationResponse,
  };
}
