/**
 * @fileoverview The slot processor is a state machine that is responsible for processing individual slots.
 *
 * It is responsible for:
 * - Fetching and processing beacon block data
 * - Processing different types of data in parallel:
 *   - Execution Layer rewards
 *   - Block and sync rewards
 *   - Attestations
 * - Handling errors with retry logic
 * - Emitting completion events
 *
 * This machine processes one slot at a time.
 *
 * NOTE: This machine uses controller-based inline actors. The old slot.actors.ts file
 * is considered legacy and should be removed once all consumers have migrated.
 */

import { setup, assign, sendParent, fromPromise } from 'xstate';

import { SlotController } from '@/src/services/consensus/controllers/slot.js';
import { Block } from '@/src/services/consensus/types.js';
import { pinoLog } from '@/src/xstate/pinoLog.js';

export interface SlotProcessorContext {
  epoch: number;
  slot: number;
  slotController: SlotController;
  beaconBlockData: {
    rawData: Block | 'SLOT MISSED' | null;
    withdrawalRewards: string[];
    clDeposits: string[];
    clVoluntaryExits: string[];
    elDeposits: string[];
    elWithdrawals: string[];
    elConsolidations: string[];
  };
  committeesCountInSlot?: Record<number, number[]>;
  lookbackSlot: number;
}

export interface SlotProcessorInput {
  epoch: number;
  slot: number;
  lookbackSlot: number;
  slotController: SlotController;
}

export const slotProcessorMachine = setup({
  types: {} as {
    context: SlotProcessorContext;
    input: SlotProcessorInput;
  },
  actors: {
    // Get slot from database
    getSlot: fromPromise(
      async ({ input }: { input: { slotController: SlotController; slot: number } }) =>
        input.slotController.getSlot(input.slot),
    ),

    // Wait until slot is ready to be processed (calculates exact wait time)
    waitUntilSlotReady: fromPromise(
      async ({ input }: { input: { slotController: SlotController; slot: number } }) =>
        input.slotController.waitUntilSlotReady(input.slot),
    ),

    // Fetch beacon block data
    fetchBeaconBlock: fromPromise(
      async ({ input }: { input: { slotController: SlotController; slot: number } }) =>
        input.slotController.fetchBeaconBlock(input.slot),
    ),

    // Fetch execution layer rewards
    fetchELRewards: fromPromise(
      async ({
        input,
      }: {
        input: { slotController: SlotController; slot: number; block: number };
      }) => input.slotController.fetchExecutionRewards(input.slot, input.block),
    ),

    // Fetch block rewards (consensus rewards)
    fetchBlockRewards: fromPromise(
      async ({
        input,
      }: {
        input: {
          slotController: SlotController;
          slot: number;
          timestamp: number;
        };
      }) => input.slotController.fetchBlockRewards(input.slot, input.timestamp),
    ),

    // Fetch sync committee rewards
    fetchSyncRewards: fromPromise(
      async ({
        input,
      }: {
        input: {
          slotController: SlotController;
          slot: number;
          timestamp: number;
        };
      }) => input.slotController.fetchSyncRewards(input.slot, input.timestamp),
    ),

    // Get committee validator amounts for attestations
    checkAndGetCommitteeValidatorsAmounts: fromPromise(
      async ({
        input,
      }: {
        input: { slotController: SlotController; slot: number; beaconBlockData: Block };
      }) => input.slotController.getCommitteeSizesForBlock(input.slot, input.beaconBlockData),
    ),

    // Process attestations
    processAttestations: fromPromise(
      async ({
        input,
      }: {
        input: {
          slotController: SlotController;
          slotNumber: number;
          attestations: Block['data']['message']['body']['attestations'];
          slotCommitteesValidatorsAmounts: Record<number, number[]>;
        };
      }) =>
        input.slotController.processBlockAttestations(
          input.slotNumber,
          input.attestations,
          input.slotCommitteesValidatorsAmounts,
        ),
    ),

    // Update attestations processed status
    updateAttestationsProcessed: fromPromise(
      async ({ input }: { input: { slotController: SlotController; slot: number } }) =>
        input.slotController.updateAttestationsProcessed(input.slot),
    ),

    // Process withdrawals rewards data (returns formatted data)
    processWithdrawalsRewardsData: fromPromise(
      async ({
        input,
      }: {
        input: {
          slotController: SlotController;
          withdrawals: Block['data']['message']['body']['execution_payload']['withdrawals'];
        };
      }) => input.slotController.formatWithdrawalsData(input.withdrawals),
    ),

    // Process CL deposits
    processClDeposits: fromPromise(
      async ({
        input,
      }: {
        input: {
          slotController: SlotController;
          slot: number;
          deposits: Block['data']['message']['body']['deposits'];
        };
      }) => input.slotController.processClDeposits(input.slot, input.deposits),
    ),

    // Process CL voluntary exits
    processClVoluntaryExits: fromPromise(
      async ({
        input,
      }: {
        input: {
          slotController: SlotController;
          slot: number;
          voluntaryExits: Block['data']['message']['body']['voluntary_exits'];
        };
      }) => input.slotController.processClVoluntaryExits(input.slot, input.voluntaryExits),
    ),

    // Process EL deposits
    processElDeposits: fromPromise(
      async ({
        input,
      }: {
        input: {
          slotController: SlotController;
          slot: number;
          executionPayload: Block['data']['message']['body']['execution_payload'];
        };
      }) => input.slotController.processElDeposits(input.slot, input.executionPayload),
    ),

    // Process EL withdrawals
    processElWithdrawals: fromPromise(
      async ({
        input,
      }: {
        input: {
          slotController: SlotController;
          slot: number;
          withdrawals: Block['data']['message']['body']['execution_payload']['withdrawals'];
        };
      }) => input.slotController.processElWithdrawals(input.slot, input.withdrawals),
    ),

    // Process EL consolidations
    processElConsolidations: fromPromise(
      async ({
        input,
      }: {
        input: {
          slotController: SlotController;
          slot: number;
          executionPayload: Block['data']['message']['body']['execution_payload'];
        };
      }) => input.slotController.processElConsolidations(input.slot, input.executionPayload),
    ),

    // Update slot processed status
    updateSlotProcessed: fromPromise(
      async ({ input }: { input: { slotController: SlotController; slot: number } }) =>
        input.slotController.updateSlotProcessed(input.slot),
    ),
  },
  guards: {
    isSlotMissed: ({ context }) => context.beaconBlockData?.rawData === 'SLOT MISSED',
    isLookbackSlot: ({ context }) => context.slot === context.lookbackSlot,
    allSlotsHaveCounts: (_, params: { allSlotsHaveCounts: boolean }) =>
      params.allSlotsHaveCounts === true,
    hasBeaconBlockData: ({ context }) => context.beaconBlockData?.rawData !== null,
  },
  delays: {},
}).createMachine({
  id: 'SlotProcessor',
  initial: 'gettingSlot',
  context: ({ input }) => ({
    epoch: input.epoch,
    slot: input.slot,
    slotController: input.slotController,
    beaconBlockData: {
      rawData: null,
      withdrawalRewards: [],
      clDeposits: [],
      clVoluntaryExits: [],
      elDeposits: [],
      elWithdrawals: [],
      elConsolidations: [],
    },
    lookbackSlot: input.lookbackSlot,
  }),

  states: {
    gettingSlot: {
      description: 'Getting the slot from the database and checking if already processed.',
      entry: pinoLog(({ context }) => `Getting slot ${context.slot}`, 'SlotProcessor:gettingSlot'),
      invoke: {
        src: 'getSlot',
        input: ({ context }) => ({
          slotController: context.slotController,
          slot: context.slot,
        }),
        onDone: [
          {
            guard: ({ event }) => event.output?.processed === true,
            target: 'completed',
          },
          {
            target: 'waitingForSlotToStart',
          },
        ],
      },
    },

    waitingForSlotToStart: {
      description:
        'Waiting for the slot to be ready. Uses beaconTime to calculate exact wait time.',
      entry: pinoLog(
        ({ context }) => `Waiting for slot ${context.slot} to be ready`,
        'SlotProcessor:waitingForSlotToStart',
      ),
      invoke: {
        src: 'waitUntilSlotReady',
        input: ({ context }) => ({
          slotController: context.slotController,
          slot: context.slot,
        }),
        onDone: {
          target: 'fetchingBeaconBlock',
        },
      },
    },

    fetchingBeaconBlock: {
      description:
        'Fetches the beacon block from the consensus layer API. ' +
        'The beacon block is the primary data source containing: ' +
        '(1) attestations for committee reward calculations, ' +
        '(2) execution payload with withdrawals, deposits, and consolidations, ' +
        '(3) proposer information for block/sync rewards, ' +
        '(4) voluntary exits and slashings. ' +
        'All parallel processing states depend on this data being available in context.',
      entry: pinoLog(
        ({ context }) => `Fetching beacon block data for slot ${context.slot}`,
        'SlotProcessor:fetchingBeaconData',
      ),
      invoke: {
        src: 'fetchBeaconBlock',
        input: ({ context }) => ({
          slotController: context.slotController,
          slot: context.slot,
        }),
        onDone: {
          target: 'checkingForMissedSlot',
          actions: assign({
            beaconBlockData: ({ event, context }) => ({
              ...context.beaconBlockData,
              rawData: event.output,
            }),
          }),
        },
      },
    },

    checkingForMissedSlot: {
      description: 'Check if the slot was missed or has valid data',
      always: [
        {
          guard: 'isSlotMissed',
          target: 'markingSlotCompleted',
        },
        {
          target: 'processingSlot',
        },
      ],
    },

    processingSlot: {
      description: 'In this state we fetch/process all the information from the block.',
      type: 'parallel',
      onDone: 'markingSlotCompleted',
      states: {
        beaconBlock: {
          description:
            'In this state the information fetched in fetchingBeaconBlock state is processed.',
          initial: 'processing',
          states: {
            processing: {
              type: 'parallel',
              onDone: 'complete',
              states: {
                attestations: {
                  description:
                    'Processing the attestations for the slot, attestations for slot n include attestations for slot n-1 up to n-slotsInEpoch',
                  initial: 'verifyingDone',
                  states: {
                    verifyingDone: {
                      always: [
                        {
                          description:
                            'if we are processing the slot CONSENSUS_LOOKBACK_SLOT, we mark attestations processed immediately ' +
                            'as it brings attestations for slots < CONSENSUS_LOOKBACK_SLOT and we should ignore them.',
                          guard: 'isLookbackSlot',
                          target: 'updateAttestationsProcessed',
                        },
                        {
                          target: 'gettingCommitteeValidatorsAmounts',
                        },
                      ],
                    },
                    gettingCommitteeValidatorsAmounts: {
                      invoke: {
                        src: 'checkAndGetCommitteeValidatorsAmounts',
                        input: ({ context }) => ({
                          slotController: context.slotController,
                          slot: context.slot,
                          beaconBlockData: context.beaconBlockData?.rawData as Block,
                        }),
                        onDone: [
                          {
                            guard: {
                              type: 'allSlotsHaveCounts',
                              params: ({ event }) => ({
                                allSlotsHaveCounts: event.output.allSlotsHaveCounts,
                              }),
                            },
                            target: 'processingAttestations',
                            actions: assign({
                              committeesCountInSlot: ({ event }) =>
                                event.output.committeesCountInSlot,
                            }),
                          },
                          {
                            actions: pinoLog(
                              ({ context }) =>
                                `error: missing committee counts for slot ${context.slot}`,
                              'SlotProcessor:attestations',
                            ),
                            target: 'error',
                          },
                        ],
                      },
                    },
                    processingAttestations: {
                      entry: pinoLog(
                        ({ context }) => `processing attestations for slot ${context.slot}`,
                        'SlotProcessor:attestations',
                      ),
                      invoke: {
                        src: 'processAttestations',
                        input: ({ context }) => {
                          const _beaconBlockData = context.beaconBlockData?.rawData as Block;
                          return {
                            slotController: context.slotController,
                            slotNumber: context.slot,
                            attestations: _beaconBlockData.data.message.body.attestations ?? [],
                            slotCommitteesValidatorsAmounts: context.committeesCountInSlot ?? {},
                          };
                        },
                        onDone: {
                          target: 'complete',
                        },
                      },
                    },
                    updateAttestationsProcessed: {
                      entry: pinoLog(
                        ({ context }) =>
                          `updating attestations processed flag for slot ${context.slot}`,
                        'SlotProcessor:attestations',
                      ),
                      invoke: {
                        src: 'updateAttestationsProcessed',
                        input: ({ context }) => ({
                          slotController: context.slotController,
                          slot: context.slot,
                        }),
                        onDone: {
                          target: 'complete',
                        },
                        onError: {
                          target: 'updateAttestationsProcessed',
                        },
                      },
                    },
                    complete: {
                      entry: pinoLog(
                        ({ context }) => `attestations complete for slot ${context.slot}`,
                        'SlotProcessor:attestations',
                      ),
                      type: 'final',
                    },
                    error: {
                      type: 'final',
                    },
                  },
                },
                withdrawalRewards: {
                  description: 'Processing withdrawal rewards from beacon block',
                  initial: 'processing',
                  states: {
                    processing: {
                      entry: pinoLog(
                        ({ context }) => `processing withdrawal rewards for slot ${context.slot}`,
                        'SlotProcessor:withdrawalRewards',
                      ),
                      invoke: {
                        src: 'processWithdrawalsRewardsData',
                        input: ({ context }) => {
                          const _beaconBlockData = context.beaconBlockData?.rawData as Block;
                          return {
                            slotController: context.slotController,
                            withdrawals:
                              _beaconBlockData?.data?.message?.body?.execution_payload
                                ?.withdrawals || [],
                          };
                        },
                        onDone: {
                          target: 'complete',
                          actions: assign({
                            beaconBlockData: ({ context, event }) => ({
                              ...context.beaconBlockData!,
                              withdrawalRewards: event.output || [],
                            }),
                          }),
                        },
                      },
                    },
                    complete: {
                      type: 'final',
                      entry: pinoLog(
                        ({ context }) => `complete withdrawal rewards for slot ${context.slot}`,
                        'SlotProcessor:withdrawalRewards',
                      ),
                    },
                  },
                },
                clDeposits: {
                  description: 'Processing CL deposits from beacon block',
                  initial: 'processing',
                  states: {
                    processing: {
                      entry: pinoLog(
                        ({ context }) => `processing CL deposits for slot ${context.slot}`,
                        'SlotProcessor:clDeposits',
                      ),
                      invoke: {
                        src: 'processClDeposits',
                        input: ({ context }) => {
                          const _beaconBlockData = context.beaconBlockData?.rawData as Block;
                          return {
                            slotController: context.slotController,
                            slot: context.slot,
                            deposits: _beaconBlockData?.data?.message?.body?.deposits || [],
                          };
                        },
                        onDone: {
                          target: 'complete',
                          actions: assign({
                            beaconBlockData: ({ context, event }) => ({
                              ...context.beaconBlockData!,
                              clDeposits: event.output || [],
                            }),
                          }),
                        },
                      },
                    },
                    complete: {
                      type: 'final',
                      entry: pinoLog(
                        ({ context }) => `complete CL deposits for slot ${context.slot}`,
                        'SlotProcessor:clDeposits',
                      ),
                    },
                  },
                },
                clVoluntaryExits: {
                  description: 'Processing CL voluntary exits from beacon block',
                  initial: 'processing',
                  states: {
                    processing: {
                      entry: pinoLog(
                        ({ context }) => `processing CL voluntary exits for slot ${context.slot}`,
                        'SlotProcessor:clVoluntaryExits',
                      ),
                      invoke: {
                        src: 'processClVoluntaryExits',
                        input: ({ context }) => {
                          const _beaconBlockData = context.beaconBlockData?.rawData as Block;
                          return {
                            slotController: context.slotController,
                            slot: context.slot,
                            voluntaryExits:
                              _beaconBlockData?.data?.message?.body?.voluntary_exits || [],
                          };
                        },
                        onDone: {
                          target: 'complete',
                          actions: assign({
                            beaconBlockData: ({ context, event }) => ({
                              ...context.beaconBlockData!,
                              clVoluntaryExits: event.output || [],
                            }),
                          }),
                        },
                      },
                    },
                    complete: {
                      type: 'final',
                      entry: pinoLog(
                        ({ context }) => `complete CL voluntary exits for slot ${context.slot}`,
                        'SlotProcessor:clVoluntaryExits',
                      ),
                    },
                  },
                },
                elDeposits: {
                  description: 'Processing EL deposits from execution payload',
                  initial: 'processing',
                  states: {
                    processing: {
                      entry: pinoLog(
                        ({ context }) => `processing EL deposits for slot ${context.slot}`,
                        'SlotProcessor:elDeposits',
                      ),
                      invoke: {
                        src: 'processElDeposits',
                        input: ({ context }) => {
                          const _beaconBlockData = context.beaconBlockData?.rawData as Block;
                          return {
                            slotController: context.slotController,
                            slot: context.slot,
                            executionPayload:
                              _beaconBlockData?.data?.message?.body?.execution_payload,
                          };
                        },
                        onDone: {
                          target: 'complete',
                          actions: assign({
                            beaconBlockData: ({ context, event }) => ({
                              ...context.beaconBlockData!,
                              elDeposits: event.output || [],
                            }),
                          }),
                        },
                      },
                    },
                    complete: {
                      type: 'final',
                      entry: pinoLog(
                        ({ context }) => `complete EL deposits for slot ${context.slot}`,
                        'SlotProcessor:elDeposits',
                      ),
                    },
                  },
                },
                elWithdrawals: {
                  description: 'Processing EL withdrawals from execution payload',
                  initial: 'processing',
                  states: {
                    processing: {
                      entry: pinoLog(
                        ({ context }) => `processing EL withdrawals for slot ${context.slot}`,
                        'SlotProcessor:elWithdrawals',
                      ),
                      invoke: {
                        src: 'processElWithdrawals',
                        input: ({ context }) => {
                          const _beaconBlockData = context.beaconBlockData?.rawData as Block;
                          return {
                            slotController: context.slotController,
                            slot: context.slot,
                            withdrawals:
                              _beaconBlockData?.data?.message?.body?.execution_payload
                                ?.withdrawals || [],
                          };
                        },
                        onDone: {
                          target: 'complete',
                          actions: assign({
                            beaconBlockData: ({ context, event }) => ({
                              ...context.beaconBlockData!,
                              elWithdrawals: event.output || [],
                            }),
                          }),
                        },
                      },
                    },
                    complete: {
                      type: 'final',
                      entry: pinoLog(
                        ({ context }) => `complete EL withdrawals for slot ${context.slot}`,
                        'SlotProcessor:elWithdrawals',
                      ),
                    },
                  },
                },
                elConsolidations: {
                  description: 'Processing EL consolidations from execution payload',
                  initial: 'processing',
                  states: {
                    processing: {
                      entry: pinoLog(
                        ({ context }) => `processing EL consolidations for slot ${context.slot}`,
                        'SlotProcessor:elConsolidations',
                      ),
                      invoke: {
                        src: 'processElConsolidations',
                        input: ({ context }) => {
                          const _beaconBlockData = context.beaconBlockData?.rawData as Block;
                          return {
                            slotController: context.slotController,
                            slot: context.slot,
                            executionPayload:
                              _beaconBlockData?.data?.message?.body?.execution_payload,
                          };
                        },
                        onDone: {
                          target: 'complete',
                          actions: assign({
                            beaconBlockData: ({ context, event }) => ({
                              ...context.beaconBlockData!,
                              elConsolidations: event.output || [],
                            }),
                          }),
                        },
                      },
                    },
                    complete: {
                      type: 'final',
                      entry: pinoLog(
                        ({ context }) => `complete EL consolidations for slot ${context.slot}`,
                        'SlotProcessor:elConsolidations',
                      ),
                    },
                  },
                },
                executionRewards: {
                  description: 'Fetching execution layer rewards for the slot proposer.',
                  initial: 'processing',
                  states: {
                    processing: {
                      entry: pinoLog(
                        ({ context }) => `fetching execution rewards for slot ${context.slot}`,
                        'SlotProcessor:executionRewards',
                      ),
                      invoke: {
                        src: 'fetchELRewards',
                        input: ({ context }) => {
                          const _beaconBlockData = context.beaconBlockData?.rawData as Block;
                          return {
                            slotController: context.slotController,
                            slot: context.slot,
                            block: Number(
                              _beaconBlockData.data.message.body.execution_payload.block_number,
                            ),
                          };
                        },
                        onDone: {
                          target: 'complete',
                        },
                        onError: {
                          target: 'processing',
                          actions: ({ event }) => {
                            console.error('Error fetching execution rewards:', event.error);
                          },
                        },
                      },
                    },
                    complete: {
                      type: 'final',
                      entry: pinoLog(
                        ({ context }) => `complete execution rewards for slot ${context.slot}`,
                        'SlotProcessor:executionRewards',
                      ),
                    },
                  },
                },
                blockRewards: {
                  description: 'Fetching block rewards (consensus rewards) for the slot proposer.',
                  initial: 'processing',
                  states: {
                    processing: {
                      entry: pinoLog(
                        ({ context }) => `fetching block rewards for slot ${context.slot}`,
                        'SlotProcessor:blockRewards',
                      ),
                      invoke: {
                        src: 'fetchBlockRewards',
                        input: ({ context }) => {
                          const _beaconBlockData = context.beaconBlockData?.rawData as Block;
                          return {
                            slotController: context.slotController,
                            slot: context.slot,
                            timestamp: Number(
                              _beaconBlockData.data.message.body.execution_payload.timestamp,
                            ),
                          };
                        },
                        onDone: {
                          target: 'complete',
                        },
                      },
                    },
                    complete: {
                      type: 'final',
                      entry: pinoLog(
                        ({ context }) => `complete block rewards for slot ${context.slot}`,
                        'SlotProcessor:blockRewards',
                      ),
                    },
                  },
                },
                syncRewards: {
                  description: 'Fetching sync committee rewards for the slot.',
                  initial: 'processing',
                  states: {
                    processing: {
                      entry: pinoLog(
                        ({ context }) => `fetching sync rewards for slot ${context.slot}`,
                        'SlotProcessor:syncRewards',
                      ),
                      invoke: {
                        src: 'fetchSyncRewards',
                        input: ({ context }) => {
                          const _beaconBlockData = context.beaconBlockData?.rawData as Block;
                          return {
                            slotController: context.slotController,
                            slot: context.slot,
                            timestamp: Number(
                              _beaconBlockData.data.message.body.execution_payload.timestamp,
                            ),
                          };
                        },
                        onDone: {
                          target: 'complete',
                        },
                      },
                    },
                    complete: {
                      type: 'final',
                      entry: pinoLog(
                        ({ context }) => `complete sync rewards for slot ${context.slot}`,
                        'SlotProcessor:syncRewards',
                      ),
                    },
                  },
                },
              },
            },
            complete: {
              type: 'final',
            },
          },
        },
      },
    },

    markingSlotCompleted: {
      description: 'Marking the slot as completed.',
      entry: pinoLog(
        ({ context }) => `Marking slot completed ${context.slot}`,
        'SlotProcessor:markingSlotCompleted',
      ),
      invoke: {
        src: 'updateSlotProcessed',
        input: ({ context }) => ({
          slotController: context.slotController,
          slot: context.slot,
        }),
        onDone: {
          target: 'completed',
        },
        onError: {
          target: 'markingSlotCompleted',
        },
      },
    },

    completed: {
      entry: [
        sendParent({ type: 'SLOT_COMPLETED' }),
        pinoLog(({ context }) => `Completed slot ${context.slot}`, 'SlotProcessor:slotCompleted'),
      ],
      type: 'final',
    },
  },
});
