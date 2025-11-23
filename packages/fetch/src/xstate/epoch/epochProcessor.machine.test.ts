import ms from 'ms';
import { test, expect, vi, beforeEach, afterEach, describe } from 'vitest';
import { createActor, SnapshotFrom } from 'xstate';

import { createControllablePromise } from '@/src/__tests__/utils.js';
import { EpochController } from '@/src/services/consensus/controllers/epoch.js';
import { SlotController } from '@/src/services/consensus/controllers/slot.js';
import { ValidatorsController } from '@/src/services/consensus/controllers/validators.js';
import { BeaconTime } from '@/src/services/consensus/utils/beaconTime.js';
import { epochProcessorMachine } from '@/src/xstate/epoch/epochProcessor.machine.js';

// ============================================================================
// Test Constants
// ============================================================================
const SLOT_DURATION = ms('10ms');
const SLOTS_PER_EPOCH = 32;
const GENESIS_TIMESTAMP = 1606824000000;
const EPOCHS_PER_SYNC_COMMITTEE_PERIOD = 256;
const SLOT_START_INDEXING = 32;
const EPOCH_100_START_TIME = GENESIS_TIMESTAMP + 100 * SLOTS_PER_EPOCH * 10;
const EPOCH_101_START_TIME = GENESIS_TIMESTAMP + 101 * SLOTS_PER_EPOCH * 10;

// ============================================================================
// Mock Controllers
// ============================================================================
const mockEpochController = {
  fetchCommittees: vi.fn<any>(),
  fetchSyncCommittees: vi.fn<any>(),
  fetchRewards: vi.fn<any>(),
  updateSlotsFetched: vi.fn<any>(),
  markEpochAsProcessed: vi.fn<any>(),
  markValidatorsActivationFetched: vi.fn<any>(),
  isValidatorsBalancesFetched: vi.fn<any>(),
  isRewardsFetched: vi.fn<any>(),
  isValidatorsActivationFetched: vi.fn<any>(),
} as unknown as EpochController;

const mockValidatorsController = {
  fetchValidatorsBalances: vi.fn<any>(),
  trackTransitioningValidators: vi.fn<any>(),
} as unknown as ValidatorsController;

const mockSlotController = {} as unknown as SlotController;

// ============================================================================
// Mock slotOrchestratorMachine
// ============================================================================
const mockSlotOrchestratorMachine = vi.hoisted(() => {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const { setup } = require('xstate');

  return setup({}).createMachine({
    id: 'slotOrchestratorMachine',
    initial: 'processing',
    states: {
      processing: {
        on: {
          SLOT_COMPLETED: {
            target: 'complete',
          },
        },
      },
      complete: {
        type: 'final',
        // When the machine reaches final state, it will trigger onDone in parent
      },
    },
  });
});

vi.mock('@/src/xstate/slot/slotOrchestrator.machine.js', () => ({
  slotOrchestratorMachine: mockSlotOrchestratorMachine,
}));

vi.mock('@/src/xstate/pinoLog.js', () => ({
  pinoLog: vi.fn(() => () => {}),
}));

vi.mock('@/src/xstate/multiMachineLogger.js', () => ({
  logActor: vi.fn(),
}));

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Reset all mocks to default successful behavior
 */
function resetMocks() {
  vi.clearAllMocks();
  (mockEpochController.fetchCommittees as any).mockResolvedValue(undefined);
  (mockEpochController.fetchSyncCommittees as any).mockResolvedValue(undefined);
  (mockEpochController.fetchRewards as any).mockResolvedValue(undefined);
  (mockEpochController.updateSlotsFetched as any).mockResolvedValue(undefined);
  (mockEpochController.markEpochAsProcessed as any).mockResolvedValue(undefined);
  (mockEpochController.markValidatorsActivationFetched as any).mockResolvedValue(undefined);
  (mockValidatorsController.fetchValidatorsBalances as any).mockResolvedValue(undefined);
  (mockValidatorsController.trackTransitioningValidators as any).mockResolvedValue(undefined);
}

/**
 * Create BeaconTime instance with test constants
 */
function createMockBeaconTime() {
  return new BeaconTime({
    genesisTimestamp: GENESIS_TIMESTAMP,
    slotDurationMs: SLOT_DURATION,
    slotsPerEpoch: SLOTS_PER_EPOCH,
    epochsPerSyncCommitteePeriod: EPOCHS_PER_SYNC_COMMITTEE_PERIOD,
    lookbackSlot: SLOT_START_INDEXING,
  });
}

/**
 * Create default input for epoch processor machine
 */
function createDefaultInput(
  epoch: number,
  overrides?: {
    beaconTime?: BeaconTime;
  },
) {
  return {
    epoch,
    config: {
      slotDuration: SLOT_DURATION,
      lookbackSlot: SLOT_START_INDEXING,
    },
    services: {
      beaconTime: overrides?.beaconTime || createMockBeaconTime(),
      epochController: mockEpochController,
      validatorsController: mockValidatorsController,
      slotController: mockSlotController,
    },
  };
}

/**
 * Create and start an actor, returning it with a state transitions array
 */
function createAndStartActor(
  input: ReturnType<typeof createDefaultInput>,
  guards?: Record<string, (...args: unknown[]) => boolean>,
) {
  const actor = createActor(
    guards ? epochProcessorMachine.provide({ guards }) : epochProcessorMachine,
    { input },
  );

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const stateTransitions: SnapshotFrom<any>[] = [];
  const subscription = actor.subscribe((snapshot) => {
    stateTransitions.push(snapshot.value);
  });

  actor.start();

  return { actor, stateTransitions, subscription };
}

/**
 * Get the last state from state transitions
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function getLastState(stateTransitions: any[]) {
  return stateTransitions[stateTransitions.length - 1];
}

/**
 * Get nested state value from state object
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function getNestedState(state: any, path: string) {
  const parts = path.split('.');
  let current = state;
  for (const part of parts) {
    if (current && typeof current === 'object' && part in current) {
      current = current[part];
    } else {
      return null;
    }
  }
  return current;
}

// ============================================================================
// Tests
// ============================================================================

describe('epochProcessorMachine', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    resetMocks();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllTimers();
  });

  describe('checkingCanProcess', () => {
    test('cannot process epoch (too early), should go to waiting and retry', async () => {
      const epochStartTime = EPOCH_100_START_TIME;
      const tooEarlyTime = epochStartTime - SLOTS_PER_EPOCH * SLOT_DURATION - 50;
      vi.setSystemTime(new Date(tooEarlyTime));

      const { actor, stateTransitions, subscription } = createAndStartActor(
        createDefaultInput(100),
        {
          canProcessEpoch: () => false,
        },
      );

      // Initial state should be checkingCanProcess
      expect(stateTransitions[0]).toBe('checkingCanProcess');

      // After timers run, we should move to waitingForEpochStart
      vi.runOnlyPendingTimers();
      await Promise.resolve();

      expect(stateTransitions[1]).toBe('waitingForEpochStart');

      actor.stop();
      subscription.unsubscribe();
    });

    test('can process epoch (1 epoch in advance), should go to epochProcessing', async () => {
      vi.setSystemTime(new Date(EPOCH_101_START_TIME + 10));

      const { actor, stateTransitions, subscription } = createAndStartActor(
        createDefaultInput(100),
      );

      // Initial snapshot should be checkingCanProcess
      expect(stateTransitions[0]).toBe('checkingCanProcess');

      vi.runOnlyPendingTimers();
      await Promise.resolve();

      // Next snapshot should be epochProcessing
      expect(typeof stateTransitions[1]).toBe('object');
      expect(stateTransitions[1]).toHaveProperty('epochProcessing');

      actor.stop();
      subscription.unsubscribe();
    });
  });

  describe('epochProcessing', () => {
    describe('monitoringEpochStart', () => {
      test('epoch already started, should go directly to epochStarted', async () => {
        vi.setSystemTime(new Date(EPOCH_101_START_TIME + 50));

        const { actor, stateTransitions, subscription } = createAndStartActor(
          createDefaultInput(100),
        );

        await vi.runAllTimersAsync();

        // Get last epochProcessing state
        const lastState = getLastState(stateTransitions);
        const monitoringState = getNestedState(lastState, 'epochProcessing.monitoringEpochStart');
        expect(monitoringState).toBe('epochStarted');

        actor.stop();
        subscription.unsubscribe();
      });

      test('epoch not started, should wait and then complete', async () => {
        vi.setSystemTime(new Date(EPOCH_100_START_TIME - 100));

        const { actor, stateTransitions, subscription } = createAndStartActor(
          createDefaultInput(100),
        );

        await vi.runAllTimersAsync();

        // Collect monitoring substate transitions

        const monitoringStates = stateTransitions
          .map((s: any) => getNestedState(s, 'epochProcessing.monitoringEpochStart'))
          .filter((s) => s !== null);

        const waitingIndex = monitoringStates.indexOf('waitingForEpochStart');
        const startedIndex = monitoringStates.indexOf('epochStarted');

        expect(waitingIndex).toBeGreaterThanOrEqual(0);
        expect(startedIndex).toBeGreaterThan(waitingIndex);

        actor.stop();
        subscription.unsubscribe();
      });

      test('epoch start respects delaySlotsToHead (waits until effective start)', async () => {
        const beaconTimeWithDelay = new BeaconTime({
          genesisTimestamp: GENESIS_TIMESTAMP,
          slotDurationMs: SLOT_DURATION,
          slotsPerEpoch: SLOTS_PER_EPOCH,
          epochsPerSyncCommitteePeriod: EPOCHS_PER_SYNC_COMMITTEE_PERIOD,
          lookbackSlot: SLOT_START_INDEXING,
          delaySlotsToHead: 4,
        });

        // Time is after nominal epoch start but before effective start (startSlot + delay)
        vi.setSystemTime(new Date(EPOCH_100_START_TIME + SLOT_DURATION));

        const { actor, stateTransitions, subscription } = createAndStartActor(
          createDefaultInput(100, { beaconTime: beaconTimeWithDelay }),
        );

        await vi.runAllTimersAsync();

        // Collect monitoring substate transitions

        const monitoringStates = stateTransitions
          .map((s: any) => getNestedState(s, 'epochProcessing.monitoringEpochStart'))
          .filter((s) => s !== null);

        const waitingIndex = monitoringStates.indexOf('waitingForEpochStart');
        const startedIndex = monitoringStates.indexOf('epochStarted');

        expect(waitingIndex).toBeGreaterThanOrEqual(0);
        expect(startedIndex).toBeGreaterThan(waitingIndex);

        actor.stop();
        subscription.unsubscribe();
      });
    });

    describe('committees', () => {
      test('not fetched, should process and complete', async () => {
        vi.setSystemTime(new Date(EPOCH_101_START_TIME + 50));

        const fetchPromise = createControllablePromise<{ success: boolean; skipped: boolean }>();
        (mockEpochController.fetchCommittees as any).mockReturnValue(fetchPromise.promise);

        const { actor, stateTransitions, subscription } = createAndStartActor(
          createDefaultInput(100),
        );

        await vi.runAllTimersAsync();

        // Should be processing
        let lastState = getLastState(stateTransitions);
        let committeesState = getNestedState(lastState, 'epochProcessing.fetching.committees');
        expect(committeesState).toBe('fetchingCommittees');

        // Complete the fetch
        fetchPromise.resolve({ success: true, skipped: false });
        await vi.runAllTimersAsync();

        // Should be complete
        lastState = getLastState(stateTransitions);
        committeesState = getNestedState(lastState, 'epochProcessing.fetching.committees');
        expect(committeesState).toBe('committeesFetched');
        expect(mockEpochController.fetchCommittees).toHaveBeenCalledWith(100);

        actor.stop();
        subscription.unsubscribe();
      });

      test('should emit COMMITTEES_FETCHED on complete', async () => {
        vi.setSystemTime(new Date(EPOCH_101_START_TIME + 50));

        const { actor, subscription } = createAndStartActor(createDefaultInput(100));

        await vi.runAllTimersAsync();

        // Committees should be marked as fetched in sync state
        expect(actor.getSnapshot().context.sync.committeesFetched).toBe(true);

        actor.stop();
        subscription.unsubscribe();
      });
    });

    describe('syncingCommittees', () => {
      test('not fetched, should process and complete', async () => {
        vi.setSystemTime(new Date(EPOCH_101_START_TIME + 50));

        const fetchPromise = createControllablePromise<{ success: boolean; skipped: boolean }>();
        (mockEpochController.fetchSyncCommittees as any).mockReturnValue(fetchPromise.promise);

        const { actor, stateTransitions, subscription } = createAndStartActor(
          createDefaultInput(100),
        );

        await vi.runAllTimersAsync();

        // Should be processing
        let lastState = getLastState(stateTransitions);
        let syncState = getNestedState(lastState, 'epochProcessing.fetching.syncingCommittees');
        expect(syncState).toBe('fetchingSyncCommittees');

        // Complete the fetch
        fetchPromise.resolve({ success: true, skipped: false });
        await vi.runAllTimersAsync();

        // Should be complete
        lastState = getLastState(stateTransitions);
        syncState = getNestedState(lastState, 'epochProcessing.fetching.syncingCommittees');
        expect(syncState).toBe('syncCommitteesFetched');
        expect(mockEpochController.fetchSyncCommittees).toHaveBeenCalledWith(100);

        actor.stop();
        subscription.unsubscribe();
      });
    });

    describe('slotsProcessing', () => {
      test('should wait for committees before processing', async () => {
        vi.setSystemTime(new Date(EPOCH_101_START_TIME + 50));

        const committeesPromise = createControllablePromise<{
          success: boolean;
          skipped: boolean;
        }>();
        (mockEpochController.fetchCommittees as any).mockReturnValue(committeesPromise.promise);

        const { actor, stateTransitions, subscription } = createAndStartActor(
          createDefaultInput(100),
        );

        await vi.runAllTimersAsync();

        // Should be waiting for committees
        let lastState = getLastState(stateTransitions);
        let slotsState = getNestedState(lastState, 'epochProcessing.fetching.slotsProcessing');
        expect(slotsState).toBe('waitingForCommittees');

        // Complete committees
        committeesPromise.resolve({ success: true, skipped: false });
        await vi.runAllTimersAsync();

        // Should now be running the slots orchestrator
        lastState = getLastState(stateTransitions);
        slotsState = getNestedState(lastState, 'epochProcessing.fetching.slotsProcessing');
        expect(slotsState).toBe('runningSlotsOrchestrator');

        actor.stop();
        subscription.unsubscribe();
      });

      test('should spawn slot orchestrator and handle SLOTS_COMPLETED lifecycle', async () => {
        vi.setSystemTime(new Date(EPOCH_100_START_TIME + 50));

        const { actor, stateTransitions, subscription } = createAndStartActor(
          createDefaultInput(100),
        );

        // Wait for committees to be ready and slots to start processing
        await vi.runAllTimersAsync();

        const lastState = getLastState(stateTransitions);
        const slotsState = getNestedState(lastState, 'epochProcessing.fetching.slotsProcessing');

        // Should be running the orchestrator now
        expect(slotsState).toBe('runningSlotsOrchestrator');

        // Get current snapshot to access slot orchestrator
        const currentSnapshot = actor.getSnapshot();

        // Should have spawned the orchestrator
        expect(currentSnapshot.context.actors.slotOrchestratorActor).toBeTruthy();

        // Verify committees were fetched for this epoch
        expect(mockEpochController.fetchCommittees).toHaveBeenCalledWith(100);

        // Simulate SLOTS_COMPLETED from child
        actor.send({ type: 'SLOTS_COMPLETED', epoch: 100 });
        await vi.runAllTimersAsync();

        // Verify lifecycle states in order
        const slotsStates = stateTransitions
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          .map((s: any) => getNestedState(s, 'epochProcessing.fetching.slotsProcessing'))
          .filter((s) => s !== null);

        const waitingIndex = slotsStates.indexOf('waitingForCommittees');
        const runningIndex = slotsStates.indexOf('runningSlotsOrchestrator');
        const updatingIndex = slotsStates.indexOf('updatingSlotsFetched');
        const processedIndex = slotsStates.indexOf('slotsProcessed');

        expect(waitingIndex).toBeGreaterThanOrEqual(0);
        expect(runningIndex).toBeGreaterThan(waitingIndex);
        expect(updatingIndex).toBeGreaterThan(runningIndex);
        expect(processedIndex).toBeGreaterThan(updatingIndex);

        // updateSlotsFetched should have been called
        expect(mockEpochController.updateSlotsFetched).toHaveBeenCalledWith(100);

        // Slot orchestrator actor should be cleared from context
        expect(actor.getSnapshot().context.actors.slotOrchestratorActor).toBeNull();

        actor.stop();
        subscription.unsubscribe();
      });
    });

    describe('trackingValidatorsActivation', () => {
      test('should wait for epoch start', async () => {
        vi.setSystemTime(new Date(EPOCH_100_START_TIME - 100));

        const { actor, stateTransitions, subscription } = createAndStartActor(
          createDefaultInput(100),
        );

        vi.runOnlyPendingTimers();
        await Promise.resolve();

        // Should be waiting for epoch start
        const lastState = getLastState(stateTransitions);
        const activationState = getNestedState(
          lastState,
          'epochProcessing.fetching.trackingValidatorsActivation',
        );
        expect(activationState).toBe('waitingForEpochStart');

        actor.stop();
        subscription.unsubscribe();
      });
      test('epoch started, should process and complete', async () => {
        vi.setSystemTime(new Date(EPOCH_101_START_TIME + 50));

        const trackingPromise = createControllablePromise<void>();
        (mockValidatorsController.trackTransitioningValidators as any).mockReturnValue(
          trackingPromise.promise,
        );

        const { actor, stateTransitions, subscription } = createAndStartActor(
          createDefaultInput(100),
        );

        await vi.runAllTimersAsync();

        // Should be processing
        let lastState = getLastState(stateTransitions);
        let activationState = getNestedState(
          lastState,
          'epochProcessing.fetching.trackingValidatorsActivation',
        );
        expect(activationState).toBe('trackingActivation');

        // Complete tracking
        trackingPromise.resolve();
        await vi.runAllTimersAsync();

        // Should be complete
        lastState = getLastState(stateTransitions);
        activationState = getNestedState(
          lastState,
          'epochProcessing.fetching.trackingValidatorsActivation',
        );
        expect(activationState).toBe('activationTracked');
        expect(mockValidatorsController.trackTransitioningValidators).toHaveBeenCalled();

        actor.stop();
        subscription.unsubscribe();
      });
    });

    describe('validatorsBalances', () => {
      test('should wait for epoch start', async () => {
        vi.setSystemTime(new Date(EPOCH_100_START_TIME - 100));

        const { actor, stateTransitions, subscription } = createAndStartActor(
          createDefaultInput(100),
        );

        vi.runOnlyPendingTimers();
        await Promise.resolve();

        // Should be waiting for epoch start
        const lastState = getLastState(stateTransitions);
        const balancesState = getNestedState(
          lastState,
          'epochProcessing.fetching.validatorsBalances',
        );
        expect(balancesState).toBe('waitingForEpochStart');

        actor.stop();
        subscription.unsubscribe();
      });

      test('epoch started, not fetched, should process and complete', async () => {
        vi.setSystemTime(new Date(EPOCH_101_START_TIME + 50));

        const balancesPromise = createControllablePromise<void>();
        (mockValidatorsController.fetchValidatorsBalances as any).mockReturnValue(
          balancesPromise.promise,
        );

        const { actor, stateTransitions, subscription } = createAndStartActor(
          createDefaultInput(100),
        );

        await vi.runAllTimersAsync();

        // Should be processing
        let lastState = getLastState(stateTransitions);
        let balancesState = getNestedState(
          lastState,
          'epochProcessing.fetching.validatorsBalances',
        );
        expect(balancesState).toBe('fetchingValidatorsBalances');

        // Complete balances fetch
        balancesPromise.resolve();
        await vi.runAllTimersAsync();

        // Should be complete
        lastState = getLastState(stateTransitions);
        balancesState = getNestedState(lastState, 'epochProcessing.fetching.validatorsBalances');
        expect(balancesState).toBe('validatorsBalancesFetched');
        expect(mockValidatorsController.fetchValidatorsBalances).toHaveBeenCalled();

        actor.stop();
        subscription.unsubscribe();
      });

      test('should emit VALIDATORS_BALANCES_FETCHED on complete', async () => {
        vi.setSystemTime(new Date(EPOCH_101_START_TIME + 50));

        const { actor, subscription } = createAndStartActor(createDefaultInput(100));

        await vi.runAllTimersAsync();

        // Balances should be marked as fetched in sync state
        expect(actor.getSnapshot().context.sync.validatorsBalancesFetched).toBe(true);

        actor.stop();
        subscription.unsubscribe();
      });
    });

    describe('rewards', () => {
      test('should wait for validators balances', async () => {
        vi.setSystemTime(new Date(EPOCH_101_START_TIME + 50));

        const balancesPromise = createControllablePromise<void>();
        (mockValidatorsController.fetchValidatorsBalances as any).mockReturnValue(
          balancesPromise.promise,
        );

        const { actor, stateTransitions, subscription } = createAndStartActor(
          createDefaultInput(100),
        );

        await vi.runAllTimersAsync();

        // Should be waiting for balances
        const lastState = getLastState(stateTransitions);
        const rewardsState = getNestedState(lastState, 'epochProcessing.fetching.rewards');
        expect(rewardsState).toBe('waitingForBalances');

        actor.stop();
        subscription.unsubscribe();
      });

      test('balances ready and epoch ended, should process rewards after prerequisites', async () => {
        // Set time after epoch has ended
        const epochEndTime = EPOCH_101_START_TIME + SLOTS_PER_EPOCH * SLOT_DURATION + 100;
        vi.setSystemTime(new Date(epochEndTime));

        (mockEpochController.fetchRewards as any).mockResolvedValue(undefined);

        const { actor, stateTransitions, subscription } = createAndStartActor(
          createDefaultInput(100),
          {
            areValidatorsBalancesFetched: () => true,
          },
        );

        await vi.runAllTimersAsync();

        // Collect rewards substates in order
        const rewardsStates = stateTransitions
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          .map((s: any) => getNestedState(s, 'epochProcessing.fetching.rewards'))
          .filter((s) => s !== null);

        const fetchingRewardsIndex = rewardsStates.indexOf('fetchingRewards');
        const rewardsFetchedIndex = rewardsStates.indexOf('rewardsFetched');

        // Depending on timing and guards, we may not observe waitingForEpochEnd
        // as a stable snapshot. We only require that rewards are fetched in order.
        expect(fetchingRewardsIndex).toBeGreaterThanOrEqual(0);
        expect(rewardsFetchedIndex).toBeGreaterThan(fetchingRewardsIndex);

        // Controller should have been called once prerequisites were met
        expect(mockEpochController.fetchRewards).toHaveBeenCalledWith(100);

        actor.stop();
        subscription.unsubscribe();
      });
    });
  });

  // Note: finalization (markingEpochProcessed/epochCompleted) and parent signaling
  // via sendParent are integration concerns when this machine is spawned by a parent.
  // They are covered indirectly by verifying that all child regions and controllers
  // behave correctly; direct parent signaling is tested at the orchestrator level.
});
