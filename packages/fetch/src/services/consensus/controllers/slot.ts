import { BeaconClient } from '../beacon.js';
import { SlotStorage } from '../storage/slot.js';
import type { Block, Attestation } from '../types.js';
import { BeaconTime } from '../utils/beaconTime.js';

import { SlotControllerHelpers } from './helpers/slotControllerHelpers.js';

import { EpochStorage } from '@/src/services/consensus/storage/epoch.js';
import { getBlock as getExecutionBlock } from '@/src/services/execution/endpoints.js';
import { convertToUTC, getUTCDatetimeRoundedToHour } from '@/src/utils/date/index.js';

/**
 * SlotController - Business logic layer for slot-related operations
 */
export class SlotController extends SlotControllerHelpers {
  constructor(
    private readonly slotStorage: SlotStorage,
    private readonly epochStorage: EpochStorage,
    private readonly beaconClient: BeaconClient,
    private readonly beaconTime: BeaconTime,
  ) {
    super();
  }

  /**
   * Get slot by number with processing data
   * TODO: why is this needed? The epoch should create the slots and we shouldn't reach to this point
   * if the slot is not created yet.
   */
  async getSlot(slot: number) {
    return this.slotStorage.getSlot(slot);
  }

  /**
   * Return the committee sizes for each slot in the beacon block data
   *
   * From the Beacon block data, collect unique `slot` values present in
   * `attestations`, filter out old slots using `this.beaconTime.getLookbackSlot()`,
   * then retrieve committee sizes for those slots from storage.
   *
   * Returns `Record<number, number[]>` where each key is a slot number and the value
   * is an array where each index equals the `committeeIndex` for that slot. That is,
   * `array[0]` is the size of slot.index 0, `array[1]` is the size of slot.index 1,
   * and so on. The value at each position is the number of validators in that committee.
   * Example: `{ 12345: [350, 349, ...] }` means slot 12345 has committee 0 with 350
   * validators, committee 1 with 349 validators, etc.
   */
  private async getCommitteeSizesForAttestations(slotNumber: number, attestations: Attestation[]) {
    // get unique slots from attestations and filter out slots that are older than the lookback slot
    let uniqueSlots = [...new Set(attestations.map((att) => Number(att.data.slot)))];
    uniqueSlots = uniqueSlots.filter((slot) => slot >= this.beaconTime.getLookbackSlot());

    if (uniqueSlots.length === 0) {
      throw new Error(`No attestations found for slot ${slotNumber}`);
    }

    const committeesCountInSlot = await this.slotStorage.getCommitteeSizesForSlots(uniqueSlots);

    // check if all slots have committee sizes
    const allSlotsHaveCounts = uniqueSlots.every((slot) =>
      Boolean(committeesCountInSlot[slot]?.length),
    );
    if (!allSlotsHaveCounts) {
      throw new Error(`Not all slots have committee sizes for beacon block ${slotNumber}`);
    }

    return committeesCountInSlot;
  }

  /**
   * Check if a slot is ready to be processed based on CONSENSUS_DELAY_SLOTS_TO_HEAD
   * TODO: delaySlotsToHead has to be handled on beaconTime class
   */
  async canSlotBeProcessed(slot: number, delaySlotsToHead: number) {
    const currentSlot = this.beaconTime.getSlotNumberFromTimestamp(Date.now());
    const maxSlotToFetch = currentSlot - delaySlotsToHead;
    const isReady = slot <= maxSlotToFetch;

    return {
      isReady,
      currentSlot,
      maxSlotToFetch,
    };
  }

  /**
   * Check if sync committee data exists for a given epoch
   */
  async isSyncCommitteeFetchedForEpoch(epoch: number) {
    return this.epochStorage.isSyncCommitteeForEpochInDB(epoch);
  }

  async areSlotConsensusRewardsFetched(slot: number) {
    return this.slotStorage.areSlotConsensusRewardsFetched(slot);
  }

  async isSyncCommitteeFetchedForSlot(slot: number) {
    return this.slotStorage.isSyncCommitteeFetchedForSlot(slot);
  }

  /**
   * Find the next unprocessed slot between startSlot and endSlot
   */
  async findMinUnprocessedSlotInEpoch(startSlot: number, endSlot: number) {
    try {
      return await this.slotStorage.findMinUnprocessedSlotInEpoch(startSlot, endSlot);
    } catch (error) {
      console.error('Error finding next unprocessed slot:', error);
      throw error;
    }
  }

  /**
   * Get the slot processing status for an epoch.
   * Returns semantic information about whether there's a slot to process
   * and whether all slots in the epoch are fully processed.
   *
   * This method is robust against the case where slots haven't been created yet,
   * distinguishing between "no slots exist" and "all slots are processed".
   */
  async getEpochSlotsStatus(startSlot: number, endSlot: number) {
    return this.slotStorage.getEpochSlotsStatus(startSlot, endSlot);
  }

  /**
   * Check if a slot is ready to be processed based on delay slots to head.
   */
  isSlotReady(slot: number) {
    return { isReady: this.beaconTime.hasSlotStarted(slot) };
  }

  /**
   * Wait until a slot is ready to be processed.
   * Uses beaconTime to calculate exact wait time including delay slots to head.
   */
  async waitUntilSlotReady(slot: number) {
    await this.beaconTime.waitUntilSlotStart(slot);
  }

  /**
   * Fetch beacon block data for a slot.
   */
  async fetchBeaconBlock(slot: number) {
    return this.beaconClient.getBlock(slot);
  }

  /**
   * Get sync committee validators for an epoch.
   */
  async getSyncCommitteeValidators(epoch: number): Promise<string[]> {
    const validators = await this.slotStorage.getSyncCommitteeValidators(epoch);
    return validators as string[];
  }

  /**
   * Get committee sizes for attestations in a beacon block.
   * Returns the committee sizes and whether all slots have counts.
   */
  async getCommitteeSizesForBlock(slot: number, beaconBlockData: Block) {
    const attestations = beaconBlockData.data.message.body.attestations || [];
    const uniqueSlots = [...new Set(attestations.map((att) => parseInt(att.data.slot)))].filter(
      (s) => s >= this.beaconTime.getLookbackSlot(),
    );

    if (uniqueSlots.length === 0) {
      return {
        committeesCountInSlot: {},
        allSlotsHaveCounts: false,
        uniqueSlots: [],
      };
    }

    const committeesCountInSlot = await this.slotStorage.getCommitteeSizesForSlots(uniqueSlots);

    const allSlotsHaveCounts = uniqueSlots.every((s) => {
      const counts = committeesCountInSlot[s];
      return counts && counts.length > 0;
    });

    return {
      committeesCountInSlot,
      allSlotsHaveCounts,
      uniqueSlots,
    };
  }

  /**
   * Process attestations from a beacon block.
   */
  /**
   * Process attestations from a beacon block.
   * Checks if already processed before processing.
   */
  async processBlockAttestations(
    slotNumber: number,
    attestations: Attestation[],
    slotCommitteesValidatorsAmounts: Record<number, number[]>,
  ) {
    const isAlreadyProcessed = await this.slotStorage.areAttestationsProcessedForSlot(slotNumber);
    if (isAlreadyProcessed) {
      return;
    }

    // Filter out attestations that are older than the oldest lookback slot
    const filteredAttestations = attestations.filter(
      (attestation) => +attestation.data.slot >= this.beaconTime.getLookbackSlot(),
    );

    // Process each attestation and calculate delays using the helper
    const allUpdates = [];
    for (const attestation of filteredAttestations) {
      const updates = this.processAttestation(
        slotNumber,
        attestation,
        slotCommitteesValidatorsAmounts,
      );
      allUpdates.push(...updates);
    }

    // Remove duplicates and keep the one with minimum delay
    const deduplicatedAttestations = this.deduplicateAttestations(allUpdates);

    // Persist to database
    await this.slotStorage.saveSlotAttestations(deduplicatedAttestations, slotNumber);
  }

  /**
   * Fetch and save block rewards for a slot.
   * Checks if already fetched before processing.
   */
  async fetchBlockRewards(slot: number, timestamp: number) {
    const isAlreadyFetched = await this.slotStorage.areSlotConsensusRewardsFetched(slot);
    if (isAlreadyFetched) {
      return;
    }

    const blockRewards = await this.beaconClient.getBlockRewards(slot);

    const { date, hour } = convertToUTC(new Date(timestamp * 1000));

    const reward = this.prepareBlockRewards(blockRewards, hour, date);

    await this.slotStorage.saveBlockRewardsAndUpdateSlot(slot, reward);
  }

  /**
   * Fetch and save sync committee rewards for a slot.
   * Checks if already fetched before processing.
   * Sync committee validators are fetched from the database (guaranteed to exist by epoch processor).
   */
  async fetchSyncRewards(slot: number, timestamp: number) {
    const isAlreadyFetched = await this.slotStorage.isSyncCommitteeFetchedForSlot(slot);
    if (isAlreadyFetched) {
      return;
    }

    const epoch = this.beaconTime.getEpochFromSlot(slot);
    const syncCommitteeValidators = await this.getSyncCommitteeValidators(epoch);

    const syncCommitteeRewards = await this.beaconClient.getSyncCommitteeRewards(
      slot,
      syncCommitteeValidators,
    );

    if (syncCommitteeRewards === 'SLOT MISSED') {
      await this.slotStorage.saveSyncRewardsAndUpdateSlot(slot, []);
      return;
    }

    const { date, hour } = convertToUTC(new Date(timestamp * 1000));

    const rewards = syncCommitteeRewards.data.map((reward) => ({
      validatorIndex: Number(reward.validator_index),
      date: new Date(date),
      hour,
      syncCommittee: BigInt(reward.reward),
    }));

    await this.slotStorage.saveSyncRewardsAndUpdateSlot(slot, rewards);
  }

  /**
   * Fetch execution layer rewards for a slot.
   * Checks if already fetched before processing.
   */
  async fetchExecutionRewards(slot: number, blockNumber: number) {
    const isAlreadyFetched = await this.slotStorage.areExecutionRewardsFetched(slot);
    if (isAlreadyFetched) {
      return;
    }

    const blockInfo = await getExecutionBlock(blockNumber);
    if (!blockInfo) {
      throw new Error(`Block ${blockNumber} not found`);
    }

    await this.slotStorage.saveExecutionRewardsAndUpdateSlot(slot, blockInfo);
  }

  /**
   * Format withdrawal rewards from beacon block data.
   */
  formatWithdrawalsData(
    withdrawals: Block['data']['message']['body']['execution_payload']['withdrawals'],
  ) {
    return withdrawals.map((w) => `${w.validator_index}:${w.amount}`);
  }

  /**
   * Fetch and process execution layer rewards
   * TODO: Implement using fetch/src/services/execution/endpoints.ts
   * And move to block controller in service/execution
   */
  // async fetchELRewards(slot: number, block: number, timestamp: number) {
  //   const blockInfo: Prisma.ExecutionRewardsUncheckedCreateInput = {
  //     address: '0x0000000000000000000000000000000000000000',
  //     timestamp: new Date(timestamp * 1000),
  //     amount: '0',
  //     blockNumber: block,
  //   };

  //   // Save execution rewards to database
  //   await this.slotStorage.saveExecutionRewards(blockInfo);

  //   // Update slot processing data
  //   await this.slotStorage.updateSlotFlags(slot, { executionRewardsFetched: true });

  //   return {
  //     slot,
  //     executionRewards: 0,
  //   };
  // }

  /**
   * Fetch and process sync committee rewards for a slot
   */
  async fetchSyncCommitteeRewards(slot: number, syncCommitteeValidators: string[]) {
    const isSyncCommitteeFetched = await this.isSyncCommitteeFetchedForSlot(slot);
    if (isSyncCommitteeFetched) {
      return;
    }

    // Fetch sync committee rewards from beacon chain
    const syncCommitteeRewards = await this.beaconClient.getSyncCommitteeRewards(
      slot,
      syncCommitteeValidators,
    );

    const slotTimestamp = await this.beaconTime.getTimestampFromSlotNumber(slot);
    const datetime = getUTCDatetimeRoundedToHour(slotTimestamp);

    // Prepare sync committee rewards for processing
    const processedRewards = this.prepareSyncCommitteeRewards(syncCommitteeRewards, slot);

    if (processedRewards.length > 0) {
      // Process sync committee rewards and aggregate into hourly data
      await this.slotStorage.processSyncCommitteeRewardsAndAggregate(
        slot,
        datetime,
        processedRewards,
      );
    } else {
      await this.slotStorage.updateSlotFlags(slot, { syncRewardsFetched: true });
    }
  }

  /**
   * Fetch and process block rewards for a slot
   * These rewards are for the proposer of the block
   */
  async fetchSlotConsensusRewards(slot: number) {
    const isBlockRewardsFetched = await this.areSlotConsensusRewardsFetched(slot);
    if (isBlockRewardsFetched) {
      return;
    }

    // Fetch block rewards from beacon chain
    const blockRewards = await this.beaconClient.getBlockRewards(slot);

    if (blockRewards === 'SLOT MISSED' || !blockRewards.data) {
      await this.slotStorage.updateSlotFlags(slot, { consensusRewardsFetched: true });
      return;
    }

    const slotTimestamp = this.beaconTime.getTimestampFromSlotNumber(slot);
    const datetime = getUTCDatetimeRoundedToHour(slotTimestamp);

    // Process block rewards and aggregate into hourly data
    await this.slotStorage.processSlotConsensusRewardsForSlot(
      slot,
      Number(blockRewards.data.proposer_index),
      datetime,
      BigInt(blockRewards.data.total),
    );
  }

  /**
   * Process attestations for a slot
   */
  private async processAttestations(slotNumber: number, attestations: Attestation[]) {
    // check if attestations are already processed
    const areAttestationsProcessed =
      await this.slotStorage.areAttestationsProcessedForSlot(slotNumber);
    if (areAttestationsProcessed) {
      return;
    }

    // Filter out attestations that are older than the oldest lookback slot
    const filteredAttestations = attestations.filter(
      (attestation) => +attestation.data.slot >= this.beaconTime.getLookbackSlot(),
    );

    // get committee sizes for attestations
    const committeesCountInSlot = await this.getCommitteeSizesForAttestations(
      slotNumber,
      filteredAttestations,
    );

    // Process each attestation and calculate delays
    const processedAttestations = [];
    for (const attestation of filteredAttestations) {
      const updates = this.processAttestation(slotNumber, attestation, committeesCountInSlot);
      processedAttestations.push(...updates);
    }

    // Remove duplicates and keep the one with minimum delay
    const deduplicatedAttestations = this.deduplicateAttestations(processedAttestations);

    // Update hourly validator data/stats with attestation delays
    await this.slotStorage.saveSlotAttestations(deduplicatedAttestations, slotNumber);
  }

  private async processWithdrawals(
    slot: number,
    withdrawals: Block['data']['message']['body']['execution_payload']['withdrawals'],
  ) {
    const baseSlot = await this.slotStorage.getBaseSlot(slot);
    if (baseSlot.validatorWithdrawalsFetched) {
      return;
    }

    await this.slotStorage.saveValidatorWithdrawals(
      baseSlot.slot,
      withdrawals.map((withdrawal) => ({
        slot: baseSlot.slot,
        validatorIndex: withdrawal.validator_index,
        amount: BigInt(withdrawal.amount),
      })),
    );
  }

  // TODO: move to execution controller
  private async processExecutionRequests(
    slot: number,
    executionRequests: Block['data']['message']['body']['execution_requests'],
  ) {
    const baseSlot = await this.slotStorage.getBaseSlot(slot);
    if (baseSlot.executionRequestsFetched) {
      return;
    }

    if (!executionRequests) {
      this.slotStorage.updateSlotFlags(slot, { executionRequestsFetched: true });
      return;
    }

    await this.slotStorage.saveExecutionRequests(slot, {
      validatorDeposits: executionRequests.deposits.map((deposit) => ({
        slot: baseSlot.slot,
        pubkey: deposit.pubkey,
        amount: BigInt(deposit.amount),
      })),
      //This field refers to withdrawal requests coming from the execution layer side,
      // i.e., operations initiated via the execution layer contract to request a withdrawal (or exit)
      // that the consensus layer will process.
      validatorWithdrawalsRequests: executionRequests.withdrawals.map((withdrawal) => ({
        slot: baseSlot.slot,
        pubKey: withdrawal.validator_pubkey,
        amount: BigInt(withdrawal.amount),
      })),
      validatorConsolidationsRequests: executionRequests.consolidations.map((consolidation) => ({
        slot: baseSlot.slot,
        sourcePubkey: consolidation.source_pubkey,
        targetPubkey: consolidation.target_pubkey,
      })),
    });
  }

  async processSlotData(slot: number) {
    const beaconBlock = await this.beaconClient.getBlock(slot);

    if (beaconBlock === 'SLOT MISSED') {
      await this.slotStorage.updateSlotProcessed(slot);
      return;
    }

    const tasks: Promise<void>[] = [];
    tasks.push(this.processAttestations(slot, beaconBlock.data.message.body.attestations));

    // validators whose balances have become eligible for withdrawal
    // (e.g., because they exited, or because their balance exceeded the effective balance cap)
    // have had their funds moved from the consensus layer (beacon chain) into the execution layer accounts.
    // MAX_WITHDRAWALS_PER_PAYLOAD
    tasks.push(
      this.processWithdrawals(slot, beaconBlock.data.message.body.execution_payload.withdrawals),
    );

    tasks.push(
      this.processExecutionRequests(slot, beaconBlock.data.message.body.execution_requests),
    );

    await Promise.all(tasks);

    return beaconBlock;
  }

  /**
   * Process sync committee attestations
   * TODO: wtf is this?
   */
  // async processSyncCommitteeAttestations(input: {
  //   slot: number;
  //   epoch: number;
  //   beaconBlockData?: Block; // TODO: fix this
  // }) {
  //   try {
  //     // Simulate some processing time
  //     await new Promise((resolve) => setTimeout(resolve, 100));

  //     return {
  //       slot: input.slot,
  //       syncCommitteeAttestations: [
  //         {
  //           validatorIndex: Math.floor(Math.random() * 1000),
  //         },
  //       ],
  //     };
  //   } catch (error) {
  //     console.error('Error processing sync committee attestations:', error);
  //     throw error;
  //   }
  // }

  /**
   * Process withdrawals rewards from beacon block data
   */
  async processWithdrawalsRewards(
    slot: number,
    withdrawals: Block['data']['message']['body']['execution_payload']['withdrawals'],
  ) {
    const withdrawalRewards = this.formatWithdrawalRewards(withdrawals);

    await this.slotStorage.updateSlotWithBeaconData(slot, {
      withdrawalsRewards: withdrawalRewards,
    });

    return withdrawalRewards;
  }

  /**
   * Process withdrawals rewards and return the data (for context updates)
   */
  async processWithdrawalsRewardsData(
    slot: number,
    withdrawals: Block['data']['message']['body']['execution_payload']['withdrawals'],
  ) {
    return this.formatWithdrawalRewards(withdrawals);
  }

  /**
   * Fetch validators balances for a slot
   */
  async fetchValidatorsBalances(slot: number, validatorIndexes: number[]) {
    // Get validator balances from storage
    const validatorBalances = await this.slotStorage.getValidatorsBalances(validatorIndexes);

    // Format for storage
    const balancesData = validatorBalances.map((validator) => ({
      index: validator.id.toString(),
      balance: validator.balance?.toString() || '0',
    }));

    // Save to database
    await this.slotStorage.saveValidatorBalances(balancesData, slot);

    return balancesData;
  }

  /**
   * Process CL deposits from beacon block
   */
  async processClDeposits(slot: number, deposits: Block['data']['message']['body']['deposits']) {
    console.log(`Processing CL deposits for slot ${slot}, found ${deposits.length} deposits`);
    return deposits.map((deposit, index) => `cl_deposit_${slot}_${index}`);
  }

  /**
   * Process CL voluntary exits from beacon block
   */
  async processClVoluntaryExits(
    slot: number,
    voluntaryExits: Block['data']['message']['body']['voluntary_exits'],
  ) {
    console.log(
      `Processing CL voluntary exits for slot ${slot}, found ${voluntaryExits.length} exits`,
    );
    return voluntaryExits.map((exit, index) => `cl_voluntary_exit_${slot}_${index}`);
  }

  /**
   * Process EL deposits from execution payload
   */
  async processElDeposits(
    slot: number,
    _executionPayload: Block['data']['message']['body']['execution_payload'],
  ) {
    console.log(`Processing EL deposits for slot ${slot}`);
    return [`el_deposit_${slot}_0`, `el_deposit_${slot}_1`];
  }

  /**
   * Process EL withdrawals from execution payload
   */
  async processElWithdrawals(
    slot: number,
    withdrawals: Block['data']['message']['body']['execution_payload']['withdrawals'],
  ) {
    console.log(
      `Processing EL withdrawals for slot ${slot}, found ${withdrawals.length} withdrawals`,
    );
    return withdrawals.map((withdrawal, index) => `el_withdrawal_${slot}_${index}`);
  }

  /**
   * Process EL consolidations from execution payload
   */
  async processElConsolidations(
    slot: number,
    _executionPayload: Block['data']['message']['body']['execution_payload'],
  ) {
    console.log(`Processing EL consolidations for slot ${slot}`);
    return [`el_consolidation_${slot}_0`];
  }

  /**
   * Update slot processed status in database
   */
  async updateSlotProcessed(slot: number) {
    // TODO: check all flags are set to true
    return this.slotStorage.updateSlotProcessed(slot);
  }

  /**
   * Update attestations processed status in database
   */
  async updateAttestationsProcessed(slot: number) {
    return this.slotStorage.updateAttestationsProcessed(slot);
  }

  /**
   * Update withdrawals processed status in database
   */
  async updateWithdrawalsProcessed(slot: number) {
    return this.slotStorage.updateSlotWithBeaconData(slot, {
      withdrawalsRewards: [], // Empty array indicates processed but no withdrawals
    });
  }

  /**
   * Update validator statuses
   */
  async updateValidatorStatuses(input: {
    slot: number;
    epoch: number;
    beaconBlockData?: Block; // TODO: fix this
  }) {
    try {
      console.log(`Updating validator statuses for slot ${input.slot}`);

      // Simulate some processing time
      await new Promise((resolve) => setTimeout(resolve, 90));

      return {
        slot: input.slot,
        validatorUpdates: [
          {
            validatorIndex: Math.floor(Math.random() * 1000),
            status: 'active',
          },
        ],
      };
    } catch (error) {
      console.error('Error updating validator statuses:', error);
      throw error;
    }
  }

  /**
   * Update slot with beacon data in database
   */
  async updateSlotWithBeaconData(slot: number, beaconBlockData: Block) {
    if (!beaconBlockData) {
      throw new Error('Beacon block data is required');
    }

    // Update slot with processed status and beacon data
    const updatedSlot = await this.slotStorage.updateSlotWithBeaconData(slot, {
      withdrawalsRewards: [], // Processed separately
      clDeposits: [], // Processed separately
      clVoluntaryExits: [], // Processed separately
      elDeposits: [], // Processed separately
      elWithdrawals: [], // Processed separately
      elConsolidations: [], // Processed separately
    });

    console.log(`Updated slot ${slot} with beacon data in database`);
    return updatedSlot;
  }

  /**
   * Cleanup old committee data
   */
  async cleanupOldCommittees(slot: number, slotsPerEpoch: number, maxAttestationDelay: number) {
    const deletedCount = await this.slotStorage.cleanupOldCommittees(
      slot,
      slotsPerEpoch,
      maxAttestationDelay,
    );

    return {
      slot,
      cleanupCompleted: true,
      deletedCount,
    };
  }
}
