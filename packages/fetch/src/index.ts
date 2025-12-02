import { PrismaClient } from '@beacon-indexer/db';
import ms from 'ms';

import { env, chainConfig } from '@/src/lib/env.js';
import createLogger from '@/src/lib/pino.js';
import { BeaconClient } from '@/src/services/consensus/beacon.js';
import { EpochController } from '@/src/services/consensus/controllers/epoch.js';
import { SlotController } from '@/src/services/consensus/controllers/slot.js';
import { ValidatorsController } from '@/src/services/consensus/controllers/validators.js';
import { EpochStorage } from '@/src/services/consensus/storage/epoch.js';
import { SlotStorage } from '@/src/services/consensus/storage/slot.js';
import { ValidatorsStorage } from '@/src/services/consensus/storage/validators.js';
import { BeaconTime } from '@/src/services/consensus/utils/beaconTime.js';
import initXstateMachines from '@/src/xstate/index.js';
import { getMultiMachineLogger } from '@/src/xstate/multiMachineLogger.js';

const logger = createLogger('index file');

// Build database URL with proper query parameter handling
const databaseUrl = env.DATABASE_URL.includes('?')
  ? `${env.DATABASE_URL}&pool_timeout=0&connect_timeout=10`
  : `${env.DATABASE_URL}?pool_timeout=0&connect_timeout=10`;

const prisma = new PrismaClient({
  datasources: {
    db: {
      url: databaseUrl,
    },
  },
  log: [
    {
      emit: 'event',
      level: 'error',
    },
  ],
});

// Log Prisma errors for debugging
prisma.$on('error' as never, (e: { message: string; target?: string }) => {
  logger.error('Prisma error:', e);
});

// Cleanup function to ensure Prisma disconnects properly
async function cleanup() {
  try {
    await prisma.$disconnect();
    logger.info('Database disconnected successfully');
  } catch (error) {
    logger.error('Error disconnecting from database:', error);
  }
  getMultiMachineLogger().done();
}

// Handle graceful shutdown signals
const shutdown = async (signal: string) => {
  logger.info(`Received ${signal}, shutting down gracefully...`);
  await cleanup();
  process.exit(0);
};

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

// Handle uncaught exceptions and unhandled rejections
process.on('uncaughtException', async (error) => {
  logger.error('Uncaught exception:', error);
  await cleanup();
  process.exit(1);
});

process.on('unhandledRejection', async (reason, promise) => {
  logger.error('Unhandled rejection at:', { promise, reason });
  await cleanup();
  process.exit(1);
});

async function main() {
  try {
    await prisma.$connect();
    logger.info('Database connected successfully');
  } catch (error) {
    logger.error('Failed to connect to database:', error);
    // Try to disconnect if connection partially succeeded
    try {
      await prisma.$disconnect();
    } catch {
      // Ignore disconnect errors if connection failed
    }
    throw error;
  }

  // Initialize dependencies
  const beaconClient = new BeaconClient({
    fullNodeUrl: env.CONSENSUS_FULL_API_URL,
    fullNodeConcurrency: env.CONSENSUS_API_REQUEST_PER_SECOND,
    fullNodeRetries: 10,
    archiveNodeUrl: env.CONSENSUS_ARCHIVE_API_URL,
    archiveNodeConcurrency: env.CONSENSUS_API_REQUEST_PER_SECOND,
    archiveNodeRetries: 30,
    baseDelay: ms('1s'),
    slotStartIndexing: env.CONSENSUS_LOOKBACK_SLOT,
  });

  const beaconTime = new BeaconTime({
    genesisTimestamp: chainConfig.beacon.genesisTimestamp,
    slotDurationMs: chainConfig.beacon.slotDuration,
    slotsPerEpoch: chainConfig.beacon.slotsPerEpoch,
    epochsPerSyncCommitteePeriod: chainConfig.beacon.epochsPerSyncCommitteePeriod,
    lookbackSlot: env.CONSENSUS_LOOKBACK_SLOT,
    delaySlotsToHead: chainConfig.beacon.delaySlotsToHead,
  });

  const validatorsStorage = new ValidatorsStorage(prisma);
  const validatorsController = new ValidatorsController(beaconClient, validatorsStorage);

  const epochStorage = new EpochStorage(prisma, validatorsStorage);
  const slotStorage = new SlotStorage(prisma);
  const epochController = new EpochController(
    beaconClient,
    epochStorage,
    validatorsStorage,
    beaconTime,
  );
  const slotController = new SlotController(slotStorage, epochStorage, beaconClient, beaconTime);

  // Start indexing the beacon chain
  await validatorsController.initValidators();

  await initXstateMachines(
    epochController,
    beaconTime,
    chainConfig.beacon.slotDuration,
    slotController,
    validatorsController,
  );
}

main().catch((e) => {
  logger.error('', e);
  cleanup()
    .then(() => {
      process.exit(1);
    })
    .catch(() => {
      process.exit(1);
    });
});
