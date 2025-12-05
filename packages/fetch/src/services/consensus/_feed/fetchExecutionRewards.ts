// import { Decimal } from '@prisma/client/runtime/library';
// import ms from 'ms';

import { CustomLogger } from '@/src/lib/pino.js';

export async function fetchExecutionRewards(logger: CustomLogger, blockToQuery: number) {
  return {} as any;
  // try {
  //   const blockInfo = await getBlock(blockToQuery);
  //   if (!blockInfo) {
  //     logger.warn('No block provided');
  //     return;
  //   }
  //   await prisma.executionRewards.create({
  //     data: blockInfo,
  //   });
  //   logger.info('done.');
  // } catch (error) {
  //   logger.warn('Not found', error);
  //   // const lastBlock = await prisma.executionRewards.findFirst({
  //   //   orderBy: {
  //   //     blockNumber: 'desc',
  //   //   },
  //   // });
  //   // // add 5 to the last block number
  //   // const timestamp = lastBlock!.timestamp.getTime() + ms('5s');
  //   // await prisma.executionRewards.create({
  //   //   data: {
  //   //     address: '',
  //   //     timestamp: new Date(timestamp),
  //   //     amount: new Decimal(0),
  //   //     blockNumber: blockToQuery,
  //   //   },
  //   // });
  // }
}
