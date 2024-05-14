import path from "path";
import { convertToCSV, publicClient, readCSV } from "../utils";
import * as constants from "../constants";
import logger from "../logger";
import fsP from "fs/promises";

const blockTimestamps = new Map<bigint, Date>();

let earliestBlock = { block: 0n, timestamp: 0n };

const timestampsFile = path.join(constants.dataDirectory, "block_timestamps.csv");

let loaded = false;

export const loadTimestamps = async () => {
  const data = await readCSV<{ block: number; timestamp: number }>(timestampsFile).catch(() => []);

  logger.info(`Loaded ${data.length} timestamps`);

  earliestBlock.block = BigInt(data[0].block);
  earliestBlock.timestamp = BigInt(data[0].timestamp);

  for (const row of data) {
    blockTimestamps.set(BigInt(row.block), new Date(+row.timestamp * 1000));
  }
};

// TODO: Only store timestamps every few seconds to save on I/O
const storeTimestamps = async () => {
  const { out, headers } = await convertToCSV(
    Array.from(blockTimestamps.entries())
      .map(([block, timestamp]) => ({
        block: block.toString(),
        timestamp: (timestamp.getTime() / 1000).toString(),
      }))
      .sort((a, b) => Number(a.block) - Number(b.block))
  );

  await fsP.writeFile(timestampsFile, headers + "\n" + out);
};

// Check how many calls we have made in the last second
let lastSecond = 0;
let lastTimestamp = Date.now();

export const getTimestamp = async (blockNumber: number) => {
  if (!loaded) {
    await loadTimestamps();
    loaded = true;
  }
  // TODO: Remove this once we have all timestamps
  return Number(earliestBlock.timestamp + BigInt(BigInt(blockNumber) - earliestBlock.block) * 2n);
  const blockNum = BigInt(blockNumber);
  if (!blockTimestamps.has(blockNum)) {
    const now = Date.now();
    if (now - lastTimestamp < 1000) {
      lastSecond++;
    } else {
      lastSecond = 0;
      lastTimestamp = now;
    }

    if (lastSecond > 10) {
      logger.warn(`Rate limiting block timestamp requests, waiting for a second`);
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
    const block = await publicClient.getBlock({ blockNumber: blockNum });
    blockTimestamps.set(block.number, new Date(Number(block.timestamp * 1000n)));
    storeTimestamps();
  }
  const ts = blockTimestamps.get(blockNum)?.getTime()! / 1000;
  const timeDiff = BigInt(ts) - earliestBlock.timestamp;
  const blockDiff = blockNum - earliestBlock.block;
  if (blockDiff !== 0n && timeDiff / blockDiff !== 2n) {
    console.error(`Time difference is not 2: ${timeDiff} / ${blockDiff} for block ${blockNumber} and earliest block ${earliestBlock.block}`);
  }
  return ts;
};
