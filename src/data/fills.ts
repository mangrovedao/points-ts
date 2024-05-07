// @ts-check

import fs from "fs/promises";
import * as constants from "../constants";
import * as utils from "../utils";
import { client } from "../db";
import logger from "../logger";

import path from "path";

const blocksPerQuery = 100_000; // 100k blocks per query is fine for WETH_USDB fills
const fileHeader = "block,fills";

const getFills = async (base: string, quote: string, key: string) => {
  const file = path.join(constants.dataDirectory, "fills", `${key}.csv`);

  let fillsQuery = await fs.readFile(path.join(__dirname, `fills.sql`), "utf-8");
  fillsQuery = fillsQuery.replace(/sgd10/g, constants.schema);

  logger.info(`Getting all fills for ${key}`);

  let start = 0;
  const end = await utils.getBlockNumber();

  // Check if the file exists, and if it does, load the last line to get the last block we pulled
  if (await utils.existsAsync(file)) {
    const lastLine = await utils.lastLine(file);
    const lastBlock = Number(lastLine?.split(",")[0]);
    if (lastLine === fileHeader) {
      logger.warn(`No data found for ${key}, attempting to start from genesis`);
      start = 0;
    } else {
      if (Number.isNaN(lastBlock)) {
        throw new Error(`Could not parse block number from ${lastBlock}`);
      }
      logger.info(`Last block for ${key} is ${lastBlock}`);
      start = lastBlock + 1; // We don't need to re-query the last block itself
    }
  } else {
    // If the file doesn't exist, create it with the headers
    await fs.writeFile(file, `${fileHeader}\n`);
  }

  let pendingRows: {}[] = [];

  for (let block = start; block <= end; block += blocksPerQuery) {
    const startingTime = process.hrtime.bigint();

    const endBlock = Math.min(block + blocksPerQuery, end);

    const res = await client.query(fillsQuery, [base, quote, block, endBlock]);
    const timeToQuery = process.hrtime.bigint() - startingTime;
    if (res.rows.length > 0) {
      pendingRows.push(...res.rows);
    }

    // Avoid writing on each loop, to save on I/O
    if (pendingRows.length > 10) {
      const { out: csv } = utils.convertToCSV(pendingRows);
      await fs.appendFile(file, csv);
      pendingRows = [];
    }

    logger.debug(`Getting fills for ${key} from block ${block.toString().padStart("1000000000".length, " ")} to ${endBlock.toString().padStart("1000000000".length, " ")}  (${pendingRows.length.toString().padStart(5, " ")}) pending rows | Timings - Query: ${timeToQuery / 10n ** 6n}s`);
  }

  // Store any pending rows
  const { out: csv } = utils.convertToCSV(pendingRows);
  if (csv.length > 0) await fs.appendFile(file, csv);
};

export const main = async () => {
  await client.connect();
  await fs.mkdir(path.join(constants.dataDirectory, "fills"), { recursive: true });

  for (let i = 0; i < constants.markets.length; i++) {
    const { base, quote, key } = constants.markets[i];
    await getFills(base, quote, key);
  }

  await client.end();
};

main();
