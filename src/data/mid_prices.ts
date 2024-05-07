// @ts-check

import fs from "fs/promises";
import * as constants from "../constants";
import * as utils from "../utils";
import { client } from "../db";
import logger from "../logger";

import path from "path";

const blocksPerQuery = 10_000;
const fileHeader = "block,price";

/**
 * Gets all mid prices for a given market and stores them in a CSV file
 * @param base The base token for the market
 * @param quote The quote token for the market
 * @param key The combined key for the market
 */
const getPrices = async (base: string, quote: string, key: string) => {
  // Get prices file for this market
  const file = path.join(constants.dataDirectory, "prices", `${key}.csv`);

  // Load the query from the file and update the schema
  let midPriceQuery = await fs.readFile(path.join(__dirname, `mid_prices.sql`), "utf-8");
  midPriceQuery = midPriceQuery.replace(/sgd10/g, constants.schema);

  logger.info(`Getting all mid prices for ${key}`);

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
      if (Number.isNaN(lastBlock)) throw new Error(`Could not parse block number from ${lastBlock}`);
      logger.info(`Last block for ${key} is ${lastBlock}`);
      start = lastBlock;
    }
  } else {
    // If the file doesn't exist, create it with the headers
    await fs.writeFile(file, `${fileHeader}\n`);
  }

  // Store pending rows to save on I/O
  let pendingRows: {}[] = [];

  for (let block = start; block <= end; block += blocksPerQuery) {
    const startingTime = process.hrtime.bigint();

    const endBlock = Math.min(block + blocksPerQuery, end);

    // Grab the prices for this block range
    const res = await client.query(midPriceQuery, [base, quote, block, endBlock]);
    const timeToQuery = process.hrtime.bigint() - startingTime;
    if (res.rows.length > 0) {
      pendingRows.push(...res.rows);
    }

    // Avoid writing on each loop, to save on I/O
    if (pendingRows.length > 10_000) {
      const { out: csv } = utils.convertToCSV(pendingRows);
      await fs.appendFile(file, csv);
      pendingRows = [];
    }

    logger.debug(`Getting prices for ${key} from block ${block.toString().padStart("1000000000".length, " ")} to ${endBlock.toString().padStart("1000000000".length, " ")}  (${pendingRows.length.toString().padStart(5, " ")}) pending rows | Timings - Query: ${timeToQuery / 10n ** 6n}s`);
  }

  // Store any pending rows to the CSV file
  const { out: csv } = utils.convertToCSV(pendingRows);
  if (csv.length > 0) await fs.appendFile(file, csv);
};

/**
 * Cleans up a price file by removing duplicate prices that repeat
 * @param key The key for the market
 */
const cleanUpPrices = async (key: string) => {
  const file = path.join(constants.dataDirectory, "prices", `${key}.csv`);
  logger.info(`Cleaning up prices for ${key}`);

  const lines = await utils.readCSV<{ block: string; price: string }>(file);

  let out = "block,price\n";

  let lastPrice: string = "";
  for (let i = 0; i < lines.length; i++) {
    const { block, price } = lines[i];
    const blockNum = block.replace(/"/g, "");
    const priceNum = price.replace(/"/g, "").replace(/(\.\d*?)0+$/, "$1");

    if (lastPrice && lastPrice === priceNum) continue;

    lastPrice = priceNum;

    out += `${blockNum},${priceNum}\n`;
  }

  await fs.unlink(file).catch((e) => {});
  await fs.writeFile(file, out).catch((e) => {});
};

export const main = async () => {
  await client.connect();
  await fs.mkdir(path.join(constants.dataDirectory, "prices"), { recursive: true });

  for (let i = 0; i < constants.markets.length; i++) {
    const { base, quote, key } = constants.markets[i];
    await getPrices(base, quote, key);
  }

  for (let i = 0; i < constants.markets.length; i++) {
    const { key } = constants.markets[i];
    await cleanUpPrices(key);
  }

  await client.end();
};

main();
