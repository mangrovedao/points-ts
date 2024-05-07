// @ts-check

import fs from "fs/promises";
import * as constants from "../constants";
import * as utils from "../utils";
import { client } from "../db";
import logger from "../logger";

import path from "path";

// Depending on the market, we need to query different amounts of blocks
const blocksPerQuery = { WETH_USDB: 10_000, PUNKS20_WETH: 1_000_000, PUNKS40_WETH: 1_000_000 };
const fileHeader = "block,book";

/**
 * Gets all books for a given market and stores them in a CSV file
 * @param base The base token for the market
 * @param quote The quote token for the market
 * @param key The combined key for the market
 */
const getBooks = async (base: string, quote: string, key: keyof typeof blocksPerQuery) => {
  const file = path.join(constants.dataDirectory, "books", `${key}.csv`);

  let booksQuery = await fs.readFile(path.join(__dirname, `books.sql`), "utf-8");
  booksQuery = booksQuery.replace(/sgd10/g, constants.schema);

  logger.info(`Getting all books for ${key}`);
  let start = 0;
  const end = await utils.getBlockNumber();

  if (await utils.existsAsync(file)) {
    const lastLine = await utils.lastLine(file);
    const lastBlock = Number(lastLine?.split(",")[0]);
    if (lastLine === fileHeader) {
      logger.warn(`No data found for ${key}, attempting to start from genesis`);
      start = 0;
    } else {
      if (Number.isNaN(lastBlock)) throw new Error(`Could not parse block number from ${lastBlock}`);
      start = lastBlock + 1;
    }
  } else {
    // If the file doesn't exist, create it with the headers
    await fs.writeFile(file, `${fileHeader}\n`);
  }

  let pendingRows: {}[] = [];

  for (let block = start; block <= end; block += blocksPerQuery[key]) {
    const startingTime = process.hrtime.bigint();

    const endBlock = Math.min(block + blocksPerQuery[key], end);

    const res = await client.query(booksQuery, [base, quote, block, endBlock]);
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

    logger.debug(`Getting books for ${key} from block ${block} to ${endBlock}  (${pendingRows.length.toString().padStart(5, " ")}) pending rows | Timings - Query: ${timeToQuery / 10n ** 6n}s`);
  }

  // Store any pending rows
  const { out: csv } = utils.convertToCSV(pendingRows);
  if (csv.length > 0) await fs.appendFile(file, csv);
};

export const main = async () => {
  await client.connect();
  await fs.mkdir(path.join(constants.dataDirectory, "books"), { recursive: true });

  for (let i = 0; i < constants.markets.length; i++) {
    const { base, quote, key } = constants.markets[i];
    await getBooks(base, quote, key);
  }

  await client.end();
};

main();
