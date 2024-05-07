import * as utils from "./utils";
import * as constants from "./constants";
import * as path from "path";

type PricesForMarket = { [block: number]: number | null };
type MarketOnBlock = { block: number; key: constants.MarketKeys };

const midPrices = {} as { [K in constants.MarketKeys]: PricesForMarket };

export const loadMidPrices = async () => {
  for (const { key } of constants.markets) {
    const priceFile = path.join(constants.dataDirectory, "prices", `${key}.csv`);
    const pricesFromFile = await utils.readCSV<{ block: string; price: string }>(`${priceFile}.csv`);
    const prices = pricesFromFile.reduce((acc, { block, price }) => {
      acc[Number(block)] = JSON.parse(price);
      return acc;
    }, {} as PricesForMarket);
    midPrices[key] = prices;
  }
};

// Need to find the closest block to the block we are looking for when the price is missing
export const findMidPrice = ({ block, key }: MarketOnBlock) => {
  let i = block;
  const prices = midPrices[key];
  let price = prices[block];
  while (price == undefined) {
    // TODO: This can be optimized by grabbing the keys and going back one
    if (i < block - 1_000_000) {
      throw new Error(`Could not find mid price for ${key} at block ${block}`);
    }
    price = prices[i--];
  }

  return Number(price);
};

export const midPriceForBaseInUSD = (market: MarketOnBlock): number => {
  // TODO: Does not work for arbitrary base, we should move to A* search
  if (!market.key.endsWith("_USDB")) {
    const midPriceInQuote = findMidPrice(market);
    const quoteInUSD = midPriceForBaseInUSD({ block: market.block, key: `${utils.getQuote(market.key) as "WETH"}_USDB` });
    return midPriceInQuote * quoteInUSD;
  }
  return findMidPrice(market);
};
