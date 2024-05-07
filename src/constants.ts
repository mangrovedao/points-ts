export const startOfEpoch3 = 1560905;
export const endOfEpoch3 = 1905514;

export const startOfEpoch4 = endOfEpoch3 + 1;
export const endOfEpoch4 = 2207893;

export const startOfEpoch5 = endOfEpoch4 + 1;
export const endOfEpoch5 = 2510293;

export const startOfEpoch6 = endOfEpoch5 + 1;
export const endOfEpoch6 = 2812693;

export const startOfEpoch7 = endOfEpoch6 + 1;
export const endOfEpoch7 = 3115093;

export const PUNKS40 = "999f220296B5843b2909Cc5f8b4204AacA5341D8";
export const PUNKS20 = "9a50953716bA58e3d6719Ea5c437452ac578705F";
export const WETH = "4300000000000000000000000000000000000004";
export const USDB = "4300000000000000000000000000000000000003";

export const names = {
  [PUNKS40]: "PUNKS40",
  [PUNKS20]: "PUNKS20",
  [WETH]: "WETH",
  [USDB]: "USDB",
} as const;

export const markets = [
  { base: PUNKS40, quote: WETH, key: "PUNKS40_WETH" },
  { base: PUNKS20, quote: WETH, key: "PUNKS20_WETH" },
  { base: WETH, quote: USDB, key: "WETH_USDB" },
] as const;

export const takerToQuote = 1;
export const makerToTaker = 4;
export const nonCompToComp = 1 / 10;

export const boosts = [
  { threshold: 500_000, boost: 4 },
  { threshold: 100_000, boost: 3.5 },
  { threshold: 50_000, boost: 3 },
  { threshold: 20_000, boost: 2.5 },
  { threshold: 10_000, boost: 1.75 },
  { threshold: 0, boost: 1 },
] as const;

export type MarketKeys = (typeof markets)[number]["key"];

export const schema = "sgd83"; // TODO: Replace with dynamic lookup

export const dataDirectory = ""; // Fill in with absolute path to data directory

