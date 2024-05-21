import exp from "constants";

export const startOfEpoch1 = 178_473;
export const endOfEpoch1 = 1_258_497;

export const startOfEpoch2 = 1_258_498; // endOfEpoch1 + 1;
export const endOfEpoch2 = 1_560_904;

export const startOfEpoch3 = 1_560_905; // endOfEpoch2 + 1;
export const endOfEpoch3 = 1_905_514;

export const startOfEpoch4 = 1_905_515; // endOfEpoch3 + 1;
export const endOfEpoch4 = 2_207_893;

export const startOfEpoch5 = 2_207_894; // endOfEpoch4 + 1;
export const endOfEpoch5 = 2_510_293;

export const startOfEpoch6 = 2_510_294; // endOfEpoch5 + 1;
export const endOfEpoch6 = 2_812_693;

export const startOfEpoch7 = endOfEpoch6 + 1;
export const endOfEpoch7 = 3_115_093;

export const startOfEpoch8 = endOfEpoch7 + 1;
export const endOfEpoch8 = 3_417_493;

export const startOfEpoch9 = endOfEpoch8 + 1;
export const endOfEpoch9 = 3_719_893;

export const startOfEpoch10 = endOfEpoch9 + 1;

export const epochs = [
  { start: startOfEpoch1, end: endOfEpoch1 },
  { start: startOfEpoch2, end: endOfEpoch2 },
  { start: startOfEpoch3, end: endOfEpoch3 },
  { start: startOfEpoch4, end: endOfEpoch4 },
  { start: startOfEpoch5, end: endOfEpoch5 },
  { start: startOfEpoch6, end: endOfEpoch6 },
  { start: startOfEpoch7, end: endOfEpoch7 },
  { start: startOfEpoch8, end: endOfEpoch8 },
  { start: startOfEpoch9, end: endOfEpoch9 },
] as const;

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

export const NFTBoosts = {
  mainnet: 3,
  forest: 2.5,
  tree: 1.75,
  base: 1,
} as const;

export type MarketKeys = (typeof markets)[number]["key"];

export const schema = "sgd83"; // TODO: Replace with dynamic lookup

export const dataDirectory = ""; // Fill in with absolute path to data directory
if (!dataDirectory) throw new Error("dataDirectory not set");

export const v = 0.7;
export const d = 0.3;
export const nu = 5;

export const minSpread = 1e-6;
export const maxSpread = 0.02;

export const phi = (spread: number) => {
  const s = Math.abs(spread);
  if (s < minSpread) return minSpread;
  if (s > maxSpread) return Infinity;
  return s;
};

export const phiPrime = (spread: number) => {
  const s = Math.abs(spread);
  return Math.max(s, minSpread) ** 3;
};
