import fs from "fs/promises";

import * as constants from "./constants";
import * as utils from "./utils";
import * as midPriceUtils from "./mid_price";
import path from "path";
import logger from "./logger";

type Fill = { maker: string; taker: string; maker_got_display: number; offer_type: "ask" | "bid"; maker_gave_display: number };
type Epoch = { start: number; end: number };

const makerDir = (key: constants.MarketKeys) => path.join(constants.dataDirectory, "volume", "maker", key);
const takerDir = (key: constants.MarketKeys) => path.join(constants.dataDirectory, "volume", "taker", key);
const cumulativeVolumeFile = (key: constants.MarketKeys) => path.join(constants.dataDirectory, "volume", "cumulative", `${key}.csv`);

export const getVolumesForEpoch = async (key: constants.MarketKeys, epoch: Epoch) => {
  const volumes = [];
  const fillsFile = path.join(constants.dataDirectory, "fills", `${key}.csv`);
  const [_, ...raw] = await fs.readFile(fillsFile, "utf8").then((x) => x.split("\n").filter(Boolean));
  const data = raw
    .map((line: string) => {
      const [block, ...fillsSplit] = line.split(",");
      const blockNumber = Number(block);
      // Skip any out of range lines
      if (blockNumber < epoch.start || blockNumber > epoch.end) return;
      const fillsParsed = JSON.parse(fillsSplit.join(","));
      return { blockNumber, fills: fillsParsed };
    })
    .filter(Boolean) as { blockNumber: number; fills: Fill[] }[];

  const makers: { [key: string]: number } = {};
  const takers: { [key: string]: number } = {};

  for (let i = 0; i < data.length; i++) {
    const priceOfQuoteInUSD = key.endsWith("_USDB") ? 1 : midPriceUtils.midPriceForBaseInUSD({ key: `${utils.getQuote(key) as "WETH"}_USDB`, block: data[i].blockNumber });
    let volumeOnBlock = 0;
    for (const { maker, taker, maker_got_display, offer_type, maker_gave_display } of data[i].fills) {
      const gain = offer_type === "ask" ? maker_got_display * priceOfQuoteInUSD : maker_gave_display * priceOfQuoteInUSD;

      makers[maker] = (makers[maker] ?? 0) + gain;
      takers[taker] = (takers[taker] ?? 0) + gain;

      volumeOnBlock += gain;
    }
    volumes.push({ block: data[i].blockNumber, volume_in_usd: volumeOnBlock });
  }

  const makerPointsArray = Object.keys(makers).map((address) => ({ address, usd: makers[address] }));
  const takerPointsArray = Object.keys(takers).map((address) => ({ address, usd: takers[address] }));

  makerPointsArray.sort((a, b) => b.usd - a.usd);
  takerPointsArray.sort((a, b) => b.usd - a.usd);

  const { out: makerPointsCSV, headers: makerPointsHeaders } = utils.convertToCSV(makerPointsArray);
  const { out: takerPointsCSV, headers: takerPointsHeaders } = utils.convertToCSV(takerPointsArray);

  await fs.writeFile(path.join(makerDir(key), `${epoch.start}-${epoch.end}.csv`), makerPointsHeaders.join(",") + "\n" + makerPointsCSV);
  await fs.writeFile(path.join(takerDir(key), `${epoch.start}-${epoch.end}.csv`), takerPointsHeaders.join(",") + "\n" + takerPointsCSV);

  return volumes;
};

const main = async () => {
  const cumulativeVolumes = {};

  await midPriceUtils.loadMidPrices();
  for (const { key } of constants.markets) {
    cumulativeVolumes[key] = [];
    await fs.mkdir(makerDir(key), { recursive: true });
    await fs.mkdir(takerDir(key), { recursive: true });
    await fs.mkdir(path.join(constants.dataDirectory, "volume", "cumulative"), { recursive: true });
    for (const epoch of constants.epochs) {
      logger.info(`Volumes for ${key} on epoch ${epoch.start}-${epoch.end}`);
      const cumulativeVolumesForEpoch = await getVolumesForEpoch(key, epoch);
      cumulativeVolumes[key] = cumulativeVolumes[key].concat(cumulativeVolumesForEpoch);
    }

    let sum = 0;
    cumulativeVolumes[key] = cumulativeVolumes[key].map((v) => {
      sum += v.volume_in_usd;
      return { ...v, cumulative: sum };
    });

    const { out, headers } = utils.convertToCSV(cumulativeVolumes[key]);

    await fs.writeFile(cumulativeVolumeFile(key), headers + "\n" + out);
  }
};

main();
