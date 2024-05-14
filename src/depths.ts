import fs from "fs";

import * as constants from "./constants";
import * as utils from "./utils";
import * as midPriceUtils from "./mid_price";
import path from "path";

type MakerVolume = { address: string; usd: string; vm: string };
type TakerVolume = { address: string; usd: string };

type MarketKeys = (typeof constants.markets)[number]["key"];

type MakerScore = { seenCount: number; D_u: number; mpPrime: number; D_u_d: number; D_a: number; D_b: number; uptime: number; mp: number; u_u: number; mVolume: number; amp: number };
type Offer = { maker: string; offer_type: "ask" | "bid"; price: string; gives_display: string };

type BlockData = { [Maker: string]: { U: number; D_U_T: number; mpPrime: number; askMpRaw: number; bidMpRaw: number } };
type BlocksSeen = { [Maker: string]: number };

const computeDepthForEpoch = async (key: MarketKeys, startBlock: number, endBlock: number, lastEpochStart: number, lastEpochEnd: number, seenOnMarket: Set<string>) => {
  const blocksToSee = endBlock - startBlock + 1;

  let makerScores: { [key: string]: MakerScore } = {};

  let lastBlock = -1;
  let lastBlockData: BlockData = {};

  const blocksSeen: BlocksSeen = {};

  const repeatLastBook = (repeats: number) => {
    for (const maker in lastBlockData) {
      const { D_U_T, mpPrime, askMpRaw, bidMpRaw, U } = lastBlockData[maker];
      makerScores[maker].D_u += D_U_T * repeats;
      makerScores[maker].mpPrime += mpPrime * repeats;
      makerScores[maker].D_u_d += Math.pow(D_U_T, constants.d) * repeats;
      makerScores[maker].D_a += askMpRaw * repeats;
      makerScores[maker].D_b += bidMpRaw * repeats;
      blocksSeen[maker] = (blocksSeen[maker] ?? 0) + repeats * U;
    }
  };

  const computeDepthForBook = (book: Offer[], block: number, key: MarketKeys) => {
    const makers = [...new Set(book.map((offer) => offer.maker))];

    const priceOfQuoteInUSD = key.endsWith("_USDB") ? 1 : midPriceUtils.midPriceForBaseInUSD({ key: `${utils.getQuote(key) as "WETH"}_USDB`, block });
    const priceOfBaseInQuote = midPriceUtils.findMidPrice({ key, block });
    const midPrice = midPriceUtils.findMidPrice({ key, block });

    for (const maker of makers) {
      seenOnMarket.add(maker);

      const offers = book
        .filter((val) => val.maker === maker)
        .map((val) => {
          const spread = (Number(val.price) - midPrice) / midPrice;
          const value = val.offer_type === "ask" ? priceOfBaseInQuote * priceOfQuoteInUSD : priceOfQuoteInUSD;
          const offerVal = Math.min(value * Number(val.gives_display), 50_000);
          const mpRaw = offerVal / constants.phi(spread);
          const mpPrimeRaw = offerVal / constants.phiPrime(spread);
          return { ...val, spread, mpRaw, mpPrimeRaw };
        });
      const asks = offers.filter((val) => val.offer_type === "ask");
      const bids = offers.filter((val) => val.offer_type === "bid");

      const askMpRaw = asks.reduce((acc, val) => acc + val.mpRaw, 0);
      const bidMpRaw = bids.reduce((acc, val) => acc + val.mpRaw, 0);

      const askMpPrimeRaw = asks.reduce((acc, val) => acc + val.mpPrimeRaw, 0);
      const bidMpPrimeRaw = bids.reduce((acc, val) => acc + val.mpPrimeRaw, 0);

      const mpPrime = askMpPrimeRaw + bidMpPrimeRaw;
      const D_U_T = Math.min(askMpRaw, bidMpRaw);
      const U = D_U_T > 0 ? 1 : 0;

      if (!(maker in makerScores)) {
        makerScores[maker] = { D_u: 0, mpPrime: 0, seenCount: 0, D_u_d: 0, D_a: 0, D_b: 0, uptime: 0, mp: 0, u_u: 0, mVolume: 0, amp: 0 };
      } else {
        makerScores[maker].seenCount++;
      }

      lastBlockData[maker] = { D_U_T, mpPrime, askMpRaw, bidMpRaw, U };
      makerScores[maker].D_u += D_U_T;
      makerScores[maker].D_u_d += Math.pow(D_U_T, constants.d);
      makerScores[maker].mpPrime += mpPrime;
      makerScores[maker].D_a += askMpRaw;
      makerScores[maker].D_b += bidMpRaw;
      blocksSeen[maker] = (blocksSeen[maker] ?? 0) + U;
    }
  };

  let cachedLine = "";
  let hasProcessedFirstBook = false;

  /** @param {string} line */
  const computeDepths = (line: string) => {
    const [blockNumber, ...bookSplit] = line.split(",").map((x) => x.trim());
    let block = Number(blockNumber);

    if (block < startBlock) {
      cachedLine = line;
      return;
    }
    if (block > endBlock) {
      return;
    }

    // Only get here once block >= startBlock

    if (!hasProcessedFirstBook) {
      hasProcessedFirstBook = true;
      if (cachedLine !== "") {
        computeDepths(startBlock + "," + cachedLine.split(",").slice(1).join(","));
      }
    }

    const isNotFirstLine = lastBlock !== -1;
    const isGap = lastBlock !== block - 1;
    if (isNotFirstLine && isGap) {
      // Duplicate last block
      const blockedPassed = block - lastBlock - 1;

      repeatLastBook(blockedPassed);
    }
    lastBlockData = {};
    lastBlock = block;

    const book: Offer[] = JSON.parse(bookSplit.join(","));

    computeDepthForBook(book, block, key);
  };

  const isPastEndOfEpoch = (line: string) => {
    const [blockNumber] = line.split(",").map((x) => x.trim());
    const block = Number(blockNumber);
    return block > endBlock;
  };

  await utils.processLineByLine(path.join(constants.dataDirectory, "books", `${key}.csv`), computeDepths, isPastEndOfEpoch);
  // await utils.processLineByLine(path.join(constants.dataDirectory, "books", `${key}-${startBlock}-${endBlock}.csv`), computeDepths);

  if (lastBlock !== endBlock) {
    const missingBlocks = endBlock - lastBlock - 1;
    // Duplicate last block
    for (const maker of Object.keys(lastBlockData)) {
      seenOnMarket.add(maker);
      const { D_U_T, mpPrime, askMpRaw, bidMpRaw, U } = lastBlockData[maker];

      makerScores[maker].D_u += D_U_T * missingBlocks;
      makerScores[maker].mpPrime += mpPrime * missingBlocks;
      makerScores[maker].D_u_d += Math.pow(D_U_T, constants.d) * missingBlocks;
      makerScores[maker].D_a += askMpRaw * missingBlocks;
      makerScores[maker].D_b += bidMpRaw * missingBlocks;

      blocksSeen[maker] = (blocksSeen[maker] ?? 0) + missingBlocks * U;
    }
    lastBlock = endBlock;
  }

  const makerVolumesArray = await utils.readCSV<MakerVolume>(path.join(constants.dataDirectory, "volume", "maker", key, `${startBlock}-${endBlock}.csv`));
  const makerVolumes: { [user: string]: { usd: number; Vm: number } } = {};
  for (const { address, usd } of makerVolumesArray) {
    makerVolumes[address] = { usd: Number(usd), Vm: Math.pow(Number(usd), constants.v) };
  }

  const makerVolumesLastEpoch: { [user: string]: number } = {};

  if (lastEpochStart !== 0) {
    const makerVolumesArray2 = await utils.readCSV<MakerVolume>(path.join(constants.dataDirectory, "volume", "maker", key, `${lastEpochStart}-${lastEpochEnd}.csv`));
    for (const { address, usd } of makerVolumesArray2) {
      makerVolumesLastEpoch[address] = Number(usd);
    }
  }

  const takerVolumesArray = await utils.readCSV<TakerVolume>(path.join(constants.dataDirectory, "volume", "taker", key, `${startBlock}-${endBlock}.csv`));
  const takerVolumes: { [user: string]: number } = {};
  for (const { address, usd } of takerVolumesArray) {
    seenOnMarket.add(address);
    takerVolumes[address] = Number(usd);
  }

  const takerVolumesLastEpoch: { [user: string]: number } = {};

  if (lastEpochStart !== 0) {
    const takerVolumesArray2 = await utils.readCSV<TakerVolume>(path.join(constants.dataDirectory, "volume", "taker", key, `${lastEpochStart}-${lastEpochEnd}.csv`));
    for (const { address, usd } of takerVolumesArray2) {
      takerVolumesLastEpoch[address] = Number(usd);
    }
  }

  const sumVt = Object.keys(takerVolumes).reduce((acc, val) => acc + (takerVolumes[val] ?? 0), 0);

  const takersAsMakers = [...seenOnMarket].map((address) => ({ address, points: { D_b: Number(0), D_u_d: Number(0), mpPrime: Number(0), D_a: Number(0), D_u: Number(0) } }));

  const makerScoresArray = Object.keys(makerScores)
    .map((address) => ({ address, points: makerScores[address] }))
    .concat(takersAsMakers.filter((a) => !(a.address in makerScores)));

  makerScoresArray.sort((a, b) => b.points.D_b - a.points.D_b);

  for (const { address, points } of makerScoresArray) {
    const uptime = (blocksSeen[address] ?? 0) / blocksToSee;
    if (blocksSeen[address] > blocksToSee) {
      console.log("ERROR: blocksSeen[address] > blocksToSee: ", blocksSeen[address], blocksToSee, address);
      blocksSeen[address] = blocksToSee;
    }
    const u_u = Math.pow(Number(uptime), constants.nu);
    const mVolume = Number(makerVolumes[address]?.Vm ?? 0);
    const mp = (points.D_u_d * u_u * mVolume) / 30; // 30 block sample factor
    points.mp = mp;
    points.mVolume = mVolume;
    points.u_u = u_u;
  }

  const sumVm = makerScoresArray.reduce((acc, val) => acc + val.points.mp, 0);

  const scaleFactor = sumVm == 0 ? 0 : (sumVt / sumVm) * constants.takerToQuote * constants.makerToTaker;

  for (const { points } of makerScoresArray) {
    points.amp = points.mp * scaleFactor;
  }

  const sumMpp = makerScoresArray.reduce((acc, val) => acc + val.points.mpPrime, 0);
  const sumAmp = makerScoresArray.reduce((acc, val) => acc + val.points.amp, 0);

  const scaleFactor2 = (constants.nonCompToComp * sumAmp) / sumMpp;

  const dataOutArr: { address: string }[] = [];
  for (const { address, points } of makerScoresArray) {
    if (!seenOnMarket.has(address)) continue;
    const { D_a, D_b, D_u_d, mpPrime, D_u, mp, mVolume, u_u, amp } = points;
    const takerVolume = takerVolumes[address] ?? 0;
    const makerVolume = makerVolumes[address]?.usd ?? 0;
    const takerPoints = takerVolume * constants.takerToQuote;
    const ampp = scaleFactor2 * mpPrime;
    const makerPoints = ampp + amp;
    const ttmp = makerPoints + takerPoints;
    const makerVolumeLastEpoch = makerVolumesLastEpoch[address] ?? 0;
    const takerVolumeLastEpoch = takerVolumesLastEpoch[address] ?? 0;
    const data = { address, takerVolumeLastEpoch, makerVolumeLastEpoch, takerVolume, makerVolume, ttmp, makerPoints, ampp, takerPoints, D_a, D_b, D_u, D_u_d, mp, Vm: mVolume, mpPrime, amp, uptime: u_u };
    dataOutArr.push(data);
  }

  const { out: csv2, headers: headers2 } = utils.convertToCSV(dataOutArr);

  const depthFile = path.join(constants.dataDirectory, "depth", key, `${startBlock}-${endBlock}.csv`);

  if (fs.existsSync(depthFile)) {
    // Attempt to load and compare, to allow for easier debugging
    const existingData = await utils.readCSV<any>(depthFile);

    for (const x of existingData) {
      const found = dataOutArr.find((y) => y.address === x.address);
      if (!found) {
        console.log("Missing data for", x.address);
      } else {
        for (const key of Object.keys(x)) {
          if (key === "address") continue;
          if (utils.fmtNumber(+x[key]) !== utils.fmtNumber(+found[key])) {
            throw new Error(`Mismatch for ${x.address} at ${key}: ${x[key]} !== ${found[key]}`);
          }
        }
      }
    }
  }

  fs.writeFileSync(depthFile, headers2.join(",") + "\n" + csv2);
};

const seenOnMarketGlobal: { [market: string]: Set<string> } = {};
const main = async () => {
  console.time("Run");

  await midPriceUtils.loadMidPrices();

  for (const market of constants.markets) {
    const { key } = market;
    fs.mkdirSync(`${constants.dataDirectory}/depth/${key}`, { recursive: true });

    for (let i = 0; i < constants.epochs.length; i++) {
      const epoch = constants.epochs[i];
      const lastEpoch = i === 0 ? { start: 0, end: 0 } : constants.epochs[i - 1];

      seenOnMarketGlobal[key] = new Set();
      const takerVolumes = await utils.readCSV<TakerVolume>(path.join(constants.dataDirectory, "volume", "taker", key, `${epoch.start}-${epoch.end}.csv`));
      for (const { address } of takerVolumes) {
        seenOnMarketGlobal[key].add(address);
      }
      const makerVolumes = await utils.readCSV<MakerVolume>(path.join(constants.dataDirectory, "volume", "maker", key, `${epoch.start}-${epoch.end}.csv`));
      for (const { address } of makerVolumes) {
        seenOnMarketGlobal[key].add(address);
      }
      console.log(`Depth for ${key} for epoch ${epoch.start} - ${epoch.end}`);
      await computeDepthForEpoch(key, epoch.start, epoch.end, lastEpoch.start, lastEpoch.end, seenOnMarketGlobal[key]);
    }
  }

  console.timeEnd("Run");
};

main();
