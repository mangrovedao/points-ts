import fsP from "fs/promises";

import * as constants from "./constants";
import * as utils from "./utils";
import path from "path";

type MarketKeys = (typeof constants.markets)[number]["key"];

type NFTBoost = 1 | 1.75 | 2.5 | 3;
type VolumeBoost = 1 | 1.75 | 2.5 | 3 | 3.5 | 4;

type DepthRow = { address: string; makerVolume: string; takerPoints: string; makerPoints: string; takerVolume: string; ttmp: string; makerVolumeLastEpoch: string; takerVolumeLastEpoch: string };

type UserTotals = { makerVolume: number; takerVolume: number; makerVolumeLastEpoch: number; takerVolumeLastEpoch: number; takerPoints: number; makerPoints: number; ttmp: number; boostFromNFT: NFTBoost; boostFromVolume: VolumeBoost; afterBoost: number; pointsGainedByReferring: number; grandTotal: number };

const baseTotals = () => ({ grandTotal: 0, makerVolume: 0, takerVolume: 0, makerVolumeLastEpoch: 0, takerVolumeLastEpoch: 0, totalVolumeLastEpoch: 0, takerPoints: 0, makerPoints: 0, combinedVolume: 0, ttmp: 0, afterBoost: 0, boostFromNFT: 1, boostFromVolume: 1, pointsGainedByReferring: 0 } as const);

const computeGrandTotals = async (startBlock: number, endBlock: number) => {
  let totalTotalTaker = 0;
  let totalTotalMaker = 0;

  const totals: { [user: string]: UserTotals } = {};

  const aggregateDepth = async (key: MarketKeys, startBlock: number, endBlock: number) => {
    const data = await utils.readCSV<DepthRow>(path.join(constants.dataDirectory, "depth", key, `${startBlock}-${endBlock}.csv`));

    for (const d of data) {
      const address = d.address;
      totals[address] = totals[address] ?? baseTotals();

      for (const key of Object.keys(d) as (keyof DepthRow)[]) {
        if (key === "address") continue;
        totals[address][key] += Number(d[key]);
      }
    }

    totalTotalMaker += data.reduce((acc, val) => acc + Number(val.makerVolume), 0);
    totalTotalTaker += data.reduce((acc, val) => acc + Number(val.takerVolume), 0);
  };

  for (const { key } of constants.markets) {
    await aggregateDepth(key, startBlock, endBlock);
  }

  const totalTotalCombined = totalTotalMaker + totalTotalTaker;

  for (const address of Object.keys(totals)) {
    const volume = totals[address].takerVolumeLastEpoch + totals[address].makerVolumeLastEpoch;
    totals[address].boostFromVolume = constants.boosts.find((b) => volume >= b.threshold)!.boost;
  }

  for (const address of Object.keys(totals)) {
    totals[address].afterBoost = totals[address].ttmp * Math.max(totals[address].boostFromVolume, totals[address].boostFromNFT);
  }

  const refs = require(`${constants.dataDirectory}/referrals.json`) as { referrer: string; referee: string; block_referred: number }[];
  const referrals = refs.filter((a) => Number(a.block_referred) < constants.endOfEpoch7); // TODO: This is a placeholder
  const referrers = [...new Set(referrals.map((r) => r.referrer))] as string[];

  for (const referrer of referrers) {
    const referred = referrals.filter((r) => r.referrer === referrer);
    if (!totals[referrer]) totals[referrer] = baseTotals();
    for (const r of referred) {
      totals[referrer].pointsGainedByReferring += Number(totals[r.referee]?.afterBoost ?? 0) / 10;
    }
  }

  for (const referrer of referrers) {
    if (JSON.stringify(totals[referrer]) == JSON.stringify(baseTotals())) {
      delete totals[referrer];
    }
  }

  for (const address of Object.keys(totals)) {
    const isReferee = referrals.some((r) => r.referee === address);
    const isReferrer = referrers.includes(address);
    if (isReferrer || isReferee) {
      totals[address].grandTotal = totals[address].afterBoost * 1.1 + Number(totals[address].pointsGainedByReferring ?? 0);
    } else {
      totals[address].grandTotal = Number(totals[address].afterBoost);
    }
  }

  const totalTotal = Object.keys(totals).reduce((acc, val) => acc + totals[val].grandTotal, 0);

  const data = Object.keys(totals)
    .map((address) => {
      const { makerVolume, takerVolume, takerPoints, makerPoints, afterBoost, boostFromNFT, boostFromVolume, pointsGainedByReferring, grandTotal } = totals[address];
      return { address, makerVolume, takerVolume, takerPoints, makerPoints, combinedVolume: takerVolume + makerVolume, boostFromVolume, boostFromNFT, pointsGainedByReferring, boostedTotals: afterBoost, grandTotal, share: (grandTotal / totalTotal) * 100 };
    })
    .sort((a, b) => b.share - a.share);

  data.forEach((d, i) => {
    d.rank = i + 1;
  });

  const tp = data.map((x) => x.takerPoints).reduce((a, b) => a + b, 0);
  const mp = data.map((x) => (totals[x.address].ttmp ?? x.takerPoints) - x.takerPoints ?? 0).reduce((a, b) => a + b, 0);

  const { out: csv, headers } = utils.convertToCSV(data);

  const minSpread = (constants.minSpread / 10e12) * 10e12;
  await fsP.writeFile(`${constants.dataDirectory}/grand_totals/${startBlock}-${endBlock}-${constants.v}-${constants.d}-${minSpread}.csv`, headers.join(",") + "\n" + csv);

  const fmtNumber = (x: number) => x.toLocaleString("en-US", { maximumFractionDigits: 2 }).padStart(20, " ");
  console.table(
    [
      { name: "Taker Points", value: tp },
      { name: "Maker Points", value: mp },
      { name: "Total Points", value: totalTotal },
      { name: "Total Maker Volume", value: totalTotalMaker },
      { name: "Total Taker Volume", value: totalTotalTaker },
      { name: "Total Combined Volume", value: totalTotalCombined },
    ].map(({ name, value }) => ({ name, value: fmtNumber(value) }))
  );
};

const main = async () => {
  console.time("Run");

  await fsP.mkdir(`${constants.dataDirectory}/grand_totals`, { recursive: true });

  await fetch("http://data.mangrove.exchange/referrals")
    .then((response) => {
      return response.json();
    })
    .then((data) => {
      return fsP.writeFile(`${constants.dataDirectory}/referrals.json`, JSON.stringify(data));
    });

  for (let i = 0; i < constants.epochs.length; i++) {
    const epoch = constants.epochs[i];
    if (epoch.start < constants.startOfEpoch8) continue;

    console.log(`Grand totals for for epoch ${epoch.start} - ${epoch.end}`);
    await computeGrandTotals(epoch.start, epoch.end);
  }

  console.timeEnd("Run");
};

main();
