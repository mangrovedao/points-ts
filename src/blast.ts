import { privateKeyToAccount } from "viem/accounts";
import Big from "big.js";
import { z } from "zod";
import { readCSV } from "./utils";

const privateKey = process.env.PRIVATE_KEY!;
const contractAddress = "0xb1a49C54192Ea59B233200eA38aB56650Dfb448C";
const operator = privateKeyToAccount(privateKey);
const operatorAddress = operator.address;

const challengeURL = "https://waitlist-api.prod.blast.io/v1/dapp-auth/challenge";
const solveURL = "https://waitlist-api.prod.blast.io/v1/dapp-auth/solve";

let bearerTokenCache = "";
let lastPoll = 0;

export const getBearerToken = async () => {
  const response = await fetch(challengeURL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      contractAddress,
      operatorAddress,
    }),
  });

  const data = (await response.json()) as { message: string; challengeData: string; success: boolean };

  const signature = await operator.signMessage({ message: data.message });

  const tokenRes = await fetch(solveURL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      signature,
      challengeData: data.challengeData,
    }),
  });

  const { bearerToken } = (await tokenRes.json()) as { bearerToken: string; success: boolean };

  if (!bearerTokenCache || lastPoll + 60 * 1000 < Date.now()) {
    bearerTokenCache = bearerToken;
    lastPoll = Date.now();
  }

  return bearerToken;
};

// TODO: May need cursor to page through all batches
export const getAllBatches = async () => {
  const bearer = await getBearerToken();
  return await fetch(`https://waitlist-api.prod.blast.io/v1/contracts/${contractAddress}/batches`, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${bearer}`,
      "Content-Type": "application/json",
    },
  }).then((res) => res.json());
};

export const cancelBatch = async (batchId: string) => {
  const bearer = await getBearerToken();

  return await fetch(`https://waitlist-api.prod.blast.io/v1/contracts/${contractAddress}/batches/${batchId}`, {
    method: "DELETE",
    headers: {
      Authorization: `Bearer ${bearer}`,
      "Content-Type": "application/json",
    },
  }).then((res) => res.json());
};

export const sendBatch = async (batch: Batch) => {
  const bearer = await getBearerToken();

  return await fetch(`https://waitlist-api.prod.blast.io/v1/contracts/${contractAddress}/batches`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${bearer}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(batch),
  }).then((res) => res.json());
};

const zodBigNumber = z.coerce.string().transform<Big>((n) => new Big(n));
const zodAddressInHex = z.string().transform((addr) => (addr.at(1) != "x" ? `0x${addr}` : addr));

const distSchema = z.object({
  rank: z.coerce.number(),
  address: zodAddressInHex,
  grandTotal: zodBigNumber,
  share: zodBigNumber,
});

type DistType = z.infer<typeof distSchema>;

type BatchKind = "DEVELOPER" | "LIQUIDITY";

export type Transfer = {
  toAddress: string;
  points: string;
};

export type Batch = {
  pointType: BatchKind;
  transfers: Transfer[];
};

/**
 * @param goldCalcFn Used to supply a custom function to calculate gold for each row (used for ranks)
 */
export const sendBatchFromFile = async (file: string, totalToDistribute: Big, type: BatchKind, goldCalcFn?: (row: DistType) => Big) => {
  const round = { DEVELOPER: 6, LIQUIDITY: 2 }[type];
  const leaderboard = await readCSV<{ rank: string; account: string; gold: string }>(file);
  const points = leaderboard.map((row) => distSchema.parse(row)) as DistType[];

  const rowToGold = goldCalcFn ?? ((row: DistType) => totalToDistribute.mul(row.share.div(100)));

  const pointsPerAccount = points
    .reduce((acc, row) => {
      if (row.grandTotal.eq(0)) return acc;
      const points = rowToGold(row).round(round).toString();
      if (points === "0") return acc;
      acc.push({ points, toAddress: row.address });
      return acc;
    }, [] as Transfer[])
    .sort((a, b) => Big(b.points).cmp(Big(a.points)));

  const totalDistributed = pointsPerAccount.reduce((acc, row) => acc.plus(Big(row.points)), Big(0));

  if (totalDistributed.gt(totalToDistribute.plus(0.0001))) {
    throw new Error(`Total distributed points is greater than wanted: ${totalDistributed} vs ${totalToDistribute}`);
  }

  console.log(`Total distributed: ${totalDistributed} vs ${totalToDistribute}`);

  await sendBatch({ pointType: type, transfers: pointsPerAccount });
};
