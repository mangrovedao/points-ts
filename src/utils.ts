import fs from "fs";
import fsP from "fs/promises";
import readline from "readline";
import { MarketKeys } from "./constants";
import { createPublicClient, http } from "viem";

export async function processLineByLine(name: string, f: (line: string) => void, stopFn?: (line: string) => boolean) {
  const fileStream = fs.createReadStream(name);
  const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

  let first = true;
  let i = 0;
  for await (const line of rl) {
    if (line === "" || line == undefined) continue;
    if (first) {
      first = false;
      continue;
    }
    if (stopFn && stopFn(line)) break;
    f(line);
    i++;
  }

  return i;
}

export async function lastLine(name: string) {
  const fileStream = fs.createReadStream(name);
  const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

  let lineToReturn;
  for await (const line of rl) {
    if (line === "" || line == undefined) continue;
    lineToReturn = line;
  }

  return lineToReturn;
}

export const remove0xPrefix = (hexString: string) => hexString.replace("0x", "").toLowerCase();
export const eq = (a: string, b: string) => remove0xPrefix(a) === remove0xPrefix(b);
export const fmtNumber = (x: number) => x.toFixed(100).replace(/0+$/, "").replace(/\.$/, "");

export const convertToCSV = <T>(data: T[]) => {
  if (data.length === 0) return { out: "", headers: [] };
  let out = "";
  const headers = Object.keys(data[0] as any) as (keyof T)[];
  for (let i = 0; i < data.length; i++) {
    for (let j = 0; j < headers.length; j++) {
      const v = data[i][headers[j]];
      let val = "";
      switch (typeof v) {
        case "number":
          val = fmtNumber(v);
          break;
        case "string":
          val = v;
          break;
        default:
          val = JSON.stringify(v);
      }
      out += headers[j] === "book__" ? `"${val}"` : val;
      if (j < headers.length - 1) out += ",";
    }
    out += "\n";
  }
  return { headers, out };
};

export const readCSV = async <T>(fileName: string) => {
  const file = fs.readFileSync(fileName, "utf-8");
  const [headersRaw, ...rows] = file
    .split("\n")
    .filter((x) => x)
    .map((row) => row.split(",").map((v) => v.trim().replace(/"/g, "")));
  const headers = headersRaw as (keyof T)[];
  const data = rows.map((values) => {
    const vals = values as T[keyof T][];
    return headers.reduce((acc, h, i) => {
      acc[h] = vals[i];
      return acc;
    }, {} as T);
  });
  return data;
};

export const red = (x: string | number) => `\x1b[31m${x}\x1b[0m`;
export const orange = (x: string | number) => `\x1b[33m${x}\x1b[0m`;
export const green = (x: string | number) => `\x1b[32m${x}\x1b[0m`;

type Bases = "WETH" | "PUNKS20" | "PUNKS40";
type Quotes = "WETH" | "USDB";
export const getBase = (key: MarketKeys) => key.split("_")[0] as Bases;
export const getQuote = (key: MarketKeys) => key.split("_")[1] as Quotes;

export const publicClient = createPublicClient({ transport: http("https://rpc.blast.io") });

export const getBlockNumber = async () => publicClient.getBlockNumber().then(Number);

export const existsAsync = async (path: string) => {
  try {
    await fsP.access(path);
    return true;
  } catch (e) {
    return false;
  }
};
