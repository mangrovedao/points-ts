# points-ts

An implementation of the MS1 Points program in typescript

To Pull data you need:

`bun run src/data/x.ts` where `x` is `mid_prices`, `books`, or `fills`

To compute all volumes you can run

`bun run src/volumes.ts` and it will produce files in `DATA_DIR/volume/{maker,taker}/{MARKET_1,MARKET_2}/start-end.csv`

To compute depths you can run

`bun run src/depths.ts` to produce depths for each epoch

# Note

Currently the code has an issue with using large files and requires a lot of RAM. We can optimise this but we can also use a smaller book to make this work.
