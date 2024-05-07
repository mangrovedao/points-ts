# points-ts

An implementation of the MS1 Points program in typescript

To Pull data you need:

`bun run src/data/x.ts` where `x` is `mid_prices`, `books`, or `fills`

# Note

Currently the code assumes the book is a slice (block 1 - 10) so you will have to download the book, slice the needed section and then run the calculation
This means the file should be

StartBlock,{}
...,
...,
EndBlock,{}

A better solution is to handle the entire book file in the calc, but the issue is as follows:

Block 1
Block 7
Block 10
Block 15

If we assume our epoch is Block 8-20 we can "duplicate" block 15 safely but we only know when we hit the line with block 10 that we need to duplicate the book before (book 7) for another block

If the calculation can be updated to handle this, we can use a full book for all calculations (fills works already)
