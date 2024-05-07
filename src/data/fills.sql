WITH
vars AS (SELECT decode($1, 'hex') AS base, decode($2, 'hex') AS quote, ($3)::int AS start_block, ($4)::int + 1 AS end_block),
fills AS (
    SELECT
        DISTINCT LOWER(of.block_range) AS block,
        ENCODE(COALESCE(lo.real_taker, kandel.admin, maker), 'hex') AS maker,
        ENCODE(COALESCE(lo_taker.real_taker, of.taker), 'hex') AS taker,
        offer_id,
        o.tick,
        of.maker_got,
        of.maker_gave,
        CASE
            WHEN m.outbound_tkn = vars.base AND m.inbound_tkn = vars.quote
                THEN m.id
                ELSE reversed_market.id -- reversed market
            END AS market,
        CASE
            WHEN m.outbound_tkn = vars.base AND m.inbound_tkn = vars.quote
                THEN maker_gave/POW(10, base_token.decimals)
                ELSE maker_gave/POW(10, quote_token.decimals)
        END AS maker_gave_display,
        CASE
            WHEN m.outbound_tkn = vars.quote
            AND m.inbound_tkn = vars.base THEN maker_got/POW(10, base_token.decimals)
            ELSE maker_got/POW(10, quote_token.decimals)
        END AS maker_got_display,
        CASE
            WHEN m.outbound_tkn = vars.base AND m.inbound_tkn = vars.quote THEN 'ask'
            ELSE 'bid'
        END AS offer_type
    FROM vars
    CROSS JOIN sgd10.offer_filled AS of
    JOIN sgd10.offer o ON of.offer = o.id AND LOWER(of.block_range) = LOWER(o.block_range)
    JOIN sgd10.market_active AS m
        ON
            o.market = m.id
        AND 
            ((m.outbound_tkn = vars.base AND m.inbound_tkn = vars.quote) or (m.outbound_tkn = vars.quote AND m.inbound_tkn = vars.base))
    JOIN sgd10.market_active AS reversed_market ON m.outbound_tkn = reversed_market.inbound_tkn AND m.inbound_tkn = reversed_market.outbound_tkn
    LEFT JOIN sgd10.limit_order_active AS lo ON o.id = lo.offer
    LEFT JOIN sgd10.limit_order_active AS lo_taker ON LOWER(lo_taker.block_range) = LOWER(of.block_range) AND lo_taker.id LIKE CONCAT('%', ENCODE(of.transaction_hash, 'hex'), '%')
    LEFT JOIN sgd10.kandel_active AS kandel ON o.kandel = kandel.id
    JOIN sgd10.token AS base_token ON base_token.address = vars.base
    JOIN sgd10.token AS quote_token ON quote_token.address = vars.quote
    WHERE int4range(vars.start_block, vars.end_block) @> LOWER(of.block_range)
),
bs AS (SELECT generate_series(vars.start_block, vars.end_block) AS block_number FROM vars),
blocks AS (SELECT DISTINCT(block_number) FROM bs JOIN sgd10.offer_filled of ON LOWER(of.block_range) = bs.block_number),
json_fills AS (
    SELECT
        fills.block,
        json_build_object(
            'maker', CONCAT('0x', fills.maker),
            'taker', CONCAT('0x', fills.taker),
            'tick', fills.tick,
            'maker_gave_display', fills.maker_gave_display,
            'maker_got_display', fills.maker_got_display,
            'offer_type', fills.offer_type
        ) AS fills
    FROM fills),
fills_per_block AS (
    SELECT blocks.block_number, json_agg(fills) AS fills_on_block
    FROM json_fills
    JOIN blocks ON json_fills.block = blocks.block_number
    GROUP by blocks.block_number
)
SELECT * FROM fills_per_block;
