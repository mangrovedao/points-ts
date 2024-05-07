WITH
-- Use a CTE to store variables for later use
vars AS (SELECT decode($1, 'hex') AS base, decode($2, 'hex') AS quote, ($3)::int AS start_block, ($4)::int + 1 AS end_block),
-- Get all the open offers that were open at some block between [start_block, end_block]
open_offers as (
	SELECT DISTINCT
    o.block_range,
    o.offer_id,
	-- Combine Limit Order and Kandel to find the real maker of an offer (since the maker will be the kandel)
    CONCAT('0x', encode(COALESCE(lo.real_taker, kandel.admin, o.maker), 'hex')) as maker,
    o.tick,
	o.gives,
    CASE
  		WHEN m.outbound_tkn = vars.base AND m.inbound_tkn = vars.quote THEN
  			o.gives/POW(10, base_token.decimals)
  	ELSE
  			o.gives/POW(10, quote_token.decimals)
  	END AS gives_display,
  	CASE
  		WHEN m.outbound_tkn = vars.base AND m.inbound_tkn = vars.quote THEN
  			POW(1.0001, o.tick) * POW(10, base_token.decimals - quote_token.decimals)
  	ELSE
  			1 / (POW(1.0001, o.tick) * POW(10, quote_token.decimals - base_token.decimals))
  	END AS price,
    CASE
      WHEN m.outbound_tkn =  vars.base AND m.inbound_tkn = vars.quote THEN o.market
      ELSE reversed_market.id -- reversed market
    END AS market,
    CASE
		WHEN m.outbound_tkn = vars.base AND m.inbound_tkn = vars.quote
			THEN 'ask'
			ELSE 'bid'
    END AS offer_type
  FROM vars CROSS JOIN sgd10.offer as o
  	JOIN sgd10.market_active as m ON o.market = m.id
	  AND
	  ((m.outbound_tkn = vars.base AND m.inbound_tkn = vars.quote)
	  or
	  (m.outbound_tkn = vars.quote AND m.inbound_tkn = vars.base))
  	JOIN sgd10.market_active as reversed_market ON m.outbound_tkn = reversed_market.inbound_tkn and m.inbound_tkn = reversed_market.outbound_tkn
  LEFT JOIN sgd10.limit_order_active as lo ON o.id = lo.offer
  LEFT JOIN sgd10.kandel_active as kandel ON o.kandel = kandel.id
  JOIN sgd10.token AS base_token ON base_token.address = vars.base
  JOIN sgd10.token AS quote_token ON quote_token.address = vars.quote
  WHERE
     o.is_open = true
     AND o.block_range && int4range(vars.start_block, vars.end_block)
),
market_we_need as (
	SELECT
	m.id as id_1, reversed_market.id as id_2
	from
		sgd10.market_active as m
	CROSS JOIN vars 
	JOIN 
		sgd10.market_active as reversed_market
	ON
		m.outbound_tkn = reversed_market.inbound_tkn and m.inbound_tkn = reversed_market.outbound_tkn
	WHERE
	  (m.outbound_tkn = vars.base AND m.inbound_tkn = vars.quote)
	  or
	  (m.outbound_tkn = vars.quote AND m.inbound_tkn = vars.base)
),
bs as (
    SELECT generate_series(vars.start_block, vars.end_block) AS block_number FROM vars
),
blocks as (
  SELECT distinct(block_number) from bs join sgd10.offer o on LOWER(o.block_range) = bs.block_number JOIN market_we_need mwn on o.market = mwn.id_1 OR o.market = mwn.id_2
	union all
  SELECT min(block_number) from bs
),
json_offer as (
	SELECT
	open_offers.block_range,
		json_build_object(
			'maker', open_offers.maker,
			'tick', open_offers.tick,
			'gives', open_offers.gives,
			'gives_display', open_offers.gives_display,
			'price', open_offers.price,
			'offer_type', open_offers.offer_type
		) as open_offers
	from open_offers
),
book_per_block as (
	SELECT
	blocks.block_number,
	json_agg(open_offers) as book
	from json_offer JOIN blocks on json_offer.block_range @> blocks.block_number GROUP by blocks.block_number
)
SELECT * from book_per_block;
