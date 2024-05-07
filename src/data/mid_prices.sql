WITH
vars AS (SELECT decode($1, 'hex') AS base, decode($2, 'hex') AS quote, ($3)::int AS start_block, ($4)::int + 1 AS end_block),
market_we_need AS (
	SELECT m.id AS id_1, reversed_market.id AS id_2
	FROM
		sgd10.market_active AS m
	CROSS JOIN vars 
	JOIN 
		sgd10.market_active AS reversed_market
	ON
		m.outbound_tkn = reversed_market.inbound_tkn AND m.inbound_tkn = reversed_market.outbound_tkn
	WHERE
	  (m.outbound_tkn = vars.base AND m.inbound_tkn = vars.quote)
	  OR
	  (m.outbound_tkn = vars.quote AND m.inbound_tkn = vars.base)
),
open_offers AS (
	SELECT
	o.block_range,
  	CASE
  		WHEN m.outbound_tkn = vars.base AND m.inbound_tkn = vars.quote THEN
  			POW(1.0001, o.tick) * POW(10, base_token.decimals - quote_token.decimals)
  	ELSE
  			POW(1.0001, -o.tick) * POW(10, quote_token.decimals - base_token.decimals)
  	END AS price,
    CASE
		WHEN m.outbound_tkn = vars.base AND m.inbound_tkn = vars.quote THEN 'ask' ELSE 'bid' END AS offer_type,
	CASE
      WHEN m.outbound_tkn =  vars.base AND m.inbound_tkn = vars.quote THEN o.market
      ELSE reversed_market.id -- reversed market
    END AS market
  FROM vars CROSS JOIN sgd10.offer AS o
  	JOIN sgd10.market_active AS m ON
		o.market = m.id
	AND
		(
			(m.outbound_tkn = vars.base AND m.inbound_tkn = vars.quote)
	  	OR
	  		(m.outbound_tkn = vars.quote AND m.inbound_tkn = vars.base)
		)
  	JOIN sgd10.market_active AS reversed_market ON m.outbound_tkn = reversed_market.inbound_tkn AND m.inbound_tkn = reversed_market.outbound_tkn
  JOIN sgd10.token AS base_token ON base_token.address = vars.base
  JOIN sgd10.token AS quote_token ON quote_token.address = vars.quote
  WHERE
     o.is_open = true AND o.block_range && int4range(vars.start_block, vars.end_block)
),
bs AS (SELECT generate_series(vars.start_block, vars.end_block) AS block_number FROM vars),
blocks AS (SELECT DISTINCT(block_number) FROM bs JOIN sgd10.offer o ON LOWER(o.block_range) = bs.block_number JOIN market_we_need mwn ON o.market = mwn.id_1 OR o.market = mwn.id_2),
price_per_block AS (
SELECT
	blocks.block_number,
	MAX(CASE WHEN offer_type = 'bid' THEN price::numeric ELSE NULL END) AS highest_bid,
	MIN(CASE WHEN offer_type = 'ask' THEN price::numeric ELSE NULL END) AS lowest_ask
FROM
	open_offers
JOIN
	blocks ON open_offers.block_range @> blocks.block_number GROUP BY blocks.block_number
)

SELECT block_number, (COALESCE(highest_bid, lowest_ask) + COALESCE(lowest_ask, highest_bid)) / 2 AS mid_price FROM price_per_block;
