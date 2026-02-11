CREATE TABLE IF NOT EXISTS poly_live_price ( 
	timestamp TIMESTAMP,
	receipt_time TIMESTAMP,
	slug VARCHAR,
	token_id VARCHAR,
	token_name VARCHAR,
	bids DOUBLE[][],
	asks DOUBLE[][]
) timestamp(timestamp) PARTITION BY DAY WAL;

CREATE TABLE IF NOT EXISTS exchanges_live_price ( 
	timestamp TIMESTAMP,
	receipt_time TIMESTAMP,
	symbol SYMBOL,
	bids DOUBLE[][],
	asks DOUBLE[][]
) timestamp(timestamp) PARTITION BY DAY WAL;