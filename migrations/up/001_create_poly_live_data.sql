CREATE TABLE poly_live_price ( 
	timestamp TIMESTAMP,
	receipt_time TIMESTAMP,
	slug VARCHAR,
	token_id VARCHAR,
	token_name VARCHAR,
	bids DOUBLE[][],
	asks DOUBLE[][]
) timestamp(timestamp) PARTITION BY DAY WAL;

