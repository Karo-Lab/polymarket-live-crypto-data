CREATE TABLE IF NOT EXISTS poly_live_tbt (
    timestamp TIMESTAMP,
	receipt_time TIMESTAMP,
	slug VARCHAR,
	token_id VARCHAR,
	token_name VARCHAR,
	side VARCHAR,
	price DOUBLE,
	size DOUBLE,
	best_bid DOUBLE,
	best_ask DOUBLE
) timestamp(timestamp) PARTITION BY HOUR TTL 3 days WAL;

CREATE TABLE IF NOT EXISTS exchanges_live_tbt (
    timestamp TIMESTAMP,
	receipt_time TIMESTAMP,
	exchange VARCHAR,
	symbol SYMBOL,
	side VARCHAR,
	price DOUBLE,
	size DOUBLE
) timestamp(timestamp) PARTITION BY HOUR TTL 3 days WAL; 
