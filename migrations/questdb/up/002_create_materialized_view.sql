CREATE MATERIALIZED VIEW IF NOT EXISTS okx_features_1s 
REFRESH IMMEDIATE 
AS (
  SELECT 
    timestamp,
    symbol,

    -- Get the last arrays for this second
    last(bids)[1][1] as best_bid,
    last(asks)[1][1] as best_ask,
    
    -- Calculate Spread using the LAST state
    (last(asks)[1][1] - last(bids)[1][1]) as spread,
    
    -- Calculate Mid-Price
    (last(asks)[1][1] + last(bids)[1][1]) / 2 as mid_price,
    
    -- Calculate Imbalance
    (array_sum(last(bids)[2][1:6]) - array_sum(last(asks)[2][1:6])) / 
    CASE 
        WHEN (array_sum(last(bids)[2][1:6]) + array_sum(last(asks)[2][1:6])) = 0 THEN NULL 
        ELSE (array_sum(last(bids)[2][1:6]) + array_sum(last(asks)[2][1:6])) 
    END as imbalance
    
  FROM okx_live_price
  SAMPLE BY 1s
) 
PARTITION BY DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS polymarket_features_1s 
REFRESH IMMEDIATE 
AS (
  SELECT 
    timestamp,
    slug,
    token_id,
    token_name,

    -- Get the last arrays for this second
    last(bids)[1][-1] as best_bid,
    last(asks)[1][-1] as best_ask,
    
    -- Calculate Spread using the LAST state
    (last(asks)[1][-1] - last(bids)[1][-1]) as spread,
    
    -- Calculate Mid-Price
    (last(asks)[1][-1] + last(bids)[1][-1]) / 2 as mid_price,
    
    -- Calculate Imbalance
    (array_sum(last(bids)[2][-5:]) - array_sum(last(asks)[2][-5:])) / 
    CASE 
        WHEN (array_sum(last(bids)[2][-5:]) + array_sum(last(asks)[2][-5:])) = 0 THEN NULL 
        ELSE (array_sum(last(bids)[2][-5:]) + array_sum(last(asks)[2][-5:])) 
    END as imbalance
    
  FROM poly_live_price
  SAMPLE BY 1s
) 
PARTITION BY DAY;

