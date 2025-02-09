DECLARE max_block_timestamp_day TIMESTAMP DEFAULT (
    SELECT COALESCE(MAX(PARSE_TIMESTAMP('%Y%m%d', partition_id)), '2021-01-01 00:00:00')
    FROM `ingestion.INFORMATION_SCHEMA.PARTITIONS`
    WHERE table_name = 'bitcoin_transactions' AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')
);
DECLARE max_block_timestamp TIMESTAMP DEFAULT (
  SELECT COALESCE(MAX(block_timestamp), '2021-01-01 00:00:00')
  FROM ingestion.bitcoin_transactions
  WHERE DATE(block_timestamp) = DATE(max_block_timestamp_day)
);
MERGE ingestion.bitcoin_transactions as T 
USING (
    SELECT `hash`, size, block_hash, block_number, block_timestamp
    FROM bigquery-public-data.crypto_bitcoin.transactions
    WHERE block_timestamp_month >= DATE(TIMESTAMP_TRUNC(max_block_timestamp, month))
    AND block_timestamp >= max_block_timestamp
) as S
ON FALSE
WHEN NOT MATCHED BY SOURCE AND T.block_timestamp >= max_block_timestamp THEN DELETE
WHEN NOT MATCHED BY TARGET THEN
    INSERT (`hash`, size, block_hash, block_number, block_timestamp)
    VALUES (`hash`, size, block_hash, block_number, block_timestamp)