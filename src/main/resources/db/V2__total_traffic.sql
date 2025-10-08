CREATE TABLE IF NOT EXISTS http_log_total_traffic
(
  date                Date,
  resource_id         UInt64,
  response_status     UInt16,
  cache_status        LowCardinality(String),
  remote_addr         String,
  total_bytes_sent    UInt64,
  total_requests      UInt64
  )
  ENGINE = SummingMergeTree((total_bytes_sent, total_requests))
  PARTITION BY toYYYYMM(date)
  ORDER BY (date, resource_id, response_status, cache_status, remote_addr);
