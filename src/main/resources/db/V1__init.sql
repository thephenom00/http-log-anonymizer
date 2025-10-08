CREATE TABLE IF NOT EXISTS http_log
(
  timestamp           DateTime,
  resource_id         UInt64,
  bytes_sent          UInt64,
  request_time_milli  UInt64,
  response_status     UInt16,
  cache_status        LowCardinality(String),
  method              LowCardinality(String),
  remote_addr         String,
  url                 String
)
  ENGINE = MergeTree()
  ORDER BY (timestamp);

