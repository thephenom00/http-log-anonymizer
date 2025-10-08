CREATE MATERIALIZED VIEW http_log_materialized_view
    TO http_log_total_traffic
AS
SELECT
  toDate(timestamp) AS date,
  resource_id,
  response_status,
  cache_status,
  remote_addr,
  sum(bytes_sent) AS total_bytes_sent,
  count() AS total_requests
FROM http_log
GROUP BY date, resource_id, response_status, cache_status, remote_addr;
