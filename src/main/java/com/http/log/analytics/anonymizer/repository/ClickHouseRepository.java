package com.road.to.cdn.anonymizer.repository;

import com.road.to.cdn.anonymizer.dto.HttpLogDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
@RequiredArgsConstructor
@Slf4j
public class ClickHouseRepository {
  private final JdbcTemplate jdbcTemplate;

  public void saveAll(List<HttpLogDto> records) {
    try {
      String sql = "INSERT INTO http_log (timestamp, resource_id, bytes_sent, request_time_milli, response_status, cache_status, method, remote_addr, url) VALUES" +
        "(?, ?, ?, ?, ?, ?, ?, ?, ?)";

      jdbcTemplate.batchUpdate(sql, records, records.size(),
        (ps, record) -> {
          ps.setString(1, record.getTimestampEpochMilli());
          ps.setLong(2, record.getResourceId());
          ps.setLong(3, record.getBytesSent());
          ps.setLong(4, record.getRequestTimeMilli());
          ps.setInt(5, record.getResponseStatus());
          ps.setString(6, record.getCacheStatus().toString());
          ps.setString(7, record.getMethod().toString());
          ps.setString(8, record.getRemoteAddr());
          ps.setString(9, record.getUrl().toString());
        }
      );

      log.info("Successfully added {} records to ClickHouse database. \n    /^\\\n   (O_O)\n  --|_|--\n    / \\", records.size());
    } catch (Exception e) {
      log.error(e.toString());
      throw new RuntimeException("Failed to save batch to ClickHouse database", e);
    }
  }

}
