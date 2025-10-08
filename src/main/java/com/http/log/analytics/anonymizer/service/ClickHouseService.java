package com.road.to.cdn.anonymizer.service;

import com.road.to.cdn.anonymizer.capnp.HttpLog;
import com.road.to.cdn.anonymizer.dto.HttpLogDto;
import com.road.to.cdn.anonymizer.repository.ClickHouseRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;


@Service
@RequiredArgsConstructor
@Slf4j
public class ClickHouseService {

  private final ClickHouseRepository clickHouseRepository;

  public void saveBatchToDatabase(List<HttpLog.HttpLogRecord.Reader> records) {
    try {
      clickHouseRepository.saveAll(
        records.stream()
          .map(r -> new HttpLogDto(
            formatTimeStamp(r.getTimestampEpochMilli()),
            r.getResourceId(),
            r.getBytesSent(),
            r.getRequestTimeMilli(),
            r.getResponseStatus(),
            r.hasCacheStatus() ? r.getCacheStatus().toString() : null,
            r.hasMethod() ? r.getMethod().toString() : null,
            r.hasRemoteAddr() ? maskIp(r.getRemoteAddr().toString()) : null,
            r.hasUrl() ? r.getUrl().toString() : null
          ))
          .collect(Collectors.toList())
      );
    } catch (Exception e) {
      throw new RuntimeException("Failed to save batch to ClickHouse database.", e);
    }

  }

  public String maskIp(String ip) {
    String[] ipArray = ip.split("\\.");
    ipArray[ipArray.length - 1] = "X";
    return String.join(".", ipArray);
  }

  public String formatTimeStamp(Long timeStamp) {
    LocalDateTime localDateTime = Instant.ofEpochMilli(timeStamp).atZone(ZoneId.systemDefault()).toLocalDateTime();
    return localDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
  }
}
