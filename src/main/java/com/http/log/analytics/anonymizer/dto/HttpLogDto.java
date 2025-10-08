package com.road.to.cdn.anonymizer.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class HttpLogDto {
  private String timestampEpochMilli;
  private long resourceId;
  private long bytesSent;
  private long requestTimeMilli;
  private short responseStatus;
  private String cacheStatus;
  private String method;
  private String remoteAddr;
  private String url;
}
