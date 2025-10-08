package com.road.to.cdn.anonymizer.dto;

import com.road.to.cdn.anonymizer.capnp.HttpLog;
import org.springframework.kafka.support.Acknowledgment;

public record RecordAck(
  HttpLog.HttpLogRecord.Reader httplog,
  Acknowledgment ack
) {
}
