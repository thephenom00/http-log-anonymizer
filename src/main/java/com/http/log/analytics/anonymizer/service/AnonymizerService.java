package com.road.to.cdn.anonymizer.service;

import com.road.to.cdn.anonymizer.capnp.HttpLog;
import com.road.to.cdn.anonymizer.dto.RecordAck;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.capnproto.MessageReader;
import org.capnproto.Serialize;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class AnonymizerService {

  private final ClickHouseService clickHouseService;

  private final Queue<RecordAck> list = new ConcurrentLinkedQueue<>();

  @KafkaListener(topics = "http_log")
  public void listen(byte[] message, Acknowledgment ack) {
    try {
      ByteBuffer buffer = ByteBuffer.wrap(message);
      MessageReader reader = Serialize.read(buffer);

      HttpLog.HttpLogRecord.Reader record = reader.getRoot(HttpLog.HttpLogRecord.factory);

      log.info("""
          \n
          ========= DECODED RECORD =========
          Timestamp (epoch ms): {}
          Resource ID: {}
          Bytes Sent: {}
          Request Time (ms): {}
          Response Status: {}
          Cache Status: {}
          Method: {}
          Remote Addr: {}
          URL: {}
          ==================================
          """,
        record.getTimestampEpochMilli(),
        record.getResourceId(),
        record.getBytesSent(),
        record.getRequestTimeMilli(),
        record.getResponseStatus(),
        record.getCacheStatus().toString(),
        record.getMethod().toString(),
        record.getRemoteAddr().toString(),
        record.getUrl().toString()
      );

      list.add(new RecordAck(record, ack));

    } catch (IOException e) {
      log.error("Failed to decode Cap'n Proto message", e);
    }
  }

  @Scheduled(fixedRate = 65000)
  public void saveToDatabase() {
    List<RecordAck> batch = new ArrayList<>();
    RecordAck record;
    while (!list.isEmpty()) {
      record = list.poll();
      batch.add(record);
    }

    try {
      if (batch.isEmpty()) {
        log.info("Batch was empty, no records were saved to database.");
        return;
      }

      clickHouseService.saveBatchToDatabase(batch.stream().map(RecordAck::httplog).collect(Collectors.toList()));

      batch.stream().forEach(r->r.ack().acknowledge());
    } catch (Exception e) {
      list.addAll(batch);
      log.error("Failed to save batch to ClickHouse database. Returning batch to queue.");
    }
  }
}
