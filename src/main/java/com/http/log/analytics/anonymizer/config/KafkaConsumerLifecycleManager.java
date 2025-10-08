package com.road.to.cdn.anonymizer.config;

import com.road.to.cdn.anonymizer.service.AnonymizerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class KafkaConsumerLifecycleManager implements SmartLifecycle {
  private final AnonymizerService anonymizerService;
  private boolean isRunning = false;

  @Override
  public void start() {
    isRunning = true;
  }

  @Override
  public void stop() {
    anonymizerService.saveToDatabase();
    isRunning = false;
  }

  @Override
  public boolean isRunning() {
    return this.isRunning;
  }
}
