package com.road.to.cdn.anonymizer.config;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

@Component
@RequiredArgsConstructor
public class DataSourceInitializer {
  private final DataSource dataSource;

  @PostConstruct
  public void init() {
    try (Connection conn = dataSource.getConnection()) {
      System.out.println("✅ Hikari pool initialized and connected to ClickHouse");
    } catch (SQLException e) {
      throw new RuntimeException("❌ Failed to initialize ClickHouse DataSource", e);
    }
  }
}

