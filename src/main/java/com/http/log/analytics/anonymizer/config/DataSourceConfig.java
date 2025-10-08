package com.road.to.cdn.anonymizer.config;

import com.clickhouse.jdbc.ClickHouseDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Properties;

@Configuration
public class DataSourceConfig {

  @Bean
  public DataSource clickHouseDataSource() throws SQLException {
    String url = "jdbc:ch://localhost:8124/default";

    Properties info = new Properties();
    info.put("user", "default");
    info.put("password", "");
    info.put("database", "default");

    return new ClickHouseDataSource(url, info);
  }
}

