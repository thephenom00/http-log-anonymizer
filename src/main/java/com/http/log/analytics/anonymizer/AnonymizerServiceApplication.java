package com.road.to.cdn.anonymizer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class AnonymizerServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(AnonymizerServiceApplication.class, args);
	}

}
