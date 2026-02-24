package com.example.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableAsync;

@Slf4j
@EnableKafka
@EnableAsync
@SpringBootApplication
public class KafkaApplication {

	public static void main(String[] args) {
		log.info("Starting Kafka Order Application...");
		SpringApplication.run(KafkaApplication.class, args);
		log.info("Kafka Order Application started successfully!");
	}
}
