# Kafka Schema Registry with JSON Schema (Spring Boot Production Guide)

## Table of Contents
1. [JSON Schema Overview](#1-json-schema-overview)
2. [How JSON Schema Works with Schema Registry](#2-how-json-schema-works-with-schema-registry)
3. [Spring Boot Setup](#3-spring-boot-setup)
4. [Production-Grade Producer Configuration](#4-production-grade-producer-configuration)
5. [Production-Grade Consumer Configuration](#5-production-grade-consumer-configuration)
6. [Backward Compatibility Setup](#6-backward-compatibility-setup)
7. [Event Model Design](#7-event-model-design)
8. [Complete Working Examples](#8-complete-working-examples)
9. [Best Practices](#9-best-practices)
10. [Troubleshooting](#10-troubleshooting)

---

## 1. JSON Schema Overview

### 1.1 What is JSON Schema?

JSON Schema is a vocabulary to **annotate and validate JSON documents**.[cite:196] Unlike Avro (which requires code generation), JSON Schema works directly with POJOs using Jackson annotations.[cite:192][cite:196]

**Why JSON Schema vs Avro?**
- No code generation needed (uses existing POJOs).
- Familiar Jackson annotations (`@JsonProperty`, `@JsonFormat`, etc.).
- Easy integration with Spring Boot REST APIs.
- Less build complexity.

**Trade-off:**
- Larger message sizes than Avro (JSON is text-based).
- Slightly slower serialization than binary formats.

### 1.2 JSON Schema Format Example

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "OrderCreatedEvent",
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "orderId": {
      "type": "string"
    },
    "customerId": {
      "type": "string"
    },
    "amount": {
      "type": "number"
    },
    "createdAt": {
      "type": "string",
      "format": "date-time"
    }
  },
  "required": ["orderId", "customerId", "amount", "createdAt"]
}
```

This schema is **automatically derived** from your Java POJO by `KafkaJsonSchemaSerializer`.[cite:192][cite:196]

---

## 2. How JSON Schema Works with Schema Registry

### 2.1 Wire Format

Same as Avro:[cite:196]

| Bytes | Field | Description |
|-------|-------|-------------|
| 0 | Magic byte | Always `0` |
| 1-4 | Schema ID | 4-byte integer from Schema Registry |
| 5… | JSON payload | JSON-encoded message |

### 2.2 Producer Flow with Auto-Registration

1. Producer serializes POJO to JSON.
2. `KafkaJsonSchemaSerializer` derives JSON Schema from POJO (using Jackson annotations).
3. Serializer checks if schema exists in Schema Registry under subject `<topic>-value`.
4. If not exists or new version:
   - Registers schema with Schema Registry.
   - Registry validates **backward compatibility** (if configured).
   - Returns schema ID.[cite:196][cite:201]
5. Serializer writes: `magicByte + schemaId + jsonPayload`.
6. Message sent to Kafka.

### 2.3 Consumer Flow

1. Consumer receives bytes.
2. `KafkaJsonSchemaDeserializer` reads schema ID.
3. Fetches schema from Schema Registry (cached after first fetch).
4. Deserializes JSON → POJO using schema.
5. Application receives typed object.[cite:196]

### 2.4 Backward Compatibility (Default Mode)

**BACKWARD** mode ensures:[cite:195][cite:201]
- New consumers can read old messages.
- You can add **optional fields with defaults**.
- You **cannot** remove required fields or change types.

**Upgrade order:** Consumers first, then producers.[cite:201]

---

## 3. Spring Boot Setup

### 3.1 Maven Dependencies (`pom.xml`)

```xml
<properties>
    <java.version>21</java.version>
    <spring-boot.version>3.2.1</spring-boot.version>
    <confluent.version>7.6.0</confluent.version>
</properties>

<dependencies>
    <!-- Spring Boot Kafka -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter</artifactId>
    </dependency>
    
    <dependency>
        <groupId>org.springframework.kafka</groupId>
        <artifactId>spring-kafka</artifactId>
    </dependency>

    <!-- Confluent JSON Schema Serializer -->
    <dependency>
        <groupId>io.confluent</groupId>
        <artifactId>kafka-json-schema-serializer</artifactId>
        <version>${confluent.version}</version>
    </dependency>

    <!-- Schema Registry Client -->
    <dependency>
        <groupId>io.confluent</groupId>
        <artifactId>kafka-schema-registry-client</artifactId>
        <version>${confluent.version}</version>
    </dependency>

    <!-- Jackson for date/time handling -->
    <dependency>
        <groupId>com.fasterxml.jackson.datatype</groupId>
        <artifactId>jackson-datatype-jsr310</artifactId>
    </dependency>

    <!-- Lombok (optional, for cleaner code) -->
    <dependency>
        <groupId>org.projectlombok</groupId>
        <artifactId>lombok</artifactId>
        <optional>true</optional>
    </dependency>
</dependencies>

<repositories>
    <!-- Confluent Maven repository -->
    <repository>
        <id>confluent</id>
        <url>https://packages.confluent.io/maven/</url>
    </repository>
</repositories>
```

### 3.2 Gradle Dependencies (`build.gradle`)

```gradle
plugins {
    id 'org.springframework.boot' version '3.2.1'
    id 'io.spring.dependency-management' version '1.1.4'
    id 'java'
}

ext {
    confluentVersion = '7.6.0'
}

repositories {
    mavenCentral()
    maven {
        url "https://packages.confluent.io/maven/"
    }
}

dependencies {
    implementation 'org.springframework.boot:spring-boot-starter'
    implementation 'org.springframework.kafka:spring-kafka'
    
    // Confluent JSON Schema
    implementation "io.confluent:kafka-json-schema-serializer:${confluentVersion}"
    implementation "io.confluent:kafka-schema-registry-client:${confluentVersion}"
    
    // Jackson
    implementation 'com.fasterxml.jackson.datatype:jackson-datatype-jsr310'
    
    compileOnly 'org.projectlombok:lombok'
    annotationProcessor 'org.projectlombok:lombok'
}
```

---

## 4. Production-Grade Producer Configuration

### 4.1 application.yml (Producer Side)

```yaml
spring:
  application:
    name: order-producer-service
  
  kafka:
    bootstrap-servers: localhost:9092
    
    producer:
      # JSON Schema Serializer
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer
      
      # Production settings
      acks: all                          # Wait for all replicas
      retries: 3                         # Retry on transient failures
      compression-type: snappy           # Compress messages
      batch-size: 32768                  # 32 KB batch
      linger-ms: 10                      # Wait 10ms for batching
      buffer-memory: 67108864            # 64 MB buffer
      
      # Transaction support (optional, for exactly-once)
      transaction-id-prefix: order-producer-
      
      properties:
        # Schema Registry URL
        schema.registry.url: http://localhost:8081
        
        # Auto-register schemas (set false in strict environments)
        auto.register.schemas: true
        
        # Use latest schema version (recommended for producers)
        use.latest.version: false
        
        # Fail on invalid schema
        json.fail.invalid.schema: true
        
        # Write dates as ISO-8601 strings
        json.write.dates.iso8601: true
        
        # Don't use oneOf for nullables (simpler schemas)
        json.oneof.for.nullables: false
        
        # Idempotence (exactly-once)
        enable.idempotence: true
        
        # Max in-flight requests
        max.in.flight.requests.per.connection: 5

# Topic configuration
app:
  kafka:
    topics:
      order-created: order.created.v1
```

### 4.2 Producer Configuration Class

```java
package com.example.producer.config;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.producer.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        
        // Kafka broker
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        
        // Serializers
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class);
        
        // Schema Registry
        config.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        config.put(KafkaJsonSchemaSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
        config.put(KafkaJsonSchemaSerializerConfig.USE_LATEST_VERSION, false);
        
        // JSON-specific settings
        config.put(KafkaJsonSchemaSerializerConfig.FAIL_INVALID_SCHEMA, true);
        config.put(KafkaJsonSchemaSerializerConfig.WRITE_DATES_AS_ISO8601, true);
        config.put(KafkaJsonSchemaSerializerConfig.ONEOF_FOR_NULLABLES, false);
        
        // Production settings
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.RETRIES_CONFIG, 3);
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
        config.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);
        
        // Idempotence (exactly-once)
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
```

### 4.3 Event Model (POJO with Jackson Annotations)

```java
package com.example.producer.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderCreatedEvent {

    /**
     * Required field - order unique identifier
     */
    @JsonProperty(required = true)
    private String orderId;

    /**
     * Required field - customer identifier
     */
    @JsonProperty(required = true)
    private String customerId;

    /**
     * Required field - order total amount
     */
    @JsonProperty(required = true)
    private BigDecimal amount;

    /**
     * Required field - order creation timestamp
     */
    @JsonProperty(required = true)
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    private OffsetDateTime createdAt;

    /**
     * Optional field - currency code (default: USD)
     * Adding this field later is backward compatible
     */
    @JsonProperty(required = false, defaultValue = "USD")
    private String currency;

    /**
     * Optional field - order items
     */
    @JsonProperty(required = false)
    private List<OrderItem> items;

    /**
     * Optional field - customer notes
     */
    @JsonProperty(required = false)
    private String notes;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderItem {
        
        @JsonProperty(required = true)
        private String productId;
        
        @JsonProperty(required = true)
        private String productName;
        
        @JsonProperty(required = true)
        private Integer quantity;
        
        @JsonProperty(required = true)
        private BigDecimal price;
    }
}
```

**Key annotations:**[cite:192][cite:196]
- `@JsonProperty(required = true)` → Field becomes **required** in JSON Schema.
- `@JsonProperty(required = false)` → Field becomes **optional** (backward compatible addition).
- `@JsonFormat` → Controls date/time serialization format.
- `defaultValue` → Provides default for optional fields.

### 4.4 Producer Service

```java
package com.example.producer.service;

import com.example.producer.event.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.kafka.topics.order-created}")
    private String orderCreatedTopic;

    /**
     * Publish order created event with async callback
     */
    public void publishOrderCreatedEvent(OrderCreatedEvent event) {
        String key = event.getOrderId();
        
        log.info("Publishing OrderCreatedEvent: orderId={}, customerId={}, amount={}", 
                 event.getOrderId(), event.getCustomerId(), event.getAmount());

        CompletableFuture<SendResult<String, Object>> future = 
            kafkaTemplate.send(orderCreatedTopic, key, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Successfully published event: orderId={}, partition={}, offset={}", 
                         event.getOrderId(),
                         result.getRecordMetadata().partition(),
                         result.getRecordMetadata().offset());
            } else {
                log.error("Failed to publish event: orderId={}, error={}", 
                          event.getOrderId(), ex.getMessage(), ex);
                // Handle failure (retry, dead-letter queue, alert, etc.)
            }
        });
    }

    /**
     * Synchronous send (blocks until acknowledgment)
     * Use only when you need guaranteed delivery before proceeding
     */
    public void publishOrderCreatedEventSync(OrderCreatedEvent event) throws Exception {
        String key = event.getOrderId();
        
        SendResult<String, Object> result = 
            kafkaTemplate.send(orderCreatedTopic, key, event).get();
        
        log.info("Synchronously published event: orderId={}, partition={}, offset={}",
                 event.getOrderId(),
                 result.getRecordMetadata().partition(),
                 result.getRecordMetadata().offset());
    }
}
```

### 4.5 Producer REST Controller (Example Usage)

```java
package com.example.producer.controller;

import com.example.producer.event.OrderCreatedEvent;
import com.example.producer.service.OrderProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderProducerService orderProducerService;

    @PostMapping
    public ResponseEntity<String> createOrder(@RequestBody CreateOrderRequest request) {
        String orderId = UUID.randomUUID().toString();
        
        OrderCreatedEvent event = OrderCreatedEvent.builder()
                .orderId(orderId)
                .customerId(request.getCustomerId())
                .amount(request.getAmount())
                .createdAt(OffsetDateTime.now())
                .currency(request.getCurrency() != null ? request.getCurrency() : "USD")
                .notes(request.getNotes())
                .build();

        orderProducerService.publishOrderCreatedEvent(event);
        
        return ResponseEntity.ok(orderId);
    }
}
```

---

## 5. Production-Grade Consumer Configuration

### 5.1 application.yml (Consumer Side)

```yaml
spring:
  application:
    name: order-consumer-service
  
  kafka:
    bootstrap-servers: localhost:9092
    
    consumer:
      # JSON Schema Deserializer
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer
      
      # Consumer group
      group-id: order-consumer-group
      
      # Start from earliest if no offset
      auto-offset-reset: earliest
      
      # Manual commit (recommended for production)
      enable-auto-commit: false
      
      # Fetch settings
      max-poll-records: 500
      fetch-min-size: 1024
      fetch-max-wait-ms: 500
      
      properties:
        # Schema Registry
        schema.registry.url: http://localhost:8081
        
        # Use specific class for deserialization
        json.value.type: com.example.consumer.event.OrderCreatedEvent
        
        # Fail on invalid schema
        json.fail.invalid.schema: true
        
        # Fail on unknown properties (strict validation)
        json.fail.unknown.properties: false
        
        # Session timeout
        session.timeout.ms: 30000
        
        # Heartbeat interval
        heartbeat.interval.ms: 10000
        
        # Max poll interval (time between polls)
        max.poll.interval.ms: 300000

    listener:
      # Manual acknowledgment mode
      ack-mode: manual
      
      # Concurrency (number of consumer threads)
      concurrency: 3
      
      # Error handling
      missing-topics-fatal: false

# Topic configuration
app:
  kafka:
    topics:
      order-created: order.created.v1
```

### 5.2 Consumer Configuration Class

```java
package com.example.consumer.config;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${spring.kafka.consumer.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        
        // Kafka broker
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        
        // Deserializers with error handling wrapper
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaJsonSchemaDeserializer.class);
        
        // Schema Registry
        config.put(KafkaJsonSchemaDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        config.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, 
                   "com.example.consumer.event.OrderCreatedEvent");
        config.put(KafkaJsonSchemaDeserializerConfig.FAIL_INVALID_SCHEMA, true);
        config.put(KafkaJsonSchemaDeserializerConfig.FAIL_UNKNOWN_PROPERTIES, false);
        
        // Consumer settings
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        config.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024);
        config.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        
        factory.setConsumerFactory(consumerFactory());
        
        // Manual acknowledgment
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        
        // Concurrency (number of consumer threads)
        factory.setConcurrency(3);
        
        // Error handling (log and continue)
        factory.setCommonErrorHandler(new org.springframework.kafka.listener.DefaultErrorHandler());
        
        return factory;
    }
}
```

### 5.3 Event Model (Consumer Side - Same as Producer)

```java
package com.example.consumer.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderCreatedEvent {

    @JsonProperty(required = true)
    private String orderId;

    @JsonProperty(required = true)
    private String customerId;

    @JsonProperty(required = true)
    private BigDecimal amount;

    @JsonProperty(required = true)
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    private OffsetDateTime createdAt;

    // Optional fields (backward compatible)
    @JsonProperty(required = false, defaultValue = "USD")
    private String currency;

    @JsonProperty(required = false)
    private List<OrderItem> items;

    @JsonProperty(required = false)
    private String notes;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderItem {
        @JsonProperty(required = true)
        private String productId;
        
        @JsonProperty(required = true)
        private String productName;
        
        @JsonProperty(required = true)
        private Integer quantity;
        
        @JsonProperty(required = true)
        private BigDecimal price;
    }
}
```

### 5.4 Consumer Listener

```java
package com.example.consumer.listener;

import com.example.consumer.event.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventListener {

    /**
     * Consume OrderCreatedEvent with manual acknowledgment
     */
    @KafkaListener(
            topics = "${app.kafka.topics.order-created}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeOrderCreatedEvent(
            @Payload OrderCreatedEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            Acknowledgment acknowledgment) {
        
        try {
            log.info("Received OrderCreatedEvent: orderId={}, customerId={}, amount={}, partition={}, offset={}",
                     event.getOrderId(), event.getCustomerId(), event.getAmount(), partition, offset);

            // Process the event (business logic)
            processOrder(event);

            // Manual commit (only after successful processing)
            acknowledgment.acknowledge();
            
            log.info("Successfully processed and acknowledged: orderId={}", event.getOrderId());
            
        } catch (Exception ex) {
            log.error("Failed to process event: orderId={}, error={}", 
                      event.getOrderId(), ex.getMessage(), ex);
            
            // Don't acknowledge - message will be retried or sent to DLQ
            // Depending on your error handler configuration
        }
    }

    private void processOrder(OrderCreatedEvent event) {
        // Business logic here
        // - Save to database
        // - Call external services
        // - Trigger workflows
        // - etc.
        
        log.debug("Processing order: orderId={}, amount={}, currency={}",
                  event.getOrderId(), event.getAmount(), 
                  event.getCurrency() != null ? event.getCurrency() : "USD");
    }
}
```

---

## 6. Backward Compatibility Setup

### 6.1 Configure Compatibility Mode Globally

```bash
# Set global compatibility to BACKWARD
curl -X PUT http://localhost:8081/config \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "BACKWARD"}'
```

### 6.2 Configure Compatibility for Specific Subject

```bash
# Set compatibility for orders-value subject
curl -X PUT http://localhost:8081/config/order.created.v1-value \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "BACKWARD"}'
```

### 6.3 Verify Compatibility Mode

```bash
# Check global compatibility
curl http://localhost:8081/config

# Check subject-specific compatibility
curl http://localhost:8081/config/order.created.v1-value
```

### 6.4 Producer Configuration for Backward Compatibility

```yaml
spring:
  kafka:
    producer:
      properties:
        # Auto-register schemas (producer creates schema)
        auto.register.schemas: true
        
        # Don't use latest version (register new version if changed)
        use.latest.version: false
        
        # Schema Registry will validate backward compatibility
        # and reject if incompatible
```

This ensures:[cite:195][cite:201]
- Producer auto-registers new schema versions.
- Schema Registry validates backward compatibility before accepting.
- If schema is incompatible, producer gets HTTP 409 error and message fails.

---

## 7. Event Model Design

### 7.1 Backward-Compatible Changes

✅ **Allowed:**[cite:195][cite:201]
- Add optional field with default value.
- Remove optional field (but data may still exist in old messages).

```java
// Version 1
public class OrderCreatedEvent {
    @JsonProperty(required = true)
    private String orderId;
    
    @JsonProperty(required = true)
    private BigDecimal amount;
}

// Version 2 (backward compatible)
public class OrderCreatedEvent {
    @JsonProperty(required = true)
    private String orderId;
    
    @JsonProperty(required = true)
    private BigDecimal amount;
    
    // ✅ Added optional field with default
    @JsonProperty(required = false, defaultValue = "USD")
    private String currency;
}
```

❌ **Not allowed:**[cite:195]
- Remove required field.
- Change field type.
- Rename field.
- Add required field without default.

### 7.2 Jackson Annotations for Schema Control

```java
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentEvent {

    // Required field
    @JsonProperty(required = true)
    private String paymentId;

    // Optional with default
    @JsonProperty(required = false, defaultValue = "PENDING")
    private String status;

    // Date/time formatting
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    private OffsetDateTime createdAt;

    // Enum as string
    @JsonProperty(required = true)
    private PaymentMethod method;

    // BigDecimal (for money)
    @JsonProperty(required = true)
    private BigDecimal amount;

    // Nested object
    @JsonProperty(required = false)
    private Address billingAddress;
}
```

---

## 8. Complete Working Examples

### 8.1 Producer Main Application

```java
package com.example.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class ProducerApplication {
    public static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
    }
}
```

### 8.2 Consumer Main Application

```java
package com.example.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class ConsumerApplication {
    public static void main(String[] args) {
        SpringApplication.run(ConsumerApplication.class, args);
    }
}
```

### 8.3 Testing Producer

```bash
# Send POST request to create order
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-123",
    "amount": 99.99,
    "currency": "USD",
    "notes": "Urgent delivery"
  }'
```

### 8.4 Verify Schema Registration

```bash
# List all subjects
curl http://localhost:8081/subjects

# Get latest schema for orders-value
curl http://localhost:8081/subjects/order.created.v1-value/versions/latest

# Get all versions
curl http://localhost:8081/subjects/order.created.v1-value/versions
```

---

## 9. Best Practices

### 9.1 Schema Design

1. **Use `@JsonProperty(required = true)` for mandatory fields**.[cite:192][cite:196]
2. **Always provide defaults for optional fields** (`defaultValue = "USD"`).
3. **Use ISO-8601 for dates** (`@JsonFormat` with pattern).
4. **Use BigDecimal for money** (not Double or Float).
5. **Document your schemas** (Javadoc on fields).
6. **Version your topics** (`order.created.v1`, `order.created.v2`).

### 9.2 Producer Settings

1. **`acks=all`** for durability (wait for all replicas).
2. **`enable.idempotence=true`** for exactly-once semantics.
3. **`auto.register.schemas=true`** to auto-create schemas.
4. **`use.latest.version=false`** to allow evolution.
5. **Async sends with callbacks** (don't block on send).

### 9.3 Consumer Settings

1. **Manual acknowledgment** (`ack-mode: manual`) for control.
2. **Error handling** (use `ErrorHandlingDeserializer`).
3. **Proper concurrency** (match partition count).
4. **Monitor lag** (use Control Center or JMX).
5. **Idempotent processing** (handle duplicate messages).

### 9.4 Schema Evolution

1. **Always test compatibility** before deploying.
2. **Add optional fields only** when evolving schemas.
3. **Never remove required fields**.
4. **Deploy consumers first** in BACKWARD mode.[cite:201]
5. **Use separate topics** for major breaking changes.

### 9.5 Production Checklist

- [ ] Schema Registry running and healthy
- [ ] Compatibility mode configured (BACKWARD recommended)
- [ ] Producer has `acks=all` and idempotence
- [ ] Consumer has manual acknowledgment
- [ ] Error handling configured (dead-letter queue)
- [ ] Monitoring and alerting set up
- [ ] Schemas versioned and documented
- [ ] Integration tests with Schema Registry

---

## 10. Troubleshooting

### 10.1 Schema Registration Fails (HTTP 409)

**Error:**
```
Schema being registered is incompatible with an earlier schema
```

**Cause:** New schema violates backward compatibility.[cite:195][cite:201]

**Fix:**
- Check what changed (removed required field? changed type?).
- Add field as optional with default instead of required.
- Or use `compatibility=NONE` (not recommended for production).

### 10.2 Consumer Deserialization Fails

**Error:**
```
Failed to deserialize value for partition X at offset Y
```

**Cause:**
- Schema ID not found in Schema Registry.
- Wrong `json.value.type` configured.
- Message not in Confluent wire format.

**Fix:**
- Check Schema Registry is running.
- Verify `schema.registry.url` is correct.
- Check `json.value.type` matches your POJO class name.
- Use `ErrorHandlingDeserializer` to log errors and continue.

### 10.3 Unknown Properties Error

**Error:**
```
Unknown property 'newField' found in JSON
```

**Cause:** Consumer has old schema, producer added new field.[cite:195]

**Fix:**
- Set `json.fail.unknown.properties=false` in consumer config.
- Update consumer to latest schema.

### 10.4 Schema Not Auto-Registered

**Cause:**
- `auto.register.schemas=false` in producer config.
- Schema Registry not reachable.

**Fix:**
- Set `auto.register.schemas=true`.
- Check Schema Registry URL and connectivity.
- Check Schema Registry logs for errors.

### 10.5 Check Schema Registry Health

```bash
# Health check
curl http://localhost:8081/

# List subjects
curl http://localhost:8081/subjects

# Get config
curl http://localhost:8081/config
```

---

## Summary

**Key points:**[cite:192][cite:195][cite:196][cite:201]

1. **JSON Schema** works directly with POJOs using Jackson annotations.
2. **Producer** auto-registers schemas with `auto.register.schemas=true`.
3. **Schema Registry** validates backward compatibility before accepting new versions.
4. **Consumer** fetches schema by ID and deserializes safely.
5. **Backward compatibility** allows adding optional fields, not removing required ones.
6. **Use production settings**: `acks=all`, idempotence, manual acknowledgment.
7. **Monitor**: Schema Registry, consumer lag, deserialization errors.

This setup provides production-grade event-driven architecture with strong schema contracts and safe evolution.
