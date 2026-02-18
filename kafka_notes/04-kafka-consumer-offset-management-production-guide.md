# Production-Grade Kafka Consumers in Spring Boot: Complete Guide

## Table of Contents
1. [Consumer Fundamentals](#1-consumer-fundamentals)
2. [Offset Management Deep Dive](#2-offset-management-deep-dive)
3. [Consumer Configuration Patterns](#3-consumer-configuration-patterns)
4. [Manual Offset Management](#4-manual-offset-management)
5. [Multiple Consumers in Consumer Group](#5-multiple-consumers-in-consumer-group)
6. [Rebalance Listeners](#6-rebalance-listeners)
7. [Advanced Consumer Patterns](#7-advanced-consumer-patterns)
8. [Production Best Practices](#8-production-best-practices)
9. [Error Handling and Retry Strategies](#9-error-handling-and-retry-strategies)
10. [Performance Tuning](#10-performance-tuning)

---

## 1. Consumer Fundamentals

### 1.1 How Kafka Consumer Groups Work

When multiple consumers share the same `group.id`, Kafka forms a **consumer group**:[cite:172][cite:209]

- Each partition is assigned to **exactly one consumer** in the group.
- Kafka's **Group Coordinator** manages partition assignment.
- When consumers join/leave, a **rebalance** occurs to redistribute partitions.[cite:210][cite:217]

**Example:** Topic with 6 partitions, 3 consumers in group:
- Consumer 1 → Partitions 0, 1
- Consumer 2 → Partitions 2, 3
- Consumer 3 → Partitions 4, 5

### 1.2 Offset Management in Consumer Groups

Each consumer tracks its **offset per partition** independently:[cite:30][cite:209]

- Offsets stored in Kafka's internal topic `__consumer_offsets`.
- Each consumer commits its own offsets for its assigned partitions.
- When rebalance occurs, new consumer for a partition starts from last committed offset.[cite:209][cite:214]

**Key point:** Multiple consumers in the same group **do not** share offsets for the same partition—each partition has **one owner** at a time.[cite:211]

---

## 2. Offset Management Deep Dive

### 2.1 Automatic vs Manual Commit

**Automatic Commit (Default):**[cite:30][cite:215]

```yaml
spring:
  kafka:
    consumer:
      enable-auto-commit: true
      auto-commit-interval: 5000  # Commit every 5 seconds
```

- Commits offsets automatically at fixed intervals.
- **Risk:** If consumer crashes between commits, duplicate processing occurs.
- **Use case:** Non-critical data, idempotent processing.

**Manual Commit:**[cite:30][cite:212]

```yaml
spring:
  kafka:
    consumer:
      enable-auto-commit: false
```

- You control exactly when to commit.
- **Benefit:** Commit only after successful processing.
- **Use case:** Production-grade systems, exactly-once semantics.

### 2.2 Commit Strategies

**Synchronous Commit:**[cite:212][cite:215]

```java
consumer.commitSync();  // Blocks until commit succeeds
```

- **Pros:** Guarantees commit before proceeding.
- **Cons:** Reduces throughput (blocking).

**Asynchronous Commit:**[cite:212][cite:215]

```java
consumer.commitAsync();  // Non-blocking
```

- **Pros:** Better throughput, doesn't block.
- **Cons:** No guarantee commit succeeded.

**Commit Specific Offsets:**[cite:212][cite:215]

```java
Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
offsets.put(
    new TopicPartition("orders", 0),
    new OffsetAndMetadata(offset + 1)  // Next offset to consume
);
consumer.commitSync(offsets);
```

---

## 3. Consumer Configuration Patterns

### 3.1 Basic Consumer Configuration (application.yml)

```yaml
spring:
  application:
    name: order-consumer-service
  
  kafka:
    bootstrap-servers: localhost:9092
    
    consumer:
      # Deserializers
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      
      # Consumer group
      group-id: order-consumer-group
      
      # Offset management
      enable-auto-commit: false
      auto-offset-reset: earliest  # earliest | latest | none
      
      # Fetch configuration
      max-poll-records: 500            # Max records per poll
      fetch-min-size: 1024             # Min bytes to fetch (1 KB)
      fetch-max-wait-ms: 500           # Max wait for min bytes
      
      # Session and heartbeat
      heartbeat-interval-ms: 3000      # Heartbeat frequency
      session-timeout-ms: 30000        # Max time between heartbeats
      max-poll-interval-ms: 300000     # Max time between polls (5 min)
      
      # Partition assignment strategy
      properties:
        partition.assignment.strategy: org.apache.kafka.clients.consumer.CooperativeStickyAssignor
        
    listener:
      # Acknowledgment mode for manual commit
      ack-mode: manual  # manual | batch | record | time | count
      
      # Concurrency (consumer threads per container)
      concurrency: 3
      
      # Error handling
      missing-topics-fatal: false
      
      # Poll timeout
      poll-timeout: 3000

# Topic configuration
app:
  kafka:
    topics:
      order-events: order.events.v1
```

### 3.2 Consumer Configuration Class

```java
package com.example.consumer.config;

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

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        
        // Broker configuration
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        
        // Deserializers with error handling
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, StringDeserializer.class);
        
        // Offset management
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // Fetch configuration
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        config.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024);
        config.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        
        // Session and heartbeat
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        
        // Partition assignment strategy (CooperativeSticky for minimal rebalance disruption)
        config.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                   "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
        
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        
        factory.setConsumerFactory(consumerFactory());
        
        // Manual acknowledgment
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        
        // Concurrency (creates multiple consumer threads)
        factory.setConcurrency(3);
        
        // Poll timeout
        factory.getContainerProperties().setPollTimeout(3000);
        
        return factory;
    }
}
```

---

## 4. Manual Offset Management

### 4.1 Pattern 1: Acknowledge After Processing (Recommended)

```java
package com.example.consumer.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class OrderEventListener {

    /**
     * Pattern 1: Acknowledge after successful processing
     * Most common pattern for production
     */
    @KafkaListener(
        topics = "${app.kafka.topics.order-events}",
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void listen(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            Acknowledgment acknowledgment) {
        
        try {
            log.info("Received message: key={}, partition={}, offset={}, message={}", 
                     key, partition, offset, message);
            
            // Business logic here
            processMessage(message);
            
            // Commit offset ONLY after successful processing
            acknowledgment.acknowledge();
            
            log.info("Successfully processed and committed: offset={}", offset);
            
        } catch (Exception ex) {
            log.error("Failed to process message: offset={}, error={}", 
                      offset, ex.getMessage(), ex);
            
            // DO NOT acknowledge - offset won't be committed
            // Message will be reprocessed after rebalance or restart
            // Optionally: send to DLQ, retry logic, etc.
        }
    }
    
    private void processMessage(String message) {
        // Your business logic
    }
}
```

**Key points:**[cite:205][cite:206]
- Only call `acknowledgment.acknowledge()` after successful processing.
- If exception thrown, offset NOT committed → message reprocessed.
- Ensures **at-least-once** semantics.

### 4.2 Pattern 2: Batch Acknowledgment

```java
@Slf4j
@Component
public class BatchOrderListener {

    /**
     * Pattern 2: Process batch and acknowledge all at once
     * Better throughput for batch processing
     */
    @KafkaListener(
        topics = "${app.kafka.topics.order-events}",
        groupId = "batch-consumer-group",
        containerFactory = "batchKafkaListenerContainerFactory"
    )
    public void listenBatch(
            List<ConsumerRecord<String, String>> records,
            Acknowledgment acknowledgment) {
        
        log.info("Received batch of {} messages", records.size());
        
        try {
            // Process entire batch
            for (ConsumerRecord<String, String> record : records) {
                log.info("Processing: partition={}, offset={}, key={}", 
                         record.partition(), record.offset(), record.key());
                processMessage(record.value());
            }
            
            // Commit all offsets in batch (better performance)
            acknowledgment.acknowledge();
            
            log.info("Successfully processed and committed batch of {} messages", records.size());
            
        } catch (Exception ex) {
            log.error("Failed to process batch: error={}", ex.getMessage(), ex);
            // Don't acknowledge - entire batch will be reprocessed
        }
    }
    
    private void processMessage(String message) {
        // Business logic
    }
}
```

**Batch container factory configuration:**

```java
@Bean
public ConcurrentKafkaListenerContainerFactory<String, String> batchKafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, String> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
    
    factory.setConsumerFactory(consumerFactory());
    
    // Enable batch listening
    factory.setBatchListener(true);
    
    // Manual acknowledgment
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
    
    factory.setConcurrency(3);
    
    return factory;
}
```

### 4.3 Pattern 3: Access Consumer Directly for Fine-Grained Control

```java
@Slf4j
@Component
public class AdvancedOffsetListener {

    /**
     * Pattern 3: Direct consumer access for granular offset control
     * Useful for custom offset management strategies
     */
    @KafkaListener(
        topics = "${app.kafka.topics.order-events}",
        groupId = "advanced-consumer-group",
        containerFactory = "consumerAwareListenerFactory"
    )
    public void listenWithConsumer(
            ConsumerRecord<String, String> record,
            Consumer<String, String> consumer) {
        
        log.info("Received: partition={}, offset={}, key={}", 
                 record.partition(), record.offset(), record.key());
        
        try {
            // Process message
            processMessage(record.value());
            
            // Commit specific offset synchronously
            Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1)  // Next offset to consume
            );
            
            consumer.commitSync(offsets);
            log.info("Committed offset: {}", record.offset() + 1);
            
        } catch (Exception ex) {
            log.error("Processing failed: offset={}, error={}", record.offset(), ex.getMessage(), ex);
            
            // Custom error handling
            // Option 1: Seek to retry
            consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
            
            // Option 2: Skip and commit
            // consumer.commitSync(...);
        }
    }
    
    private void processMessage(String message) {
        // Business logic
    }
}
```

**Consumer-aware factory configuration:**

```java
@Bean
public ConcurrentKafkaListenerContainerFactory<String, String> consumerAwareListenerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, String> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
    
    factory.setConsumerFactory(consumerFactory());
    
    // Disable auto-commit (we'll commit manually via Consumer)
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
    
    factory.setConcurrency(3);
    
    return factory;
}
```

### 4.4 Pattern 4: Commit Every N Records

```java
@Slf4j
@Component
public class PeriodicCommitListener {

    private final AtomicInteger messageCount = new AtomicInteger(0);
    private final int COMMIT_THRESHOLD = 50;

    /**
     * Pattern 4: Commit every N records for balanced performance
     */
    @KafkaListener(
        topics = "${app.kafka.topics.order-events}",
        groupId = "periodic-commit-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void listenWithPeriodicCommit(
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {
        
        try {
            processMessage(record.value());
            
            // Increment counter
            int count = messageCount.incrementAndGet();
            
            // Commit every 50 messages
            if (count % COMMIT_THRESHOLD == 0) {
                acknowledgment.acknowledge();
                log.info("Committed batch at count: {}, offset: {}", count, record.offset());
            }
            
        } catch (Exception ex) {
            log.error("Processing failed: offset={}", record.offset(), ex);
            // Reset counter and don't commit
            messageCount.set(0);
        }
    }
    
    private void processMessage(String message) {
        // Business logic
    }
}
```

---

## 5. Multiple Consumers in Consumer Group

### 5.1 How Partition Assignment Works

When you have **multiple consumers in the same group**, Kafka assigns partitions automatically:[cite:172][cite:209][cite:217]

**Example scenario:**
- Topic: `orders` with **6 partitions** (P0-P5)
- Consumer group: `order-processor-group`
- **3 consumer instances** deployed

**Partition assignment:**
```
Consumer Instance 1 → Partitions: 0, 1
Consumer Instance 2 → Partitions: 2, 3
Consumer Instance 3 → Partitions: 4, 5
```

**Key rules:**[cite:172][cite:211]
1. Each partition assigned to **exactly one consumer** in the group.
2. One consumer can handle **multiple partitions**.
3. If consumers > partitions, some consumers will be **idle**.
4. Each consumer tracks offsets **independently** for its partitions.

### 5.2 Application Configuration for Multiple Instances

**application.yml (same across all instances):**

```yaml
spring:
  application:
    name: order-consumer-service
  
  kafka:
    bootstrap-servers: localhost:9092
    
    consumer:
      # SAME group ID across all instances
      group-id: order-processor-group
      
      enable-auto-commit: false
      auto-offset-reset: earliest
      
    listener:
      ack-mode: manual
      
      # Concurrency per instance (each creates N consumer threads)
      concurrency: 2  # Each instance has 2 consumer threads

app:
  kafka:
    topics:
      orders: orders.v1
```

**Deployment:**
```bash
# Instance 1 (server-1)
java -jar consumer-service.jar --server.port=8081

# Instance 2 (server-2)
java -jar consumer-service.jar --server.port=8082

# Instance 3 (server-3)
java -jar consumer-service.jar --server.port=8083
```

**Result:**
- 3 instances × 2 threads = **6 total consumers** in group.
- Topic with 6 partitions → each consumer gets 1 partition.
- Topic with 12 partitions → each consumer gets 2 partitions.

### 5.3 Consumer Listener (All Instances Use Same Code)

```java
package com.example.consumer.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class OrderListener {

    /**
     * This listener runs on ALL instances
     * Kafka automatically assigns different partitions to each consumer
     */
    @KafkaListener(
        topics = "${app.kafka.topics.orders}",
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeOrder(
            @Payload String order,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.GROUP_ID) String groupId,
            Acknowledgment acknowledgment) {
        
        log.info("Consumer [{}] processing message from partition={}, offset={}, key={}", 
                 Thread.currentThread().getName(), partition, offset, key);
        
        try {
            // Business logic
            processOrder(order, partition);
            
            // Commit offset for THIS partition only
            acknowledgment.acknowledge();
            
            log.info("Successfully committed: partition={}, offset={}", partition, offset);
            
        } catch (Exception ex) {
            log.error("Failed to process: partition={}, offset={}, error={}", 
                      partition, offset, ex.getMessage(), ex);
            // Don't commit - will be retried
        }
    }
    
    private void processOrder(String order, int partition) {
        log.debug("Processing order on partition {}: {}", partition, order);
        // Your business logic here
    }
}
```

**Key observations:**[cite:209][cite:211]
- Each consumer instance runs the **same code**.
- Kafka **automatically distributes** partitions across instances.
- Each consumer **independently commits** offsets for its assigned partitions.
- If one consumer fails, Kafka **rebalances** and reassigns its partitions to surviving consumers.

### 5.4 Offset Isolation Between Consumers

**Critical concept:**[cite:211][cite:209]

```
Consumer 1 (Partition 0) commits offset 100
Consumer 2 (Partition 1) commits offset 200
Consumer 3 (Partition 2) commits offset 150

These offsets are INDEPENDENT and stored separately in __consumer_offsets
```

**Each consumer:**
- Tracks its own offsets for its assigned partitions.
- Cannot see or affect offsets of other consumers' partitions.
- Commits are **per-partition, per-consumer-group**.

**Verification:**

```bash
# Check consumer group status
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group order-processor-group \
  --describe

# Output shows offset per partition:
# TOPIC     PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID                    HOST
# orders    0          1000            1000            0    consumer-1-abc123             /192.168.1.10
# orders    1          2000            2000            0    consumer-2-def456             /192.168.1.11
# orders    2          1500            1500            0    consumer-3-ghi789             /192.168.1.12
```

### 5.5 Handling Consumer Failures and Rebalancing

**Scenario: Consumer crashes**[cite:210][cite:217]

```
Initial state:
Consumer 1 → Partitions 0, 1
Consumer 2 → Partitions 2, 3
Consumer 3 → Partitions 4, 5

Consumer 2 crashes →

After rebalance:
Consumer 1 → Partitions 0, 1, 2
Consumer 3 → Partitions 4, 5, 3

Consumer 1 picks up partition 2 from last committed offset by Consumer 2
```

**What happens:**[cite:210][cite:213]
1. Kafka detects Consumer 2 missed heartbeats.
2. Group coordinator triggers **rebalance**.
3. Partitions 2 and 3 reassigned to surviving consumers.
4. New owners start consuming from **last committed offsets** for those partitions.

---

## 6. Rebalance Listeners

### 6.1 Understanding Rebalance Lifecycle

Rebalance occurs when:[cite:210][cite:217]
- Consumer joins or leaves the group.
- Consumer crashes (misses heartbeats).
- Topic partition count changes.
- Consumer subscription changes.

**Lifecycle:**[cite:213][cite:210]
1. **onPartitionsRevoked** - Called before partitions are taken away.
2. **Rebalance happens** - Coordinator assigns partitions.
3. **onPartitionsAssigned** - Called after new partitions assigned.

### 6.2 ConsumerAwareRebalanceListener Implementation

```java
package com.example.consumer.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;

@Slf4j
@Component
public class CustomRebalanceListener implements ConsumerAwareRebalanceListener {

    /**
     * Called BEFORE partitions are revoked from this consumer
     * Last chance to commit offsets or clean up resources
     */
    @Override
    public void onPartitionsRevokedBeforeCommit(
            Consumer<?, ?> consumer,
            Collection<TopicPartition> partitions) {
        
        log.warn("Partitions being revoked BEFORE commit: {}", partitions);
        
        // Option: Force manual commit if you have pending acknowledgments
        // If using manual ack, acknowledge any pending messages here
    }

    /**
     * Called AFTER pending offsets are committed (if auto-commit enabled)
     * Use this to store offsets externally or clean up resources
     */
    @Override
    public void onPartitionsRevokedAfterCommit(
            Consumer<?, ?> consumer,
            Collection<TopicPartition> partitions) {
        
        log.warn("Partitions revoked AFTER commit: {}", partitions);
        
        // Store current offsets externally (database, Redis, etc.)
        for (TopicPartition partition : partitions) {
            OffsetAndMetadata offsetMetadata = consumer.committed(partition);
            if (offsetMetadata != null) {
                long offset = offsetMetadata.offset();
                log.info("Storing offset for {}: {}", partition, offset);
                // Store to external system: saveOffsetToDatabase(partition, offset);
            }
        }
        
        // Clean up partition-specific resources
        cleanupResources(partitions);
    }

    /**
     * Called when new partitions are assigned to this consumer
     * Use this to seek to custom offsets or initialize resources
     */
    @Override
    public void onPartitionsAssigned(
            Consumer<?, ?> consumer,
            Collection<TopicPartition> partitions) {
        
        log.info("New partitions assigned: {}", partitions);
        
        // Option 1: Seek to custom offsets (e.g., from external database)
        for (TopicPartition partition : partitions) {
            // Long customOffset = loadOffsetFromDatabase(partition);
            // if (customOffset != null) {
            //     consumer.seek(partition, customOffset);
            //     log.info("Seeking {} to custom offset: {}", partition, customOffset);
            // }
        }
        
        // Option 2: Initialize partition-specific resources
        initializeResources(partitions);
    }

    /**
     * Called when partitions are lost (exceptional case)
     * Consumer lost ownership without graceful revocation
     */
    @Override
    public void onPartitionsLost(
            Consumer<?, ?> consumer,
            Collection<TopicPartition> partitions) {
        
        log.error("Partitions LOST (ungraceful): {}", partitions);
        
        // Emergency cleanup - partitions already reassigned to other consumers
        emergencyCleanup(partitions);
    }
    
    private void cleanupResources(Collection<TopicPartition> partitions) {
        log.debug("Cleaning up resources for: {}", partitions);
        // Close connections, flush caches, etc.
    }
    
    private void initializeResources(Collection<TopicPartition> partitions) {
        log.debug("Initializing resources for: {}", partitions);
        // Open connections, warm up caches, etc.
    }
    
    private void emergencyCleanup(Collection<TopicPartition> partitions) {
        log.debug("Emergency cleanup for: {}", partitions);
        // Force cleanup without graceful shutdown
    }
}
```

### 6.3 Registering Rebalance Listener

```java
@Configuration
public class KafkaConsumerConfig {

    @Autowired
    private CustomRebalanceListener rebalanceListener;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        
        factory.setConsumerFactory(consumerFactory());
        
        // Register rebalance listener
        factory.getContainerProperties().setConsumerRebalanceListener(rebalanceListener);
        
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setConcurrency(3);
        
        return factory;
    }
}
```

### 6.4 Use Case: External Offset Storage

```java
@Slf4j
@Component
public class DatabaseOffsetRebalanceListener implements ConsumerAwareRebalanceListener {

    @Autowired
    private OffsetRepository offsetRepository;  // Your database repository

    @Override
    public void onPartitionsRevokedAfterCommit(
            Consumer<?, ?> consumer,
            Collection<TopicPartition> partitions) {
        
        // Save offsets to database before losing partitions
        for (TopicPartition partition : partitions) {
            OffsetAndMetadata committed = consumer.committed(partition);
            if (committed != null) {
                offsetRepository.save(
                    partition.topic(),
                    partition.partition(),
                    committed.offset()
                );
                log.info("Saved offset to DB: {} = {}", partition, committed.offset());
            }
        }
    }

    @Override
    public void onPartitionsAssigned(
            Consumer<?, ?> consumer,
            Collection<TopicPartition> partitions) {
        
        // Restore offsets from database when receiving new partitions
        for (TopicPartition partition : partitions) {
            Long savedOffset = offsetRepository.findOffset(
                partition.topic(),
                partition.partition()
            );
            
            if (savedOffset != null) {
                consumer.seek(partition, savedOffset);
                log.info("Restored offset from DB: {} = {}", partition, savedOffset);
            } else {
                // No saved offset, use Kafka's committed offset or auto-offset-reset
                log.info("No saved offset for {}, using Kafka default", partition);
            }
        }
    }

    @Override
    public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        // Optional: handle pre-commit logic
    }

    @Override
    public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        log.error("Lost partitions without cleanup: {}", partitions);
    }
}
```

---

## 7. Advanced Consumer Patterns

### 7.1 Partition-Specific Listeners

```java
@Slf4j
@Component
public class PartitionSpecificListener {

    /**
     * Listen to specific partitions only
     * Useful for testing or specific partition processing
     */
    @KafkaListener(
        topicPartitions = @TopicPartition(
            topic = "${app.kafka.topics.orders}",
            partitions = {"0", "1", "2"}  // Only partitions 0, 1, 2
        ),
        groupId = "partition-specific-group"
    )
    public void listenToSpecificPartitions(
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {
        
        log.info("Processing from partition {}: offset={}", 
                 record.partition(), record.offset());
        
        try {
            processMessage(record.value());
            acknowledgment.acknowledge();
        } catch (Exception ex) {
            log.error("Failed: {}", ex.getMessage(), ex);
        }
    }
    
    private void processMessage(String message) {
        // Business logic
    }
}
```

### 7.2 Seeking to Specific Offsets

```java
@Slf4j
@Component
public class CustomSeekListener {

    /**
     * Use rebalance listener to seek to custom offset
     */
    @Bean
    public ConsumerAwareRebalanceListener seekToOffsetListener() {
        return new ConsumerAwareRebalanceListener() {
            
            @Override
            public void onPartitionsAssigned(
                    Consumer<?, ?> consumer,
                    Collection<TopicPartition> partitions) {
                
                for (TopicPartition partition : partitions) {
                    // Seek to specific offset (e.g., replay from 1000)
                    long customOffset = 1000L;
                    consumer.seek(partition, customOffset);
                    log.info("Seeking {} to offset {}", partition, customOffset);
                }
            }

            @Override
            public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {}

            @Override
            public void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {}

            @Override
            public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {}
        };
    }
}
```

### 7.3 Seeking to Beginning or End

```java
@Slf4j
@Component
public class SeekToBeginningListener {

    @Bean
    public ConsumerAwareRebalanceListener seekToBeginningListener() {
        return new ConsumerAwareRebalanceListener() {
            
            @Override
            public void onPartitionsAssigned(
                    Consumer<?, ?> consumer,
                    Collection<TopicPartition> partitions) {
                
                // Seek to beginning of all assigned partitions
                consumer.seekToBeginning(partitions);
                log.info("Seeking to beginning for partitions: {}", partitions);
                
                // OR seek to end
                // consumer.seekToEnd(partitions);
            }

            @Override
            public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {}
            @Override
            public void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {}
            @Override
            public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {}
        };
    }
}
```

---

## 8. Production Best Practices

### 8.1 Offset Management Checklist

- [ ] **Use manual commit** (`enable-auto-commit: false`) for production.
- [ ] **Commit only after successful processing** to avoid data loss.
- [ ] **Handle exceptions gracefully** - don't commit on failure.
- [ ] **Monitor consumer lag** to detect processing issues early.
- [ ] **Use idempotent processing** - handle duplicate messages.
- [ ] **Configure proper timeouts** (session, heartbeat, poll).
- [ ] **Implement rebalance listeners** for resource cleanup.
- [ ] **Use CooperativeStickyAssignor** for minimal rebalance disruption.[cite:216][cite:217]

### 8.2 Concurrency Guidelines

**Topic with 12 partitions, 3 instances:**

```yaml
# Option 1: 1 thread per instance = 3 consumers total (9 partitions idle)
spring.kafka.listener.concurrency: 1

# Option 2: 4 threads per instance = 12 consumers total (perfect match)
spring.kafka.listener.concurrency: 4  # Recommended

# Option 3: 6 threads per instance = 18 consumers total (6 idle)
spring.kafka.listener.concurrency: 6  # Over-provisioned
```

**Rule of thumb:**
- **Concurrency × Instances ≈ Number of partitions**
- More consumers than partitions = wasted resources (idle consumers).
- Fewer consumers than partitions = some consumers handle multiple partitions.

### 8.3 Session and Heartbeat Tuning

```yaml
spring:
  kafka:
    consumer:
      # Heartbeat sent every 3 seconds
      heartbeat-interval-ms: 3000
      
      # Consumer considered dead after 30 seconds without heartbeat
      session-timeout-ms: 30000
      
      # Max time between polls before consumer kicked out
      max-poll-interval-ms: 300000  # 5 minutes
```

**Guidelines:**
- `heartbeat-interval-ms` < `session-timeout-ms / 3`
- Increase `max-poll-interval-ms` if processing takes long.
- Lower `session-timeout-ms` for faster failure detection (but increases rebalances).

---

## 9. Error Handling and Retry Strategies

### 9.1 Dead Letter Queue (DLQ) Pattern

```java
@Slf4j
@Component
public class ErrorHandlingListener {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${app.kafka.topics.orders-dlq}")
    private String dlqTopic;

    @KafkaListener(
        topics = "${app.kafka.topics.orders}",
        groupId = "error-handling-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void listen(
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {
        
        try {
            processMessage(record.value());
            acknowledgment.acknowledge();
            
        } catch (RetryableException ex) {
            log.warn("Retryable error: offset={}, error={}", record.offset(), ex.getMessage());
            // Don't acknowledge - will be retried after rebalance
            
        } catch (NonRetryableException ex) {
            log.error("Non-retryable error: offset={}, sending to DLQ", record.offset(), ex);
            
            // Send to DLQ
            sendToDLQ(record, ex);
            
            // Acknowledge to skip this message
            acknowledgment.acknowledge();
        }
    }
    
    private void sendToDLQ(ConsumerRecord<String, String> record, Exception ex) {
        // Add error metadata
        ProducerRecord<String, String> dlqRecord = new ProducerRecord<>(
            dlqTopic,
            record.key(),
            record.value()
        );
        
        dlqRecord.headers().add("original-topic", record.topic().getBytes());
        dlqRecord.headers().add("original-partition", 
                                String.valueOf(record.partition()).getBytes());
        dlqRecord.headers().add("original-offset", 
                                String.valueOf(record.offset()).getBytes());
        dlqRecord.headers().add("error-message", ex.getMessage().getBytes());
        
        kafkaTemplate.send(dlqRecord);
        log.info("Sent to DLQ: key={}, offset={}", record.key(), record.offset());
    }
    
    private void processMessage(String message) throws RetryableException, NonRetryableException {
        // Business logic
    }
}
```

---

## 10. Performance Tuning

### 10.1 Key Configuration Parameters

```yaml
spring:
  kafka:
    consumer:
      # Fetch more records per poll for better throughput
      max-poll-records: 500
      
      # Minimum bytes to fetch (reduces small fetches)
      fetch-min-size: 1024  # 1 KB
      
      # Max wait time for min bytes
      fetch-max-wait-ms: 500
      
      # Larger fetch buffer (default 50 MB)
      fetch-max-bytes: 52428800  # 50 MB
      
      # Per-partition fetch buffer
      max-partition-fetch-bytes: 1048576  # 1 MB
```

### 10.2 Monitoring Consumer Lag

```bash
# Check consumer group lag
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group order-processor-group \
  --describe

# Output:
# TOPIC     PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
# orders    0          10000           10500           500
# orders    1          9500            9500            0
```

**Lag indicators:**
- Lag = 0: Consumer is caught up.
- Lag increasing: Consumer can't keep up with producers.
- Lag decreasing: Consumer catching up.

---

## Summary

**Key takeaways:**

1. **Multiple consumers in a group** share workload by partition assignment.[cite:172][cite:209]
2. **Each consumer manages offsets independently** for its assigned partitions.[cite:209][cite:211]
3. **Manual commit** with `Acknowledgment.acknowledge()` is production standard.[cite:205][cite:206]
4. **Rebalance listeners** enable custom offset management and resource cleanup.[cite:210][cite:213]
5. **CooperativeStickyAssignor** minimizes rebalance disruption.[cite:216][cite:217]
6. **Offset commits are per-partition** - consumers don't interfere with each other.[cite:211]
7. **Monitor consumer lag** to detect processing bottlenecks early.[cite:30][cite:214]

This configuration provides production-grade consumers with proper offset management, error handling, and scalability across multiple instances.[cite:172][cite:205][cite:209][cite:210]
