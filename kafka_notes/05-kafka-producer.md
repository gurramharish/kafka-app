# Kafka Producers: Patterns, Best Practices, Exactly-Once, and Spring Boot

## 1. Producer Basics (From Zero)

A Kafka producer sends records (key, value, headers) to a **topic**, and Kafka routes each record to a **partition**.[web:288] Ordering is guaranteed **within a partition**, not across the whole topic.[web:172]

Key basic configs:[web:288][web:289]  
- `bootstrap.servers` – broker addresses.  
- `key.serializer` / `value.serializer` – how keys/values become bytes.  
- `acks` – durability level: `0`, `1`, or `all`.  
- `retries`, `enable.idempotence` – reliability on failures.  

---

## 2. Ways to Produce Messages

### 2.1 Fire-and-Forget (Async, No Check)

```java
ProducerRecord<String, String> record =
    new ProducerRecord<>("orders", "order-1", "payload");

producer.send(record); // no callback, no get()
```

- Producer **does not wait** for broker response; errors are not handled.[web:288]
- Good for **low-value telemetry/logs**, not for critical data.

---

### 2.2 Async with Callback (Recommended Default)

```java
producer.send(record, (metadata, exception) -> {
    if (exception == null) {
        System.out.printf("Sent to %s-%d offset=%d%n",
                metadata.topic(), metadata.partition(), metadata.offset());
    } else {
        // log, metrics, retry/route to DLQ
        exception.printStackTrace();
    }
});
```

- Non-blocking, high throughput, and you still see success/failure.[web:288][web:289]
- Recommended for most services.

---

### 2.3 Synchronous Send (Blocking)

```java
try {
    RecordMetadata metadata = producer.send(record).get(); // blocks
    System.out.printf("Sent to %s-%d offset=%d%n",
            metadata.topic(), metadata.partition(), metadata.offset());
} catch (Exception e) {
    // handle failure
    e.printStackTrace();
}
```

- Simple error handling: if `get()` throws, you know it failed.[web:288]
- Much lower throughput: each call waits for the broker.

Use sparingly (scripts, admin tools, low-traffic paths).

---

### 2.4 Batching with `batch.size` and `linger.ms`

Producer batches records **per partition** to reduce network calls:[web:288][web:289]

Key configs:

- `batch.size` (bytes) – max batch size in memory.
- `linger.ms` – wait time to accumulate more records before sending.

Example:

```properties
batch.size=32768       # 32 KB
linger.ms=10           # wait up to 10 ms
compression.type=snappy
```

Effects:[web:289][web:298]

- Higher throughput (more records per request).
- Slightly higher latency per record (wait for batching window).

---

### 2.5 Idempotent Producer (Exactly-Once Writes Per Partition)

Config:[web:286][web:287][web:306]

```properties
enable.idempotence=true
acks=all
retries=Integer.MAX_VALUE
max.in.flight.requests.per.connection=5
```

- Guarantees **no duplicates** when the producer retries due to transient errors.[web:286][web:287]
- Works per producer instance and partition.
- Should be default for **any critical stream**.

---

### 2.6 Transactional Producer (Exactly-Once Processing)

Adds **transactions** on top of idempotence:[web:34][web:43]

```properties
transactional.id=orders-service-1   # unique per instance
enable.idempotence=true             # implied by transactional.id
acks=all
```

Pattern for exactly-once processing (read → process → write) is in section **4** below.

Use for: atomic writes to multiple topics and atomic offset commits.

---

## 3. Producer Best Practices (Basic → Advanced)

### 3.1 Basic Practices

- **Always use keys for ordered entities**
E.g., key = `orderId` so all events for one order go to the same partition.[web:288]
- **Set `acks` based on data criticality**:[web:288][web:289]
    - `acks=0`: max speed, risk of loss.
    - `acks=1`: leader only, reasonable default for non-critical events.
    - `acks=all`: safest; recommended with idempotence for critical data.
- **Reuse producer instance**
Producer is thread-safe; create **one per app/config**, not per message.[web:288]

---

### 3.2 Reliability and Throughput

- **Idempotence ON for important topics**
Use `enable.idempotence=true` + `acks=all` for all “business events”.[web:286][web:287][web:289]
- **Tune batching**
    - Larger `batch.size` + small `linger.ms` (5–20 ms) + compression = good balance.[web:289][web:298]
    - Measure with metrics (`record-send-rate`, `request-latency-avg`).
- **Use built-in retries, not custom loops**
Kafka’s own `retries` + idempotence handle transient errors safely; custom loops can re-break exactly-once guarantees.[web:287][web:295]
- **Set timeouts**
    - `delivery.timeout.ms` – overall send timeout.
    - `request.timeout.ms` – RPC timeout.
Prevents hung sends on unstable clusters.[web:288][web:298]

---

### 3.3 Advanced Practices

- **Transactions for read-process-write**
Use transactional producer + `sendOffsetsToTransaction` for exactly-once between input and output topics (see section 4).[web:43][web:300][web:305]
- **Back-pressure awareness**
    - Watch for buffer exhaustion and `max.block.ms` violations.
    - Apply rate limiting or queueing upstream if the producer cannot keep up.[web:298]
- **Observability**
Monitor: error rate, retries, request latency, record sizes, compression ratio.[web:289][web:298]

---

## 4. Exactly-Once Semantics with Transactions (Producer Side)

### 4.1 Transactional Producer Setup

```properties
bootstrap.servers=...

key.serializer=org.apache.kafka.common.serialization.StringSerializer
value.serializer=org.apache.kafka.common.serialization.StringSerializer

transactional.id=orders-processor-1   # unique per instance
# Idempotence + acks=all implied[^1][^2]
```

Initialize and transact:[web:34][web:300][web:303]

```java
KafkaProducer<String, String> producer = new KafkaProducer<>(props);
producer.initTransactions();   // one-time per instance

try {
    producer.beginTransaction();

    producer.send(new ProducerRecord<>("orders-out", key, value));
    producer.send(new ProducerRecord<>("orders-audit", key, auditValue));

    producer.commitTransaction();    // all-or-nothing
} catch (Exception e) {
    producer.abortTransaction();     // no records visible
}
```

This gives **exactly-once writes** for those messages: either all visible or none, with no duplicates.[web:34][web:302]

---

### 4.2 Exactly-Once Processing (Read → Process → Write)

Consumer config:[web:300][web:306][web:307]

```properties
enable.auto.commit=false
isolation.level=read_committed    # ignore uncommitted/aborted records
group.id=orders-processor
```

Core loop (simplified):[web:300][web:303][web:305]

```java
producer.initTransactions();

while (true) {
    ConsumerRecords<String, String> records =
            consumer.poll(Duration.ofMillis(500));

    if (records.isEmpty()) continue;

    try {
        producer.beginTransaction();

        // 1) process and write
        for (ConsumerRecord<String, String> record : records) {
            String out = transform(record.value());
            producer.send(new ProducerRecord<>("orders-out", record.key(), out));
        }

        // 2) prepare offsets to commit
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (TopicPartition tp : records.partitions()) {
            long lastOffset = records.records(tp)
                                     .get(records.records(tp).size() - 1)
                                     .offset();
            offsets.put(tp, new OffsetAndMetadata(lastOffset + 1));
        }

        // 3) include offsets in the transaction
        producer.sendOffsetsToTransaction(
                offsets,
                new ConsumerGroupMetadata("orders-processor")
        );

        producer.commitTransaction();
    } catch (Exception e) {
        producer.abortTransaction();  // everything rolled back
    }
}
```

Guarantee: each input record from `orders-in` is **processed exactly once** into `orders-out` (as seen by `read_committed` consumers), even across failures and retries.[web:43][web:304][web:305]

---

## 5. Error Handling Strategies for Producer Sends and Retries

### 5.1 Types of Failures

- **Retriable** (transient):
    - `TimeoutException`, `NetworkException`, `NotEnoughReplicasException`.[web:295][web:298]
    - Typically safe to retry with idempotence.
- **Non-retriable**:
    - `SerializationException`, `InvalidTopicException`, `RecordTooLargeException`, authorization/ACL errors.[web:288][web:289]
    - Must be fixed in code/config; retrying won’t help.

---

### 5.2 Strategy with Async Callbacks

```java
producer.send(record, (metadata, ex) -> {
    if (ex == null) {
        metrics.success();
        return;
    }

    if (ex instanceof RetriableException) {
        // Option: log and rely on internal retries (producer retries config)
        log.warn("Retriable send failure", ex);
    } else {
        // Non-retriable: log + DLQ + alert
        log.error("Non-retriable send failure", ex);
        sendToDlq(record, ex);
    }
});
```

Best practices:[web:289][web:298]

- Use **producer’s own `retries`** and `delivery.timeout.ms` first.
- Optionally add **application-level retry** with:
    - Fixed or exponential backoff.
    - Max attempts, then route to **DLQ** for manual inspection.

---

### 5.3 Strategy with Sync Sends

```java
try {
    producer.send(record).get();
} catch (ExecutionException ee) {
    Throwable ex = ee.getCause();

    if (ex instanceof RetriableException) {
        // maybe small number of manual retries with backoff
    } else {
        // log + DLQ, or fail the request
    }
} catch (InterruptedException ie) {
    Thread.currentThread().interrupt();
}
```

- Simpler control flow, but can **block threads** under load.
- Combine with bounded queues / back-pressure to avoid exhausting thread pools.

---

## 6. Producer Config Tuning for High Throughput

### 6.1 Core Throughput Settings

Typical starting point for a high-throughput, reliable producer:[web:289][web:298]

```properties
acks=all
enable.idempotence=true
retries=Integer.MAX_VALUE
max.in.flight.requests.per.connection=5

batch.size=32768         # 32 KB
linger.ms=5-20           # trade-off throughput vs latency
compression.type=snappy  # or lz4
buffer.memory=67108864   # 64 MB (Java producer)
```

Effects:[web:288][web:289][web:298]

- **Bigger batch.size** + small **linger.ms** ⇒ better throughput.
- **Compression** ⇒ reduce network usage at cost of some CPU.
- **Idempotence + retries** ⇒ strong reliability with minimal overhead on modern clusters.

---

### 6.2 Monitoring and Adjusting

Watch:[web:289][web:298]

- `record-send-rate` – messages per second.
- `request-latency-avg` / `p95` – end-to-end send latency.
- `record-error-rate` / `record-retry-rate`.

Adjust if:

- Latency too high ⇒ reduce `linger.ms`.
- CPU too high ⇒ try different compression or smaller `batch.size`.
- Network saturated ⇒ increase compression and `batch.size`.

---

## 7. Spring Boot `KafkaTemplate` – Patterns and Retries

### 7.1 Basic Async Send

```java
@Autowired
private KafkaTemplate<String, OrderEvent> kafkaTemplate;

public void publishOrder(OrderEvent event) {
    ListenableFuture<SendResult<String, OrderEvent>> future =
        kafkaTemplate.send("orders", event.getOrderId(), event);

    future.addCallback(result -> {
        // success: log partition/offset
    }, ex -> {
        // failure: log, metrics, maybe send to DLQ
    });
}
```

`KafkaTemplate.send()` is **asynchronous** and returns a future; adding a callback is equivalent to the plain producer async pattern.[web:291][web:294][web:297]

---

### 7.2 Sync Send in Spring

```java
public void publishOrderSync(OrderEvent event) throws Exception {
    SendResult<String, OrderEvent> result =
        kafkaTemplate.send("orders", event.getOrderId(), event).get();
    // handle success
}
```

Same trade-offs: easier error handling, lower throughput.[web:290][web:294]

---

### 7.3 Retries and Timeouts in Spring

Spring delegates to the Kafka producer configs:[web:291][web:294]

```yaml
spring:
  kafka:
    producer:
      retries: 5
      properties:
        delivery.timeout.ms: 120000
        request.timeout.ms: 30000
        enable.idempotence: true
        acks: all
```

For **application-level retries** on send failures, you can use:

- Spring Retry on your service method.
- Or custom retry logic in the send callback with backoff and a max retry count.

---

### 7.4 Transactions with `KafkaTemplate`

Producer factory (transactional):[web:290][web:311]

```java
@Bean
public ProducerFactory<String, OrderEvent> txProducerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "orders-tx-");
    return new DefaultKafkaProducerFactory<>(props);
}

@Bean
public KafkaTemplate<String, OrderEvent> txKafkaTemplate() {
    KafkaTemplate<String, OrderEvent> template =
        new KafkaTemplate<>(txProducerFactory());
    template.setTransactionIdPrefix("orders-tx-");
    return template;
}
```

Transactional send:[web:290][web:311]

```java
public void publishOrderWithAudit(OrderEvent order, AuditEvent audit) {
    txKafkaTemplate.executeInTransaction(operations -> {
        operations.send("orders", order.getOrderId(), order);
        operations.send("orders-audit", order.getOrderId(), audit);
        return null;
    });
}
```

- Spring handles `beginTransaction`, commit/abort around the lambda.
- Combined with a transactional consumer container, this forms an **exactly-once processing pipeline** within Kafka.[web:311]

---

## 8. Key Takeaways (Cheat Sheet)

- **Basic ways to send**: fire-and-forget, async with callback (recommended), sync/blocking.
- **Reliability**: always prefer `acks=all` + `enable.idempotence=true` for important data.[web:286][web:287][web:289]
- **Throughput**: tune `batch.size`, `linger.ms`, and compression; measure with producer metrics.[web:289][web:298]
- **Exactly-once**: use transactional producers + `sendOffsetsToTransaction` + `read_committed` consumers for EOS inside Kafka.[web:34][web:43][web:300][web:305]
- **Spring Boot**: `KafkaTemplate` wraps these patterns; use async sends with callbacks, transactional templates for EOS, and proper retries/timeouts in `spring.kafka.producer` config.[web:291][web:294][web:311]

```


<div align="center">⁂</div>

[^1]: https://stackoverflow.com/questions/60283718/difference-between-kafka-idempotent-and-transactional-producer-setup/71130037
[^2]: https://programming.vip/docs/transaction-and-idempotency-of-kafka-producer.html```

