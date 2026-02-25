

# Spring KafkaTemplate

Spring’s `KafkaTemplate` lets you send records in several ways (different overloads and patterns) and then layer transactions on top to get “all‑or‑nothing” semantics for batches. Below is a structured walkthrough of the send methods, usage patterns, and all the transactional options.

## Core KafkaTemplate send methods

These are the main send APIs from the Spring Kafka docs and `KafkaTemplate` Javadoc.[^1][^2]


| Method family | Typical use case |
| :-- | :-- |
| `sendDefault(...)` | Use a preconfigured default topic; reduce boilerplate when most sends go to one topic. |
| `send(String topic, ...)` | Explicit topic each time; most common choice in services writing to multiple topics. |
| `send(ProducerRecord<K,V> record)` | Full control over headers, partition, timestamp; when you manually build `ProducerRecord`. |
| `send(Message<?> message)` | Spring Messaging style; topic/partition/key in headers; nice for generic messaging components. |
| `flush()` | Force immediate send of buffered records; rarely needed in normal request/response code. |
| `execute(ProducerCallback)` | Direct low‑level access to the raw `Producer` for custom operations. |
| `executeInTransaction(OperationsCallback)` | Run several sends in a single Kafka transaction (commit/rollback as a unit). |
| `sendOffsetsToTransaction(...)` | For exactly‑once “read→process→write” scenarios; commit consumer offsets as part of the tx. |

### Overloads and when to use them

From the reference docs, the key overloads are:[^2][^1]

- **Default‑topic variants** (you configure `template.setDefaultTopic("my-topic")` or via Boot properties):
    - `sendDefault(V data)`
    - `sendDefault(K key, V data)`
    - `sendDefault(Integer partition, K key, V data)`
    - `sendDefault(Integer partition, Long timestamp, K key, V data)`

**When:** Your service almost always writes to the same topic and you want less noise in code.
- **Explicit topic variants**:
    - `send(String topic, V data)`
    - `send(String topic, K key, V data)`
    - `send(String topic, Integer partition, K key, V data)`
    - `send(String topic, Integer partition, Long timestamp, K key, V data)`

**When:**
- You write to multiple topics from the same template.
- You need to pin to a partition (e.g., ordering guarantees by key).
- You need custom timestamp for time‑based retention/stream processing.
- **Record / Message variants**:
    - `send(ProducerRecord<K,V> record)` – use if you want:
        - custom headers,
        - explicit topic/partition/timestamp/key/headers in one object.[^2]
    - `send(Message<?> message)` – Spring `Message` where topic, key, partition, etc., are in headers (e.g. `KafkaHeaders.TOPIC`, `KafkaHeaders.MESSAGE_KEY`).[^2]

**When:** You are integrating with generic Spring Messaging/Integration pipelines or want heavy use of headers.

All `send(...)` methods return `CompletableFuture<SendResult<K,V>>` (from Spring 3+; older versions used `ListenableFuture`).[^2]

## Usage patterns: fire‑and‑forget, async, sync

All the above send methods return a `CompletableFuture`, so you can use three main styles:[^3][^2]

1. **Fire‑and‑forget (good throughput, less safety)**
```java
kafkaTemplate.send("orders", orderId, payload);
// no future handling – failures only show up in logs
```

Use when:

- Kafka is best‑effort for this use case, or
- you have global error handling via producer interceptors / monitoring.

2. **Async with callback (non‑blocking, handles failures)**
```java
CompletableFuture<SendResult<String, String>> future =
        kafkaTemplate.send("orders", orderId, payload);

future.whenComplete((result, ex) -> {
    if (ex == null) {
        log.info("Sent to topic={}, partition={}, offset={}",
                 result.getRecordMetadata().topic(),
                 result.getRecordMetadata().partition(),
                 result.getRecordMetadata().offset());
    } else {
        log.error("Failed to send order {}", orderId, ex);
        // maybe enqueue for retry / alert
    }
});
```

Use when:

- You want high throughput and non‑blocking I/O.
- You need to log or act on send failures.

3. **Sync / blocking (request‑response style)**
```java
try {
    SendResult<String, String> result =
        kafkaTemplate.send("orders", orderId, payload)
                     .get(10, TimeUnit.SECONDS);
    // success
} catch (ExecutionException e) {
    // real send failure – inspect e.getCause()
} catch (TimeoutException | InterruptedException e) {
    // timeout or thread interrupted
}
```

Use when:

- The HTTP request/DB transaction should fail immediately if Kafka send fails (strong coupling).
- End‑user operation must not be acknowledged unless the message is durably written.[^3][^2]


## Enabling transactions for KafkaTemplate

To use any transactional APIs, the underlying `ProducerFactory` must be transaction‑capable (transactional id prefix set). Spring Boot uses this when you set `spring.kafka.producer.transaction-id-prefix`.[^4][^5]

### Example config (Java)

```java
@Bean
public ProducerFactory<String, String> producerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    DefaultKafkaProducerFactory<String, String> factory =
            new DefaultKafkaProducerFactory<>(props);

    // key line: makes this producer transactional
    factory.setTransactionIdPrefix("tx-orders-"); // or via Boot property

    return factory;
}

@Bean
public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> pf) {
    return new KafkaTemplate<>(pf);
}
```

Key points:[^6][^5][^4]

- Setting a transaction id prefix internally enables Kafka’s idempotent + transactional producer.
- `KafkaTemplate.isTransactional()` now returns `true`.
- Any transactional use (local `executeInTransaction` or `@Transactional` with `KafkaTransactionManager`) now works.
- Consumers that must not see aborted records should set `isolation.level=read_committed`.[^7][^8]


## Local Kafka‑only transactions with executeInTransaction

This is the simplest way to wrap multiple sends in one Kafka transaction:

```java
boolean result = kafkaTemplate.executeInTransaction(ops -> {
    for (OrderEvent evt : events) {
        ops.send("order-events", evt.getKey(), evt);
        // optionally block & check
        // ops.send(...).get();
    }

    // if some business validation fails, abort:
    if (!isValid(events)) {
        throw new IllegalStateException("Validation failed");
    }

    return true;
});
```

Behavior (per docs and examples):[^5][^4][^6][^7]

- Spring opens a Kafka transaction at the start of the callback.
- All `ops.send(...)` calls participate in that transaction.
- If the callback returns normally, the transaction is committed and:
    - All records become visible atomically to `read_committed` consumers.
- If the callback throws any exception, Spring rolls back the transaction:
    - None of the records become visible; this gives you **all‑or‑nothing** semantics.


### Detecting send failures and triggering rollback

Inside the `executeInTransaction` callback you can:

- **Block on each future** and throw if it fails:

```java
kafkaTemplate.executeInTransaction(ops -> {
    for (OrderEvent evt : events) {
        try {
            ops.send("order-events", evt.getKey(), evt)
               .get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Failed to send", e); // rollback
        }
    }
    return null;
});
```

- **Use async callbacks and track failure flag**, then throw at the end (works if you wait for all futures to complete before returning).

Any unchecked exception from the callback will mark the Kafka transaction for rollback.[^5]

This gives you exactly what you asked: “batch messages in a transaction so that if anything fails in the loop, all are rolled back.”

## Spring @Transactional with KafkaTransactionManager

Instead of calling `executeInTransaction` yourself, you can let Spring’s transaction infrastructure manage Kafka transactions using `KafkaTransactionManager`.[^4][^5]

### Configuration

```java
@Bean
public KafkaTransactionManager<String, String> kafkaTransactionManager(
        ProducerFactory<String, String> pf) {
    return new KafkaTransactionManager<>(pf);
}
```

Then:

```java
@Transactional("kafkaTransactionManager")
public void publishAll(List<OrderEvent> events) {
    for (OrderEvent evt : events) {
        kafkaTemplate.send("order-events", evt.getKey(), evt);
    }

    // same rule: throw to rollback
    if (!isValid(events)) {
        throw new IllegalStateException("Validation failed");
    }
}
```

Behavior:[^9][^4][^5]

- The `@Transactional` interceptor starts a Spring transaction backed by `KafkaTransactionManager`.
- `KafkaTemplate` detects the active transaction and enlists its sends into that Kafka transaction.
- On method exit:
    - If no exception, Kafka transaction commits and all events become visible.
    - If runtime (or configured) exception, Kafka transaction rolls back; events are not visible.


### Chaining Kafka and DB in one logical unit

You can also combine DB and Kafka so that both commit/rollback together, using transaction synchronization or a chained transaction manager:[^9][^4][^5]

```java
@Transactional  // with a JPA/DataSource transaction manager
public void process(List<Thing> things) {
    things.forEach(t -> kafkaTemplate.send("topic", t)); // enlisted in Kafka tx
    updateDb(things); // DB updates
}
```

Spring arranges that DB and Kafka commits/rollbacks happen in a defined order; if something fails, both are rolled back (details depend on configuration, but conceptually this is a “read‑process‑write” EOS building block).[^4][^9]

## Consumer‑initiated transactions and sendOffsetsToTransaction

For full **exactly‑once** “read → process → write” pipelines (Kafka → app → Kafka) you typically:

1. Configure the listener container with `KafkaTransactionManager` so it starts a Kafka transaction per poll.
2. Use `KafkaTemplate` to send output records.
3. Use `KafkaTemplate.sendOffsetsToTransaction(...)` to commit consumer offsets in the same transaction.[^10][^5][^4]

Example sketch (manual style):

```java
kafkaTemplate.executeInTransaction(ops -> {
    consumerRecords.forEach(rec -> {
        // process
        ops.send("output-topic", rec.key(), transform(rec.value()));
    });

    // commit offsets atomically with produced messages
    ops.sendOffsetsToTransaction(offsetsToCommit, consumerGroupMetadata);

    return null;
});
```

- If the transaction commits, both:
    - produced messages, and
    - updated consumer offsets
      become visible/committed together.[^8][^11][^5][^4]
- If it rolls back, neither is committed; on restart, the same input records will be re‑processed, but downstream will not see duplicates.

This is the canonical **KafkaTemplate + transactions + batching** pattern for EOS.

## Batching vs transactions: how they interact

- **Producer batching** (controlled by `batch.size`, `linger.ms`, etc.) is internal to the Kafka client and groups records in the same partition into one request for efficiency.[^12][^13]
- **Transactions** group a set of records (possibly across partitions and topics) into a logical unit that is either committed or aborted.[^14][^15][^8]

In your Spring Boot app:

- Put your send loop inside `executeInTransaction` or an `@Transactional` Kafka method for **all‑or‑nothing visibility**.
- Rely on `batch.size` + `linger.ms` for wire‑level batching; you do not need to manually build huge “batch messages.”


## Practical recommendations

- For simple producers:
    - Use `send(topic, key, value)` + async callbacks in most REST‑style services.
- For **transactional batch send in a loop**:
    - Prefer `executeInTransaction` for purely Kafka‑side transactions.
    - Or `@Transactional("kafkaTransactionManager")` when you want Spring managing Kafka transactions.
    - Inside the loop, either block on each future or aggregate results and throw if anything fails, to roll back.
- For end‑to‑end EOS (consumer → producer):
    - Use container‑managed transactions plus `KafkaTemplate.sendOffsetsToTransaction(...)` and consumers configured with `isolation.level=read_committed`.[^7][^8][^5][^4]

If you share your current Spring Boot configuration (producer props and how you wire `KafkaTemplate`), a concrete, end‑to‑end transactional example tailored to your setup can be sketched.
<span style="display:none">[^16][^17][^18][^19][^20][^21]</span>

<div align="center">⁂</div>

[^1]: https://docs.spring.io/spring-kafka/api/org/springframework/kafka/core/KafkaTemplate.html

[^2]: https://spring-doc.cadn.net.cn/spring-kafka/3.2.4-SNAPSHOT/kafka_sending-messages.en.html

[^3]: https://stackoverflow.com/questions/59632296/kafkatemplate-send-method-safe-to-use-without-manual-blocking-check-of-returne

[^4]: https://docs.spring.io/spring-kafka/reference/kafka/transactions.html

[^5]: https://docs.spring.io/spring-kafka/docs/3.1.1/reference/kafka/transactions.html

[^6]: https://gunju-ko.github.io/kafka/spring-kafka/2018/03/31/Spring-KafkaTransaction.html

[^7]: https://github.com/cch0/spring-boot-kafka

[^8]: https://www.baeldung.com/kafka-exactly-once

[^9]: https://springbuilders.dev/raphaeldelio/chaining-kafka-and-database-transactions-30en

[^10]: https://github.com/spring-projects/spring-kafka/issues/1168

[^11]: https://chrzaszcz.dev/2019/12/kafka-transactions/

[^12]: https://www.geeksforgeeks.org/java/apache-kafka-linger-ms-and-batch-size/

[^13]: https://www.automq.com/blog/kafka-performance-tuning-linger-ms-batch-size

[^14]: https://www.codestudy.net/blog/kafka-exactly-once-producer/

[^15]: https://www.automq.com/blog/what-is-kafka-exactly-once-semantics

[^16]: https://docs.spring.io/spring-kafka/reference/kafka/sending-messages.html

[^17]: https://developer.confluent.io/courses/spring/send-messages/

[^18]: https://howtodoinjava.com/kafka/spring-boot-kafkatemplate/

[^19]: https://www.youtube.com/watch?v=0PTBG-3QQLY

[^20]: https://springdoc.tech/spring-kafka/kafka/sending-messages/

[^21]: https://github.com/spring-projects/spring-kafka/issues/227

