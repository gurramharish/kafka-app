# Kafka Schema Registry: How Schemas Are Created and Used

## 1. What Schema Registry Is

Schema Registry is a separate service (usually at `http://schema-registry:8081`) that stores **versioned schemas** for Kafka messages and enforces **compatibility rules** when those schemas evolve.[cite:177][cite:181]

Key points:
- Central, versioned store for Avro / Protobuf / JSON Schema definitions.[cite:177]
- Exposes a **REST API** to register, fetch, and validate schemas.[cite:181]
- Works together with special **serializers/deserializers (SerDes)** in Kafka clients (Java, .NET, Python, etc.).[cite:177]
- Optimizes payloads by sending only a **schema ID** in each message instead of the full schema.[cite:177][cite:182]

---

## 2. Core Concepts

### 2.1 Subjects

A **subject** is the logical name under which schemas are registered. Common subject name strategies:

- **Topic-based (default):**
  - `<topic>-value` for message values
  - `<topic>-key` for message keys
- Key and value of the same topic typically use **separate subjects**.

Example:
- Topic: `orders`
- Subjects:
  - `orders-key`
  - `orders-value`

### 2.2 Versions and IDs

For each subject, Schema Registry maintains **versions**:

- First schema you register under a subject becomes **version 1**.
- Next compatible change becomes **version 2**, and so on.[cite:178][cite:181]
- Each schema (content) also gets a **global integer ID**. This ID is what is sent on the wire with messages.[cite:182][cite:187]

### 2.3 Compatibility Modes

Compatibility defines which schema changes are allowed compared to previous versions:[cite:177][cite:184]

- **BACKWARD:** New schema can read data written with older schemas (upgrade consumers first).
- **FORWARD:** Old schema can read data written with newer schemas (upgrade producers first).
- **FULL:** Both backward and forward; safest, most restrictive.
- **NONE:** No compatibility checks.

Typical allowed changes (for Avro in BACKWARD/FULL):[cite:184]
- Add a **new optional field** (with default).
- Remove an **optional** field.
- Disallow: removing required fields, changing field types, renaming fields (breaking).

Compatibility is configured globally or per subject.

---

## 3. How Schemas Are Created and Registered

You usually write schemas in **Avro**, **JSON Schema**, or **Protobuf**. Then you register them in two main ways:

1. **Programmatically via serializers (preferred).**
2. **Manually via Schema Registry REST API (curl, Postman, etc.).**

### 3.1 Example Avro Schema (Order Event)

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.example.avro",
  "fields": [
    { "name": "orderId", "type": "string" },
    { "name": "customerId", "type": "string" },
    { "name": "amount", "type": "double" },
    { "name": "createdAt", "type": { "type": "long", "logicalType": "timestamp-millis" } }
  ]
}
```

Save this to a file, for example: `order.avsc`.

### 3.2 Registering a Schema Manually via REST

Schema Registry REST API to register a new schema version for subject `orders-value`:[cite:183][cite:188]

```bash
jq '. | {schema: tojson}' order.avsc | \
  curl -X POST http://localhost:8081/subjects/orders-value/versions \
       -H "Content-Type: application/json" \
       -d @-
```

Simpler inline form (small schema):

```bash
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"orderId\",\"type\":\"string\"}]}"}' \
  http://localhost:8081/subjects/orders-value/versions
```

Response:

```json
{"id": 1}
```

- `id` is the **schema ID** used in messages.

### 3.3 Programmatic Registration via SerDes

With **Confluent serializers**, you usually **do not call the REST API yourself**:

- Producer holds a schema (from generated class or schema file).
- Serializer contacts Schema Registry, auto-registers the schema **if not present**, or validates compatibility if it is.
- Schema Registry returns the **schema ID**; serializer embeds it in the message payload.[cite:177][cite:181]

Config for a Java producer using Avro serializer (example):[cite:177][cite:186]

```properties
key.serializer=org.apache.kafka.common.serialization.StringSerializer
value.serializer=io.confluent.kafka.serializers.KafkaAvroSerializer
schema.registry.url=http://schema-registry:8081
# Optional: auto register or not
value.subject.name.strategy=io.confluent.kafka.serializers.subject.TopicNameStrategy
```

---

## 4. Wire Format: How Schemas Are Used on the Wire

When using Confluent Avro / Protobuf / JSON Schema serializers, each message value is encoded as:[cite:182][cite:187]

| Bytes      | Field        | Description                                      |
|-----------:|--------------|--------------------------------------------------|
| 0          | Magic Byte   | Always `0` for Confluent format version 1       |
| 1–4        | Schema ID    | 4‑byte integer from Schema Registry             |
| 5…end      | Payload      | Binary Avro/Protobuf/JSON-SR encoded message    |

This means:
- The **schema itself is NOT sent every time**.
- Only the **schema ID** + binary data are sent.
- Consumer uses the ID to fetch the schema from Schema Registry and then deserialize safely.[cite:182][cite:177]

---

## 5. How Producers Use Schema Registry

### 5.1 High-Level Flow

1. Application creates a strongly-typed object (e.g. an Avro `Order` record).
2. Kafka producer uses a **Schema Registry–aware serializer**.
3. Serializer:
   - Extracts the schema from the object.
   - Checks if this schema is already registered under the subject (for example, `orders-value`).[cite:177]
   - If not registered:
     - Sends a `POST /subjects/{subject}/versions` request to Schema Registry with the schema.
     - Registry validates **compatibility** with previous version (if any). If incompatible, it returns HTTP 409 (rejected).[cite:177][cite:184]
   - Gets back a **schema ID** from Schema Registry (existing or new).[cite:183]
   - Writes the message in Confluent wire format: `magicByte + schemaId + serializedPayload`.[cite:182][cite:187]
4. Producer sends the serialized bytes to the Kafka topic.

### 5.2 Pseudo-Code (Java Avro Producer)

```java
Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          org.apache.kafka.common.serialization.StringSerializer.class);
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          io.confluent.kafka.serializers.KafkaAvroSerializer.class);
props.put("schema.registry.url", "http://localhost:8081");

Producer<String, Order> producer = new KafkaProducer<>(props);

Order order = Order.newBuilder()
    .setOrderId("O-123")
    .setCustomerId("C-42")
    .setAmount(99.99)
    .build();

ProducerRecord<String, Order> record =
    new ProducerRecord<>("orders", order.getOrderId(), order);

producer.send(record);
producer.flush();
producer.close();
```

- `KafkaAvroSerializer` handles schema registration and ID lookup transparently.[cite:181][cite:186]

---

## 6. How Consumers Use Schema Registry

### 6.1 High-Level Flow

1. Consumer polls the topic and receives **raw bytes**.
2. The **deserializer** reads:
   - Byte 0 → magic byte (should be `0`).
   - Bytes 1–4 → schema ID.
3. Deserializer queries Schema Registry:
   - `GET /schemas/ids/{id}` to fetch the writer schema.[cite:177][cite:183]
4. Deserializer uses the fetched schema (and reader schema, if different) to **decode** the binary payload into an object.
5. Your consumer code sees a fully-typed object (e.g. Avro `Order`) and processes it.

You **do not manually parse bytes** when using the official SerDes.

### 6.2 Pseudo-Code (Java Avro Consumer)

```java
Properties props = new Properties();
props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
          org.apache.kafka.common.serialization.StringDeserializer.class);
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
          io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
props.put("schema.registry.url", "http://localhost:8081");
props.put(ConsumerConfig.GROUP_ID_CONFIG, "orders-service");
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);
consumer.subscribe(Collections.singletonList("orders"));

while (true) {
    ConsumerRecords<String, Order> records = consumer.poll(Duration.ofMillis(500));
    for (ConsumerRecord<String, Order> rec : records) {
        Order order = rec.value();
        // business logic here
    }
}
```

`KafkaAvroDeserializer` hides Schema Registry lookups and schema evolution logic.[cite:181][cite:186]

---

## 7. Schema Evolution in Practice

When you change your schema, Schema Registry enforces the configured compatibility mode.[cite:177][cite:184]

### 7.1 Backward-Compatible Change Example

Original schema:

```json
{
  "type": "record",
  "name": "Order",
  "fields": [
    { "name": "orderId", "type": "string" },
    { "name": "amount", "type": "double" }
  ]
}
```

New schema (add optional field with default):

```json
{
  "type": "record",
  "name": "Order",
  "fields": [
    { "name": "orderId", "type": "string" },
    { "name": "amount", "type": "double" },
    { "name": "currency", "type": "string", "default": "USD" }
  ]
}
```

- This is **backward compatible**: new consumers can read old messages (missing `currency` becomes `"USD"`).[cite:184]
- Registry allows registration as next version for subject `orders-value`.

### 7.2 Incompatible Change Example

- Removing `orderId` or changing its type from `string` to `int` is **not backward compatible**.
- In FULL/BACKWARD mode, Schema Registry rejects this new schema with HTTP 409.[cite:184]

---

## 8. Typical Workflow Summary

### 8.1 Producer Side

1. Define schema (Avro / Protobuf / JSON Schema).
2. Generate language-specific classes (optional, but common for Avro/Protobuf).
3. Configure producer with Schema Registry–aware serializer.
4. When sending first message with a new schema:
   - Serializer registers schema or verifies it.
   - Schema Registry assigns/returns schema ID.
5. All subsequent messages reference **only the schema ID**.

### 8.2 Consumer Side

1. Configure consumer with Schema Registry–aware deserializer.
2. Deserializer reads magic byte + schema ID from each record.
3. Fetches schema from Schema Registry if not cached.
4. Deserializes bytes → concrete object.
5. Your code operates on typed data, oblivious to wire format.

---

## 9. Why Use Schema Registry?

Benefits:[cite:177][cite:186]

- **Strong contracts** between producers and consumers.
- **Safer schema evolution** with compatibility checks.
- **Central visibility** into all schemas and their versions.
- **Smaller messages** by referencing schemas via IDs.
- Easier **multi-team** and **polyglot** (Java, .NET, Python, etc.) integration.

Without Schema Registry, each service must:
- Hard-code or distribute schemas manually.
- Handle breaking changes and evolution on its own.
- Risk corrupt or unreadable data when formats drift.

---

## 10. Mental Model

- Schema Registry = **API management for your event data**.[cite:177]
- Schemas = **interfaces**; topics = **endpoints**.
- Producers = **API writers**, consumers = **API clients**.
- Compatibility = **versioning rules**.
- Serializers/deserializers = **codecs** that implement the contract.

Keep this model in your head when designing events: treat schema changes as carefully as you treat changes to public APIs.
