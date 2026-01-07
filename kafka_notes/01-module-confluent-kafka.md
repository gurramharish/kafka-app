# Module 1 – Kafka Fundamentals with Confluent Platform (cp-all-in-one)

> Goal: Understand Kafka core concepts (brokers, topics, partitions, replication, producers, consumers, consumer groups, offsets, KRaft) **and** get a full Confluent Platform running locally with hands‑on exercises.

---

## 1. Theory – Kafka Fundamentals

### 1.1 Kafka as a Distributed Commit Log

Kafka is not just a "message queue"; it is a **distributed, replicated commit log**.

- **Log**: An append-only sequence of records ordered by offset.
- **Distributed**: Data is partitioned across multiple brokers for scalability.
- **Replicated**: Partitions are copied to multiple brokers for fault-tolerance.

Key properties:
- Messages are **durable** (stored on disk, replicated).
- Consumers can **re-read** data from any point using offsets.
- Many independent consumer groups may read the same data.

---

### 1.2 Brokers, Cluster, and KRaft

- **Broker**: A single Kafka server (process). Stores topic partitions on disk and serves reads/writes.
- **Cluster**: A set of brokers working together.
- **Controller (KRaft)**: Special role elected among brokers that manages cluster metadata using a Raft consensus protocol.

Modern Kafka (3.5+) typically runs in **KRaft mode**:
- No external ZooKeeper.
- Metadata (topics, ACLs, configs) stored in internal topic `__cluster_metadata`.
- One or more brokers act as **controllers**; others are **brokers-only**.

Core ideas:
- Each broker has a **broker.id** (numeric identifier).
- Clients connect via **bootstrap servers** (e.g. `localhost:9092`).
- Controllers coordinate partition leadership, broker membership, and configuration.

---

### 1.3 Topics and Partitions

- **Topic**: A named logical stream of records (e.g. `orders`, `payments`).
- **Partition**: A **subset** of a topic – a totally ordered log. Topics consist of one or more partitions.

Why partitions?
- **Scalability / Throughput**: Producers and consumers can work in parallel on different partitions.
- **Ordering**: Kafka only guarantees ordering **within a partition**, not across the whole topic.

Design rules:
- More partitions ⇒ more parallelism but more overhead (files, open handles, replication traffic).
- Too few partitions ⇒ limited throughput and limited consumer parallelism.

---

### 1.4 Replication Factor and In-Sync Replicas (ISR)

Each partition is replicated **RF** times across brokers.

- **Replication Factor (RF)**: Number of copies of a partition.
- **Leader**: Broker that handles all reads and writes for that partition.
- **Follower(s)**: Brokers that replicate data from the leader.
- **In-Sync Replicas (ISR)**: Set of replicas that are fully caught up with the leader.

Trade-offs:
- RF = 1
  - Only one copy, **no fault tolerance**.
  - Fastest, least disk usage.
- RF = 2 or 3
  - Survive 1 (or 2) broker failures.
  - More durable but more network+disk overhead.

Rule: **RF ≤ number of brokers**. If RF is larger than live brokers, topic creation fails.

---

### 1.5 Producers, Keys, and Partitioning

- **Producer**: Sends records to a topic.
- **Record**: (key, value, headers, timestamp).
- **Key**: Used to determine which partition a message goes to (usually via hash(key) % numPartitions).

Behaviors:
- With **key**: Records with same key go to same partition → per-key ordering.
- Without key: Records distributed round-robin among partitions.

Important configs:
- `acks` (0, 1, all): Durability vs latency trade-off.
- `batch.size`, `linger.ms`: Impact throughput and latency.

---

### 1.6 Consumers, Offsets, and Consumer Groups

- **Consumer**: Reads records from partitions.
- **Offset**: Monotonic index of a record within a partition.
- **Consumer Group**: Named group of one or more consumers that coordinate to consume a topic.

Rules:
- Each partition in a topic is consumed by **at most one** consumer **within the same group**.
- Different groups consume independently (like separate applications).
- Kafka stores **committed offsets** per `(group, topic, partition)`.

Scenarios:
- 3 partitions, 1 consumer in group ⇒ that consumer gets all partitions.
- 3 partitions, 3 consumers in the same group ⇒ each consumer gets one partition.
- 3 partitions, 4 consumers ⇒ one consumer will be idle (3 active, 1 idle).

---

### 1.7 Rebalancing

When a consumer joins or leaves a group, or when partitions change, Kafka triggers a **rebalance**:
- Partitions are reassigned among consumers in the group.
- Causes a short pause in consumption.
- Consumers receive new assignments and resume reading.

Rebalance triggers:
- New consumer joins the group.
- Existing consumer crashes or closes.
- Topic partitions are added/removed.
- Broker failures or leadership changes.

---

### 1.8 Schema Registry and Data Contracts

In event-driven systems, **schemas** define the structure of messages.

**Schema Registry**:
- Stores schemas for topics (`<topic>-value`, `<topic>-key`).
- Supports Avro, Protobuf, JSON Schema.
- Manages schema **versions** and compatibility (backward, forward, full, etc.).

Benefits:
- Producers and consumers share a stable **contract** for data.
- Prevents breaking changes when evolving fields.
- Enables small, efficient encodings (e.g. Avro with schema IDs).

---

### 1.9 Why KRaft Replaces ZooKeeper

Old Kafka used **ZooKeeper** for cluster metadata.
Modern versions support **KRaft** (Kafka Raft):
- Eliminates external ZooKeeper dependency.
- Uses an internal Raft quorum of controllers.
- Stores metadata in Kafka itself (`__cluster_metadata`).
- Simplifies operations and deployment.

For new clusters, **KRaft is the recommended mode**.

---

## 2. Confluent Platform Setup (cp-all-in-one)

We use Confluent's `cp-all-in-one` repo to bring up a full local environment:
- Kafka broker
- Schema Registry
- Kafka Connect
- ksqlDB
- Control Center
- REST Proxy

> Prerequisites: Docker & docker compose (or `docker compose` v2) installed.

### 2.1 Clone cp-all-in-one

```bash
git clone https://github.com/confluentinc/cp-all-in-one.git
cd cp-all-in-one

# Optionally checkout a stable tag, e.g. 7.5.x or 8.x
git checkout 7.5.0-post   # example; use latest available

cd cp-all-in-one
```

### 2.2 Start the Stack

```bash
# Start all services in the background
docker compose up -d

# Check status of all services
docker compose ps
```

Expected (container names may vary):
- `broker` – Kafka broker
- `schema-registry`
- `connect`
- `ksqldb-server`
- `ksqldb-cli`
- `control-center`
- `rest-proxy`

Wait ~2–3 minutes for everything to become healthy.

### 2.3 Access Control Center

```text
http://localhost:9021
```

From here you can:
- View **cluster** and **broker** metrics.
- Create and manage **topics**.
- Monitor **consumer groups**.
- Configure **Kafka Connect** connectors (including Datagen).
- Inspect **schemas** via UI.

### 2.4 Ports Quick Reference

| Component          | Port   | Description                      |
|--------------------|--------|----------------------------------|
| Kafka Broker       | 9092   | Client bootstrap (PLAINTEXT)    |
| Schema Registry    | 8081   | HTTP API for schemas            |
| REST Proxy         | 8082   | HTTP → Kafka bridge             |
| Kafka Connect      | 8083   | Connect REST API                |
| ksqlDB Server      | 8088   | ksqlDB API                      |
| Control Center     | 9021   | Web UI                          |

### 2.5 Container Names

In the default `cp-all-in-one` compose file, common service names:
- `broker`: Kafka broker container.
- `connect`: Kafka Connect worker.
- `schema-registry`: Schema Registry service.
- `control-center`: Confluent Control Center.

These names are used with `docker exec` in exercises.

---

## 3. CLI Tools in Confluent Images (Important)

Confluent images expose CLI tools **without** the `.sh` extension.

| Task               | Apache (tarball)          | Confluent Docker          |
|--------------------|---------------------------|---------------------------|
| Topics             | `kafka-topics.sh`         | `kafka-topics`           |
| Console producer   | `kafka-console-producer.sh` | `kafka-console-producer` |
| Console consumer   | `kafka-console-consumer.sh` | `kafka-console-consumer` |
| Consumer groups    | `kafka-consumer-groups.sh`  | `kafka-consumer-groups`  |
| Broker API versions| `kafka-broker-api-versions.sh` | `kafka-broker-api-versions` |

All commands below use the Confluent style (no `.sh`).

---

## 4. Module 1 – Hands-On Exercises

This section ties the theory to practical commands against your local cp-all-in-one cluster.

### 4.1 Exercise 1 – Explore Cluster Topology

**Goal**: Understand brokers and controllers.

#### 4.1.1 Using Control Center

1. Open `http://localhost:9021`.
2. Click the cluster name (e.g. `kafka-cluster`).
3. Inspect **Brokers** tab:
   - Broker ID (usually `1` in single-broker dev setup).
   - Listener addresses (e.g. `PLAINTEXT://broker:29092`).
4. Note the **controller** indicator (which broker acts as controller).

#### 4.1.2 Using CLI

```bash
# Show broker API versions and confirm broker is reachable
docker exec broker kafka-broker-api-versions \
  --bootstrap-server localhost:9092
```

---

### 4.2 Exercise 2 – Create Topics and Partitions

**Goal**: See how partitions work and how to inspect them.

#### 4.2.1 Create Topics

```bash
# Topic with 1 partition
docker exec broker kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --topic single-partition-topic \
  --partitions 1 --replication-factor 1

# Topic with 3 partitions
docker exec broker kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --topic orders \
  --partitions 3 --replication-factor 1

# Topic with 6 partitions
docker exec broker kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --topic high-throughput-topic \
  --partitions 6 --replication-factor 1
```

#### 4.2.2 List and Describe Topics

```bash
# List all topics
docker exec broker kafka-topics \
  --bootstrap-server localhost:9092 --list

# Describe one topic
docker exec broker kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe --topic orders
```

Look for:
- Partition count.
- Leader broker per partition.
- ISR list.

---

### 4.3 Exercise 3 – Replication Factor Check

**Goal**: Understand replication factor constraints.

In a single-broker cp-all-in-one environment, trying RF=3 will fail (as expected).

```bash
# Attempt to create RF=3 topic (with 1 broker)
docker exec broker kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --topic high-availability-topic \
  --partitions 3 --replication-factor 3
```

Expected error:
> `Replication factor: 3 larger than available brokers: 1`  
This demonstrates RF ≤ broker count.

---

### 4.4 Exercise 4 – Produce Messages

**Goal**: Write data into Kafka.

#### 4.4.1 Via CLI Producer

```bash
# Start interactive producer
docker exec -it broker kafka-console-producer \
  --broker-list localhost:9092 \
  --topic orders
```

Type a few JSON lines and press **Enter** after each:
```text
{"orderId": 1, "customerId": 100, "amount": 99.99}
{"orderId": 2, "customerId": 101, "amount": 149.99}
{"orderId": 3, "customerId": 102, "amount": 79.99}
```

Exit with **Ctrl+D**.

#### 4.4.2 Via Datagen Connector (Optional)

Using Control Center → Connect → `connect-default` → **DatagenConnector**:
- Set topic to e.g. `test-topic`.
- Set `quickstart` to `users`.
- Launch connector to auto-generate data.

Then inspect `test-topic` messages in **Topics → test-topic → Messages**.

---

### 4.5 Exercise 5 – Consume Messages and Offsets

**Goal**: Learn consumer groups and offset behavior.

#### 4.5.1 First Consumption (from-beginning)

```bash
# Consume orders with group "my-group"
docker exec broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group my-group \
  --from-beginning
```

You should see the messages you produced.

Exit with **Ctrl+C**.

#### 4.5.2 Second Run – No New Messages

Run the same command again:

```bash
docker exec broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group my-group \
  --from-beginning
```

If no new messages were produced, you will see **no output**. The group has already consumed all records and committed offsets.

#### 4.5.3 Inspect Consumer Group

```bash
# List groups
docker exec broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 --list

# Describe one group
docker exec broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group my-group --describe
```

Observe:
- `CURRENT-OFFSET`
- `LOG-END-OFFSET`
- `LAG` (should be 0 if caught up)

---

### 4.6 Exercise 6 – Rebalancing with Multiple Consumers

**Goal**: Observe partition rebalancing in a group.

#### 4.6.1 First Consumer

Terminal 1:
```bash
docker exec -it broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group rebalance-group
```

#### 4.6.2 Second Consumer

Terminal 2:
```bash
docker exec -it broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group rebalance-group
```

You will notice:
- Short pause (rebalance).
- Then both terminals receive data, each handling a subset of partitions.

Stop one of them (Ctrl+C) and watch another rebalance occur.

---

### 4.7 Exercise 7 – Schema Registry Integration

**Goal**: Inspect and register schemas.

#### 4.7.1 List Subjects

```bash
curl http://localhost:8081/subjects
```

Example output:
```json
["test-topic-value","orders-value"]
```

#### 4.7.2 Get Latest Schema for a Subject

```bash
curl http://localhost:8081/subjects/test-topic-value/versions/latest
```

#### 4.7.3 Register Custom Schema

```bash
curl -X POST http://localhost:8081/subjects/orders-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"orderId\",\"type\":\"int\"},{\"name\":\"customerId\",\"type\":\"int\"},{\"name\":\"amount\",\"type\":\"double\"}]}"
  }'
```

Response includes a schema ID (e.g. `{ "id": 1 }`).

---

### 4.8 Exercise 8 – Verify KRaft Mode

**Goal**: Confirm cluster is not using ZooKeeper.

```bash
# Look for KRaft-related log entries
docker logs broker | grep -i "kraft\|controller"
```

You should see references to **KRaft** or **controller quorum**, and **no** `zookeeper.connect` configuration.

---

### 4.9 Exercise 9 – Monitor Cluster Health

**Goal**: Use Control Center metrics.

In Control Center:
- `Cluster → Brokers` – Check traffic metrics.
- `Topics → orders` – Look at throughput, partition distribution.
- `Consumer Groups → my-group` – Check lag and assignment.

These metrics are critical in production to detect slow consumers, under-replicated partitions, etc.

---

### 4.10 Exercise 10 – Clean Up and Review

#### 4.10.1 List and Delete Topics (Optional)

```bash
# List
docker exec broker kafka-topics --bootstrap-server localhost:9092 --list

# Delete sample topics
docker exec broker kafka-topics --bootstrap-server localhost:9092 --delete --topic orders

docker exec broker kafka-topics --bootstrap-server localhost:9092 --delete --topic single-partition-topic

docker exec broker kafka-topics --bootstrap-server localhost:9092 --delete --topic high-throughput-topic
```

#### 4.10.2 Self-Check Questions

1. Difference between a **broker** and a **controller**?
2. Why do topics have **partitions** and what trade-offs come with more partitions?
3. Why must replication factor be ≤ number of brokers?
4. How do **consumer groups** enable parallelism and independent consumption?
5. What triggers a **rebalance**?
6. What problem does **Schema Registry** solve?
7. Why is **KRaft** preferred over ZooKeeper in new deployments?
8. What are **current offset**, **log end offset**, and **lag**?

If you can answer these confidently and have run all the commands above, you have effectively completed **Module 1 – Kafka Fundamentals with Confluent Platform**.

---

## 5. Stopping the Environment

When you are done:

```bash
# Stop and remove all cp-all-in-one containers
docker compose down

# Optionally remove volumes (be careful – removes data)
docker compose down -v
```

You now have:
- A full local Confluent Platform environment.
- Practical experience with topics, partitions, replication, producers, consumers, groups, Schema Registry, and KRaft.
- A solid foundation for deeper modules (topic design, Streams, ksqlDB, Connect, etc.).
