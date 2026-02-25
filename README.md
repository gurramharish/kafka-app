# Kafka Order Processing Application

A production-grade Spring Boot application for processing orders through Kafka with consistent hashing and comprehensive logging.

## Features

- **Producer**: Sends orders to `just-pay-orders` topic with consistent hashing
- **Consumer**: Processes orders with manual offset management
- **Logging**: Log4j2 with ThreadContext for offset tracking
- **REST API**: Endpoints to send orders
- **Production Configuration**: Optimized Kafka settings

## Quick Start

### Prerequisites
- Java 17+
- Maven 3.6+
- Docker & Docker Compose

### Start Kafka Infrastructure
```bash
docker-compose up -d broker
```

### Create Kafka Topic
```bash
docker exec broker kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --topic just-pay-orders \
  --partitions 3 --replication-factor 1
```

### Run Application
```bash
# Default profile (balanced performance)
mvn spring-boot:run

# With specific profile
mvn spring-boot:run -Dspring-boot.run.profiles=local

# With custom performance profile
APP_PRODUCER_PERFORMANCE_PROFILE=high-throughput mvn spring-boot:run
```

## API Endpoints

### Order Management

#### Send Full Order
```bash
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "orderId": "ORD-001",
    "customerId": "CUST-001",
    "amount": 99.99,
    "currency": "USD",
    "status": "PENDING",
    "paymentMethod": "CREDIT_CARD",
    "description": "Test order"
  }'
```

#### Send Simple Order
```bash
curl -X POST http://localhost:8080/api/orders/simple \
  -H "Content-Type: application/json" \
  -d '{
    "orderId": "ORD-002",
    "customerId": "CUST-002",
    "amount": 149.99
  }'
```

#### Generate Random Orders
```bash
# Generate 100 random orders (default)
curl -X POST http://localhost:8080/api/orders/generate

# Generate 500 random orders
curl -X POST "http://localhost:8080/api/orders/generate?count=500"

# Generate orders asynchronously (for large batches)
curl -X POST "http://localhost:8080/api/orders/generate-async?count=1000"
```

#### Get Order Statistics
```bash
curl -X GET http://localhost:8080/api/orders/stats
```

### Batch Processing

#### Publish All Pending Orders
```bash
curl -X POST http://localhost:8080/api/batch/publish
```

#### Get Batch Processing Progress
```bash
curl -X GET http://localhost:8080/api/batch/progress
```

#### Get Batch Statistics
```bash
curl -X GET http://localhost:8080/api/batch/stats
```

#### Get Batch Status
```bash
curl -X GET http://localhost:8080/api/batch/status
```

### Monitoring

#### Get Comprehensive Monitoring Stats
```bash
curl -X GET http://localhost:8080/api/monitoring/stats
```

#### Get Batch Processing Stats
```bash
curl -X GET http://localhost:8080/api/monitoring/batch-stats
```

#### Get Consumption Stats
```bash
curl -X GET http://localhost:8080/api/monitoring/consumption-stats
```

### Performance Management

#### Get Current Performance Profile
```bash
curl -X GET http://localhost:8080/api/performance/profile
```

#### Get Performance Recommendations
```bash
curl -X GET "http://localhost:8080/api/performance/recommendation?messageSize=1024&messageRate=5000&latencyCritical=false"
```

#### Get All Available Profiles
```bash
curl -X GET http://localhost:8080/api/performance/profiles
```

#### Switch Performance Profile
```bash
curl -X POST http://localhost:8080/api/performance/profile/switch \
  -H "Content-Type: application/json" \
  -d '{"profile": "high-throughput"}'
```

## API Response Examples

### Order Response
```json
{
  "orderId": "ORD-001",
  "status": "SENT",
  "partition": 2,
  "offset": 15,
  "timestamp": 1708503045123,
  "message": "Order sent successfully"
}
```

### Generation Response
```json
{
  "requested": 100,
  "generated": 100,
  "message": "Successfully generated orders"
}
```

### Order Statistics
```json
{
  "total": 1500,
  "pending": 250
}
```

### Batch Processing Result
```json
{
  "totalProcessed": 250,
  "successful": 245,
  "failed": 5,
  "message": "Batch processing completed"
}
```

### Batch Progress
```json
{
  "isProcessing": true,
  "activeBatches": 3,
  "remainingOrders": 150,
  "batchSize": 50,
  "status": "Processing batches..."
}
```

### Monitoring Statistics
```json
{
  "batchStats": {
    "activeBatches": 2,
    "pendingOrders": 100,
    "batchSize": 50,
    "totalProcessed": 500,
    "successful": 495,
    "failed": 5
  },
  "consumptionStats": {
    "totalConsumed": 495,
    "successful": 490,
    "failed": 5,
    "lastConsumedTimestamp": 1708503045123
  }
}
```

### Performance Profile
```json
{
  "activeProfile": "balanced",
  "timestamp": 1708503045123
}
```

### Performance Recommendation
```json
{
  "messageSize": 1024,
  "messageRate": 5000,
  "latencyCritical": false,
  "recommendedProfile": "balanced",
  "explanation": "Balanced profile recommended for general use with 5000 messages/sec and 1024 byte messages. Good compromise between throughput and latency."
}
```

## Key Features

### Consistent Hashing
- Orders with same `orderId` always go to same partition
- Ensures ordering for the same order ID
- Custom partitioner: `ConsistentHashPartitioner`

### Logging with ThreadContext
- Log pattern includes: `[topic-partition-offset]`
- Example: `2024-02-21 10:30:45.123 [just-pay-orders-2-15] [kafka-listener-0] INFO  OrderConsumerService - Received order: orderId=ORD-001`

### Production Configuration
- Manual offset commits
- Cooperative sticky partition assignment
- Idempotent producer
- Compression (LZ4)
- Retry mechanisms

## Monitoring

### Consumer Group Status
```bash
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group order-consumer-group --describe
```

### Topic Details
```bash
kafka-topics --describe --topic just-pay-orders --bootstrap-server localhost:9092
```

## Performance Configuration

The application supports **profile-based performance tuning** for the Kafka producer. Configuration is managed through Spring profiles and environment variables.

### Available Profiles

#### Production Profile (`prod`)
Optimized for high throughput and reliability:
```yaml
spring.kafka.producer:
  batch-size: 65536        # 64KB batches
  linger-ms: 20            # 20ms wait for batching
  buffer-memory: 67108864  # 64MB buffer
  compression-type: lz4
  max-in-flight-requests-per-connection: 5
  delivery-timeout-ms: 120000  # 2 minutes
```

#### Development Profile (`dev`)
Balanced for development with moderate performance:
```yaml
spring.kafka.producer:
  batch-size: 8192         # 8KB batches
  linger-ms: 2             # 2ms wait
  buffer-memory: 16777216   # 16MB buffer
  compression-type: none   # No compression for debugging
  delivery-timeout-ms: 30000  # 30 seconds
```

#### Local Profile (`local`)
Optimized for minimal latency in local development:
```yaml
spring.kafka.producer:
  batch-size: 4096         # 4KB batches
  linger-ms: 0             # No waiting (immediate send)
  buffer-memory: 8388608   # 8MB buffer
  compression-type: none   # No compression
  max-in-flight-requests-per-connection: 1
  delivery-timeout-ms: 5000   # 5 seconds
```

### Running with Different Profiles

```bash
# Production profile
mvn spring-boot:run -Dspring-boot.run.profiles=prod

# Development profile
mvn spring-boot:run -Dspring-boot.run.profiles=dev

# Local profile
mvn spring-boot:run -Dspring-boot.run.profiles=local

# Multiple profiles (e.g., prod with custom performance)
mvn spring-boot:run -Dspring-boot.run.profiles=prod,custom
```

### Environment Variable Override

You can override the performance profile using environment variables:

```bash
# Set performance profile via environment
export APP_PRODUCER_PERFORMANCE_PROFILE=high-throughput
mvn spring-boot:run

# Or inline
APP_PRODUCER_PERFORMANCE_PROFILE=balanced mvn spring-boot:run
```

### Performance Trade-offs

| Setting | High Throughput | Low Latency | Balanced |
|---------|----------------|-------------|----------|
| `batch-size` | 64KB | 4KB | 16KB |
| `linger-ms` | 20ms | 0ms | 2ms |
| `buffer-memory` | 64MB | 8MB | 16MB |
| `compression` | lz4 | none | none |
| Use Case | Batch processing | Real-time | Development |

## Configuration

Key configurations in `application.yml`:
- Producer: `acks: all`, `enable.idempotence: true`
- Consumer: `enable-auto-commit: false`, `ack-mode: manual`
- Custom partitioner for consistent hashing

## Testing Consistent Hashing

Send multiple orders with same `orderId`:
```bash
for i in {1..5}; do
  curl -X POST http://localhost:8080/api/orders/simple \
    -H "Content-Type: application/json" \
    -d '{
      "orderId": "ORD-SAME",
      "customerId": "CUST-001",
      "amount": 100.00
    }'
done
```

All messages will go to the same partition!

## Local Development Setup

### Complete Setup Commands
```bash
# 1. Start Kafka infrastructure
docker-compose up -d broker

# 2. Wait for Kafka to be ready (optional check)
docker exec broker kafka-broker-api-versions --bootstrap-server localhost:9092

# 3. Create the topic
docker exec broker kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --topic just-pay-orders \
  --partitions 3 --replication-factor 1

# 4. Run the application with local profile
mvn spring-boot:run -Dspring-boot.run.profiles=local
```

### Development Tools

#### Monitoring with Control Center
```bash
# Start the full Confluent stack (includes Control Center)
docker-compose up -d

# Access Control Center at http://localhost:9021
# Monitor topics, consumer groups, and message throughput
```

#### Schema Registry
Available at `http://localhost:8081` for managing Avro schemas (if needed).

#### Kafka REST Proxy
Access Kafka via REST API at `http://localhost:8082`.

### Environment Variables for Development
```bash
# Override Kafka bootstrap servers
export SPRING_KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Set performance profile
export APP_PRODUCER_PERFORMANCE_PROFILE=local

# Enable debug logging
export LOGGING_LEVEL_COM_EXAMPLE_KAFKA=DEBUG
```

### Build and Run
```bash
# Build the project
mvn clean compile

# Run tests
mvn test

# Package the application
mvn clean package

# Run the JAR directly
java -jar target/kafka-demo-0.0.1-SNAPSHOT.jar --spring.profiles.active=local
```

## Troubleshooting

### Common Issues

#### Kafka Connection Failed
```bash
# Check if Kafka is running
docker ps | grep broker

# Check Kafka logs
docker logs broker

# Test Kafka connectivity
docker exec broker kafka-topics --bootstrap-server localhost:9092 --list
```

#### Topic Not Found
```bash
# Create topic manually
docker exec broker kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --topic just-pay-orders \
  --partitions 3 --replication-factor 1

# Verify topic exists
docker exec broker kafka-topics --describe --topic just-pay-orders --bootstrap-server localhost:9092
```

#### Consumer Group Issues
```bash
# Check consumer group status
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group order-consumer-group --describe

# Reset consumer offsets (if needed)
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group order-consumer-group --reset-offsets \
  --to-earliest --topic just-pay-orders --execute
```

#### Port Conflicts
If ports are already in use:
```bash
# Check what's using port 8080
lsof -i :8080

# Or change application port
export SERVER_PORT=8081
mvn spring-boot:run
```

### Performance Tuning Tips

#### For High Throughput
- Use `prod` profile
- Increase `batch-size` and `linger-ms`
- Enable compression (`lz4`)
- Monitor consumer lag

#### For Low Latency
- Use `local` profile
- Set `linger-ms: 0`
- Disable compression
- Reduce `batch-size`

#### Monitoring Performance
```bash
# Monitor producer metrics
curl -s http://localhost:8080/actuator/metrics | jq '.[] | select(.name | contains("kafka.producer"))'

# Monitor consumer lag
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group order-consumer-group --describe --offsets
```
