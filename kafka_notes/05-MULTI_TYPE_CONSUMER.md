# Multi-Type Kafka Consumer Configuration

This document explains different approaches for handling multiple value types across Kafka topics.

## **Problem Statement**
When consuming from different Kafka topics with different message types, the default JSON deserializer configuration:
```java
configProps.put(JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, "com.example.kafka.model.Order");
```
forces ALL messages to be deserialized as `Order` objects, causing deserialization failures for other types.

## **Solution Approaches**

### **Approach 1: Multiple ConsumerFactories (Recommended)**
Create specific ConsumerFactories for each message type:

```java
// Generic factory for Object types
@Bean
public ConsumerFactory<String, Object> consumerFactory() {
    // No VALUE_DEFAULT_TYPE - handles any JSON object
}

// Specific factory for Order objects  
@Bean
public ConsumerFactory<String, Order> orderConsumerFactory() {
    configProps.put(JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, "com.example.kafka.model.Order");
}

// Corresponding container factories
@Bean
public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
    factory.setConsumerFactory(consumerFactory());
}

@Bean
public ConcurrentKafkaListenerContainerFactory<String, Order> orderKafkaListenerContainerFactory() {
    factory.setConsumerFactory(orderConsumerFactory());
}
```

**Usage:**
```java
@KafkaListener(containerFactory = "orderKafkaListenerContainerFactory")
public void consumeOrder(Order order) { ... }

@KafkaListener(containerFactory = "kafkaListenerContainerFactory") 
public void consumeGeneric(Object message) { ... }
```

### **Approach 2: Type Headers (Advanced)**
Producers include type information in message headers:

```java
// Producer
kafkaTemplate.send(topic, message, 
    new RecordHeaders().add("message-type", "order".getBytes()));

// Consumer
@Header("message-type") String messageType,
@Payload Object message

if ("order".equals(messageType) && message instanceof Order) {
    Order order = (Order) message;
}
```

### **Approach 3: Generic Consumer with Type Checking**
Single consumer handles multiple types with runtime checking:

```java
@KafkaListener(containerFactory = "kafkaListenerContainerFactory")
public void consumeMessage(Object message) {
    if (message instanceof Order order) {
        processOrder(order);
    } else if (message instanceof String str) {
        processString(str);
    } else if (message instanceof Map map) {
        processMap(map);
    }
}
```

## **Current Implementation**

Our application now supports:

1. **OrderConsumerService** - Uses `orderKafkaListenerContainerFactory` for Order objects
2. **GenericConsumerService** - Uses `kafkaListenerContainerFactory` for any Object type

## **Configuration Summary**

```yaml
# Multiple consumer groups for different processing
spring:
  kafka:
    consumer:
      group-id: order-consumer-group  # Default group
```

## **Best Practices**

1. **Use specific ConsumerFactories** for known types (Orders, Payments, etc.)
2. **Use generic ConsumerFactory** for unknown or mixed types
3. **Include type headers** for complex routing scenarios
4. **Separate consumer groups** for different processing logic
5. **Handle deserialization exceptions** gracefully

## **Example Usage**

```java
// Order-specific consumer
@KafkaListener(
    topics = "orders",
    containerFactory = "orderKafkaListenerContainerFactory"
)
public void handleOrder(Order order) {
    // Type-safe Order processing
}

// Generic consumer for other topics
@KafkaListener(
    topics = "events,notifications",
    containerFactory = "kafkaListenerContainerFactory"
)
public void handleEvent(Object event) {
    // Runtime type checking and processing
}
```
