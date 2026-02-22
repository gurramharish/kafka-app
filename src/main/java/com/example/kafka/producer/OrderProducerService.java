package com.example.kafka.producer;

import com.example.kafka.model.Order;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProducerService {
    
    private final KafkaTemplate<String, Object> kafkaTemplate;
    
    @Value("${app.kafka.topics.orders}")
    private String ordersTopic;
    
    /**
     * Send order to Kafka topic with consistent hashing
     * Uses orderId as partition key to ensure same order goes to same partition
     */
    public CompletableFuture<SendResult<String, Object>> sendOrder(Order order) {
        log.info("Sending order to topic: {}, orderId: {}, customerId: {}", 
                ordersTopic, order.orderId(), order.customerId());
        
        // Use orderId as partition key for consistent hashing
        String partitionKey = order.getPartitionKey();
        
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(
            ordersTopic, 
            partitionKey, 
            order
        );
        
        return future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to send order {}: {}", order.orderId(), ex.getMessage(), ex);
            } else {
                log.info("Successfully sent order {} to partition {} with offset {}", 
                        order.orderId(), 
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
    
    /**
     * Send order synchronously (blocking)
     */
    public SendResult<String, Object> sendOrderSync(Order order) {
        try {
            log.info("Sending order synchronously: {}", order.orderId());
            return sendOrder(order).get();
        } catch (Exception ex) {
            log.error("Failed to send order synchronously: {}", order.orderId(), ex);
            throw new RuntimeException("Failed to send order", ex);
        }
    }
}
