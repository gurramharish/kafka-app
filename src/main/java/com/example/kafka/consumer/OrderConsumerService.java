package com.example.kafka.consumer;

import com.example.kafka.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class OrderConsumerService {
    
    /**
     * Listen to orders topic with manual acknowledgment
     * Uses ThreadContext to add partition and offset information to logs
     */
    @KafkaListener(
        topics = "${app.kafka.topics.orders}",
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "orderKafkaListenerContainerFactory"
    )
    public void consumeOrder(
            @Payload Order order,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            Acknowledgment acknowledgment) {
        
        // Set ThreadContext for consistent logging
        org.apache.logging.log4j.ThreadContext.put("topic", topic);
        org.apache.logging.log4j.ThreadContext.put("partition", String.valueOf(partition));
        org.apache.logging.log4j.ThreadContext.put("offset", String.valueOf(offset));
        
        try {
            log.info("Received order: orderId={}, customerId={}, amount={}, status={}", 
                    order.orderId(), 
                    order.customerId(), 
                    order.amount(), 
                    order.status());
            
            // Process the order
            processOrder(order);
            
            log.info("Successfully processed order: {}", order.orderId());
            
            // Acknowledge the message
            acknowledgment.acknowledge();
            
            log.info("Acknowledged offset {} for partition {}", offset, partition);
            
        } catch (Exception ex) {
            log.error("Failed to process order: {}, error: {}", order.orderId(), ex.getMessage(), ex);
            
            // Don't acknowledge - message will be reprocessed
            log.warn("Message not acknowledged, will be reprocessed");
            
        } finally {
            // Clear ThreadContext
            org.apache.logging.log4j.ThreadContext.clearAll();
        }
    }
    
    /**
     * Business logic for processing orders
     */
    private void processOrder(Order order) {
        log.debug("Processing order business logic: {}", order.orderId());
        
        // Simulate order processing
        try {
            Thread.sleep(100); // Simulate processing time
            
            // Add your business logic here
            // - Validate order
            // - Update database
            // - Send notifications
            // - etc.
            
            log.debug("Order processing completed: {}", order.orderId());
            
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Order processing interrupted", ex);
        }
    }
}
