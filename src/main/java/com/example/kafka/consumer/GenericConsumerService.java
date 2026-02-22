package com.example.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class GenericConsumerService {
    
    /**
     * Generic consumer for different message types
     * Uses Object type and manual type casting
     */
    @KafkaListener(
        topics = "${app.kafka.topics.orders}",
        groupId = "generic-consumer-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeGenericMessage(
            @Payload Object message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            ConsumerRecord<String, Object> record,
            Acknowledgment acknowledgment) {
        
        // Set ThreadContext for consistent logging
        org.apache.logging.log4j.ThreadContext.put("topic", topic);
        org.apache.logging.log4j.ThreadContext.put("partition", String.valueOf(partition));
        org.apache.logging.log4j.ThreadContext.put("offset", String.valueOf(offset));
        
        try {
            // Handle different message types based on headers or content
            if (message instanceof com.example.kafka.model.Order order) {
                log.info("Received message: orderId={}, customerId={}, amount={}",
                        key, order.customerId(), order.amount());
                processOrder(order);
                
            } else if (message instanceof String stringMessage) {
                log.info("Received String message: {}", stringMessage);
                processStringMessage(stringMessage);
                
            } else if (message instanceof java.util.Map<?, ?> mapMessage) {
                log.info("Received Map message: {}", mapMessage);
                processMapMessage(mapMessage);
                
            } else if(message instanceof ConsumerRecord<?,?>){
                log.info("Received ConsumerRecord message: {}", message);
                log.info("Received ConsumerRecord message key: {}", ((ConsumerRecord<?, ?>) message).key());
                log.info("Received ConsumerRecord message value: {}", ((ConsumerRecord<?, ?>) message).value());
            } else {
                log.info("Received unknown message type: {} - {}", 
                        message.getClass().getSimpleName(), message);
                processUnknownMessage(message);
            }
            
            // Acknowledge the message
            acknowledgment.acknowledge();
            log.info("Acknowledged offset {} for partition {}", offset, partition);
            
        } catch (Exception ex) {
            log.error("Failed to process message: {}, error: {}", message, ex.getMessage(), ex);
            // Don't acknowledge - message will be reprocessed
            log.warn("Message not acknowledged, will be reprocessed");
            
        } finally {
            // Clear ThreadContext
            org.apache.logging.log4j.ThreadContext.clearAll();
        }
    }
    
    private void processOrder(com.example.kafka.model.Order order) {
        // Order-specific processing logic
        log.debug("Processing order: {}", order.orderId());
    }
    
    private void processStringMessage(String message) {
        // String message processing logic
        log.debug("Processing string message: {}", message);
    }
    
    private void processMapMessage(java.util.Map<?, ?> message) {
        // Map message processing logic
        log.debug("Processing map message: {}", message);
    }
    
    private void processUnknownMessage(Object message) {
        // Fallback processing for unknown types
        log.debug("Processing unknown message type: {}", message.getClass().getName());
    }
}
