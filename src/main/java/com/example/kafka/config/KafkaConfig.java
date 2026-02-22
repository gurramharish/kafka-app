package com.example.kafka.config;

import com.example.kafka.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
@Slf4j
public class KafkaConfig {
    
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    
    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroupId;
    
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(producerFactory);
        template.setDefaultTopic("just-pay-orders");
        return template;
    }
    
    // ==================== Consumer Configuration ====================
    
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> configProps = createBaseConsumerConfig();
        
        log.info("Consumer factory configured with group ID: {}", consumerGroupId);
        
        return new DefaultKafkaConsumerFactory<>(configProps);
    }
    
    // Specific consumer factory for Order objects
    @Bean
    public ConsumerFactory<String, Order> orderConsumerFactory() {
        Map<String, Object> configProps = createBaseConsumerConfig();
        
        // Add default type for Order deserialization
        configProps.put(JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, "com.example.kafka.model.Order");
        
        log.info("Order consumer factory configured with group ID: {}", consumerGroupId);
        
        return new DefaultKafkaConsumerFactory<>(configProps);
    }
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        
        factory.setConsumerFactory(consumerFactory());
        configureContainerFactory(factory);
        
        log.info("Kafka listener container factory configured");
        
        return factory;
    }
    
    // Specific container factory for Order consumers
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Order> orderKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Order> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        
        factory.setConsumerFactory(orderConsumerFactory());
        configureContainerFactory(factory);
        
        log.info("Order Kafka listener container factory configured");
        
        return factory;
    }
    
    /**
     * Creates base consumer configuration common to all consumer factories
     */
    private Map<String, Object> createBaseConsumerConfig() {
        Map<String, Object> configProps = new HashMap<>();
        
        // Basic configuration
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);
        
        // Offset management
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // Performance
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        configProps.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024);
        configProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        
        // Session and heartbeat
        configProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        configProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        configProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        
        // Partition assignment strategy
        configProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, 
                        "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
        
        // JSON deserializer trusted packages
        configProps.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, "com.example.kafka.model");
        
        return configProps;
    }
    
    /**
     * Configures common container factory properties
     */
    private void configureContainerFactory(ConcurrentKafkaListenerContainerFactory<?, ?> factory) {
        // Manual acknowledgment for production
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        
        // Concurrency
        factory.setConcurrency(3);
        
        // Poll timeout
        factory.getContainerProperties().setPollTimeout(3000);
        
        // Error handling
        factory.getContainerProperties().setMissingTopicsFatal(false);
    }
}
