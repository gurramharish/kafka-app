package com.example.kafka.config;

import com.example.kafka.producer.ConsistentHashPartitioner;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    
    @Value("${app.producer.performance.profile:balanced}")
    private String performanceProfile;

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> configProps = createBaseConfig();
        applyPerformanceSettings(configProps, performanceProfile.toLowerCase());
        
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * Applies performance settings based on the profile
     */
    private void applyPerformanceSettings(Map<String, Object> configProps, String profile) {
        PerformanceSettings settings = getPerformanceSettings(profile);
        
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, settings.batchSize);
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, settings.lingerMs);
        configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, settings.bufferMemory);
        configProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, settings.compressionType);
        configProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, settings.maxInFlightRequests);
        configProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, settings.deliveryTimeoutMs);
        
        log.info("🔧 Producer configured ({}) - Batch: {}KB, Linger: {}ms, Buffer: {}MB, Compression: {}",
                profile.toUpperCase(),
                settings.batchSize / 1024,
                settings.lingerMs,
                settings.bufferMemory / (1024 * 1024),
                settings.compressionType);
    }

    /**
     * Returns performance settings for the given profile
     */
    private PerformanceSettings getPerformanceSettings(String profile) {
      return switch (profile) {
        case "high-throughput" -> new PerformanceSettings(
            65536,      // 64KB batches
            20,         // 20ms wait
            67108864,   // 64MB buffer
            "lz4",      // Fast compression
            5,          // Max in-flight requests
            120000      // 2 minutes timeout
        );
        case "low-latency" -> new PerformanceSettings(
            4096,       // 4KB batches
            0,          // No waiting
            16777216,   // 16MB buffer
            "none",     // No compression
            1,          // Max in-flight requests
            30000       // 30 seconds timeout
        );
        default -> // balanced
            new PerformanceSettings(
                16384,      // 16KB batches
                5,          // 5ms wait
                33554432,   // 32MB buffer
                "lz4",      // Fast compression
                3,          // Max in-flight requests
                60000       // 1 minute timeout
            );
      };
    }

    /**
     * Creates base configuration common to all producer factories
     */
    private Map<String, Object> createBaseConfig() {
        Map<String, Object> configProps = new HashMap<>();
        
        // Basic configuration
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        
        // Common reliability settings
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        configProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        // Custom partitioner for consistent hashing
        configProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, ConsistentHashPartitioner.class);
        
        return configProps;
    }

    /**
     * Inner class to hold performance settings
     */
    private static class PerformanceSettings {
        final int batchSize;
        final int lingerMs;
        final long bufferMemory;
        final String compressionType;
        final int maxInFlightRequests;
        final int deliveryTimeoutMs;

        PerformanceSettings(int batchSize, int lingerMs, long bufferMemory, String compressionType,
                           int maxInFlightRequests, int deliveryTimeoutMs) {
            this.batchSize = batchSize;
            this.lingerMs = lingerMs;
            this.bufferMemory = bufferMemory;
            this.compressionType = compressionType;
            this.maxInFlightRequests = maxInFlightRequests;
            this.deliveryTimeoutMs = deliveryTimeoutMs;
        }
    }
}
