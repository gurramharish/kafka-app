package com.example.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;

@Slf4j
@Service
public class PerformanceProfileService {
    
    @Autowired
    private ApplicationContext applicationContext;
    
    private String activeProfile;
    
    @PostConstruct
    public void init() {
        // Get active profile
        String[] activeProfiles = applicationContext.getEnvironment().getActiveProfiles();
        activeProfile = activeProfiles.length > 0 ? activeProfiles[0] : "balanced";
        
        log.info("=== Kafka Producer Performance Profile: {} ===", activeProfile.toUpperCase());
        logPerformanceInfo();
    }
    
    /**
     * Get current performance profile information
     */
    public String getPerformanceProfile() {
        return activeProfile;
    }
    
    /**
     * Log current performance configuration
     */
    private void logPerformanceInfo() {
        switch (activeProfile.toLowerCase()) {
            case "high-throughput":
                log.info("🚀 HIGH THROUGHPUT MODE:");
                log.info("   - Batch Size: 64KB (maximize batching)");
                log.info("   - Linger Time: 20ms (accumulate larger batches)");
                log.info("   - Buffer Memory: 64MB (handle bursts)");
                log.info("   - Compression: LZ4 (fast compression)");
                log.info("   - Best for: Batch processing, analytics, data pipelines");
                break;
                
            case "low-latency":
                log.info("⚡ LOW LATENCY MODE:");
                log.info("   - Batch Size: 4KB (minimize batching delay)");
                log.info("   - Linger Time: 0ms (immediate sending)");
                log.info("   - Buffer Memory: 16MB (minimal buffering)");
                log.info("   - Compression: None (no CPU overhead)");
                log.info("   - Best for: Real-time, user interactions, API responses");
                break;
                
            case "custom":
                log.info("⚙️  CUSTOM MODE:");
                log.info("   - Batch Size: {} bytes", System.getProperty("producer.batch.size", "16384"));
                log.info("   - Linger Time: {}ms", System.getProperty("producer.linger.ms", "5"));
                log.info("   - Buffer Memory: {} bytes", System.getProperty("producer.buffer.memory", "33554432"));
                log.info("   - Compression: {}", System.getProperty("producer.compression.type", "lz4"));
                log.info("   - Best for: Tuned workloads, specific requirements");
                break;
                
            default: // balanced
                log.info("⚖️  BALANCED MODE (Default):");
                log.info("   - Batch Size: 16KB (good batching)");
                log.info("   - Linger Time: 5ms (small wait)");
                log.info("   - Buffer Memory: 32MB (moderate buffering)");
                log.info("   - Compression: LZ4 (good balance)");
                log.info("   - Best for: General purpose, mixed workloads");
                break;
        }
        log.info("==============================================");
    }
    
    /**
     * Switch to different performance profile at runtime
     * Note: This requires application restart for full effect
     */
    public void switchProfile(String newProfile) {
        log.warn("Profile switch requested from '{}' to '{}'. Restart application for full effect.", 
                activeProfile, newProfile);
        log.info("To switch profiles, use: -Dspring.profiles.active={}", newProfile);
    }
    
    /**
     * Get performance recommendations based on message characteristics
     */
    public String getRecommendation(int messageSize, int messageRate, boolean latencyCritical) {
        if (latencyCritical) {
            return "low-latency";
        } else if (messageRate > 10000) {
            return "high-throughput";
        } else if (messageSize > 1024 * 10) { // > 10KB messages
            return "high-throughput";
        } else {
            return "balanced";
        }
    }
}
