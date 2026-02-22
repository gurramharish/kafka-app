package com.example.kafka.controller;

import com.example.kafka.service.PerformanceProfileService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/performance")
@RequiredArgsConstructor
public class PerformanceController {
    
    @Autowired
    private PerformanceProfileService performanceProfileService;
    
    /**
     * Get current performance profile
     */
    @GetMapping("/profile")
    public ResponseEntity<Map<String, Object>> getCurrentProfile() {
        Map<String, Object> response = new HashMap<>();
        response.put("activeProfile", performanceProfileService.getPerformanceProfile());
        response.put("timestamp", System.currentTimeMillis());
        
        return ResponseEntity.ok(response);
    }
    
    /**
     * Get performance recommendations
     */
    @GetMapping("/recommendation")
    public ResponseEntity<Map<String, Object>> getRecommendation(
            @RequestParam(defaultValue = "1024") int messageSize,
            @RequestParam(defaultValue = "5000") int messageRate,
            @RequestParam(defaultValue = "false") boolean latencyCritical) {
        
        String recommendation = performanceProfileService.getRecommendation(messageSize, messageRate, latencyCritical);
        
        Map<String, Object> response = new HashMap<>();
        response.put("messageSize", messageSize);
        response.put("messageRate", messageRate);
        response.put("latencyCritical", latencyCritical);
        response.put("recommendedProfile", recommendation);
        response.put("explanation", getExplanation(recommendation, messageSize, messageRate, latencyCritical));
        
        return ResponseEntity.ok(response);
    }
    
    /**
     * Switch performance profile (requires restart)
     */
    @PostMapping("/profile/switch")
    public ResponseEntity<Map<String, String>> switchProfile(@RequestBody Map<String, String> request) {
        String newProfile = request.get("profile");
        performanceProfileService.switchProfile(newProfile);
        
        Map<String, String> response = new HashMap<>();
        response.put("message", "Profile switch requested");
        response.put("currentProfile", performanceProfileService.getPerformanceProfile());
        response.put("requestedProfile", newProfile);
        response.put("action", "Restart application with: -Dspring.profiles.active=" + newProfile);
        
        return ResponseEntity.ok(response);
    }
    
    /**
     * Get all available profiles
     */
    @GetMapping("/profiles")
    public ResponseEntity<Map<String, Object>> getAvailableProfiles() {
        Map<String, Object> response = new HashMap<>();
        response.put("profiles", Map.of(
            "high-throughput", Map.of(
                "description", "Maximum throughput, higher latency",
                "batchSize", "65536",
                "lingerMs", "20",
                "bufferMemory", "67108864",
                "compression", "lz4",
                "useCase", "Batch processing, analytics"
            ),
            "low-latency", Map.of(
                "description", "Minimum latency, lower throughput", 
                "batchSize", "4096",
                "lingerMs", "0",
                "bufferMemory", "16777216",
                "compression", "none",
                "useCase", "Real-time, API responses"
            ),
            "balanced", Map.of(
                "description", "Good balance of throughput and latency",
                "batchSize", "16384", 
                "lingerMs", "5",
                "bufferMemory", "33554432",
                "compression", "lz4",
                "useCase", "General purpose"
            ),
            "custom", Map.of(
                "description", "Custom settings via system properties",
                "batchSize", "System property: producer.batch.size",
                "lingerMs", "System property: producer.linger.ms",
                "bufferMemory", "System property: producer.buffer.memory",
                "compression", "System property: producer.compression.type",
                "useCase", "Tuned workloads"
            )
        ));
        
        return ResponseEntity.ok(response);
    }
    
    private String getExplanation(String profile, int messageSize, int messageRate, boolean latencyCritical) {
        return switch (profile) {
            case "high-throughput" -> String.format(
                "High throughput recommended due to %s messages/sec and %s byte message size. " +
                "Optimized for batch processing with 64KB batches and 20ms linger time.",
                messageRate, messageSize);
            case "low-latency" -> String.format(
                "Low latency recommended due to critical latency requirement. " +
                "Optimized for real-time with 4KB batches and immediate sending.",
                messageRate, messageSize);
            default -> String.format(
                "Balanced profile recommended for general use with %s messages/sec and %s byte messages. " +
                "Good compromise between throughput and latency.",
                messageRate, messageSize);
        };
    }
}
