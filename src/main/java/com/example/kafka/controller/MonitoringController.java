package com.example.kafka.controller;

import com.example.kafka.service.BatchPublishingService;
import com.example.kafka.service.OrderConsumedService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api/monitoring")
@RequiredArgsConstructor
public class MonitoringController {
    
    private final BatchPublishingService batchPublishingService;
    private final OrderConsumedService orderConsumedService;
    
    /**
     * Get comprehensive monitoring statistics
     */
    @GetMapping("/stats")
    public ResponseEntity<MonitoringStats> getMonitoringStats() {
        try {
            BatchPublishingService.BatchStats batchStats = batchPublishingService.getBatchStats();
            OrderConsumedService.ConsumptionStats consumptionStats = orderConsumedService.getConsumptionStats();
            
            MonitoringStats stats = new MonitoringStats(batchStats, consumptionStats);
            log.info("Retrieved monitoring stats: {}", stats);
            
            return ResponseEntity.ok(stats);
            
        } catch (Exception ex) {
            log.error("Failed to get monitoring stats: {}", ex.getMessage(), ex);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Get batch processing statistics
     */
    @GetMapping("/batch-stats")
    public ResponseEntity<BatchPublishingService.BatchStats> getBatchStats() {
        try {
            BatchPublishingService.BatchStats stats = batchPublishingService.getBatchStats();
            return ResponseEntity.ok(stats);
        } catch (Exception ex) {
            log.error("Failed to get batch stats: {}", ex.getMessage(), ex);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Get consumption statistics
     */
    @GetMapping("/consumption-stats")
    public ResponseEntity<OrderConsumedService.ConsumptionStats> getConsumptionStats() {
        try {
            OrderConsumedService.ConsumptionStats stats = orderConsumedService.getConsumptionStats();
            return ResponseEntity.ok(stats);
        } catch (Exception ex) {
            log.error("Failed to get consumption stats: {}", ex.getMessage(), ex);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Comprehensive monitoring statistics
     */
    public record MonitoringStats(
        BatchPublishingService.BatchStats batchStats,
        OrderConsumedService.ConsumptionStats consumptionStats
    ) {
        @Override
        public String toString() {
            return String.format(
                "MonitoringStats{batchStats=%s, consumptionStats=%s}",
                batchStats, consumptionStats
            );
        }
    }
}
