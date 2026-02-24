package com.example.kafka.controller;

import com.example.kafka.service.BatchPublishingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.CompletableFuture;

@Slf4j
@RestController
@RequestMapping("/api/batch")
@RequiredArgsConstructor
public class BatchPublishingController {
    
    private final BatchPublishingService batchPublishingService;
    
    /**
     * Trigger batch publishing of pending orders
     * Continues processing until all pending orders are published
     * 
     * @return Publishing result with statistics
     */
    @PostMapping("/publish")
    public CompletableFuture<ResponseEntity<BatchPublishingService.PublishingResult>> publishPendingOrders() {
        log.info("Received request to publish ALL pending orders");
        
        return batchPublishingService.processPendingOrders()
            .thenApply(result -> {
                log.info("Complete batch publishing finished: {}", result);
                return ResponseEntity.ok(result);
            })
            .exceptionally(ex -> {
                log.error("Complete batch publishing failed", ex);
                return ResponseEntity.internalServerError()
                    .body(new BatchPublishingService.PublishingResult(
                        0, 0, 0, "Failed: " + ex.getMessage()
                    ));
            });
    }
    
    /**
     * Get real-time progress of current batch processing
     * 
     * @return Current progress information
     */
    @GetMapping("/progress")
    public ResponseEntity<BatchProgress> getBatchProgress() {
        try {
            BatchPublishingService.BatchStats stats = batchPublishingService.getBatchStats();
            BatchProgress progress = new BatchProgress(
                stats.activeBatches() > 0,
                stats.activeBatches(),
                stats.pendingOrders(),
                stats.batchSize(),
                stats.activeBatches() > 0 ? "Processing batches..." : "Idle"
            );
            return ResponseEntity.ok(progress);
        } catch (Exception ex) {
            log.error("Failed to get batch progress", ex);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Get batch processing statistics
     * 
     * @return Current batch statistics
     */
    @GetMapping("/stats")
    public ResponseEntity<BatchPublishingService.BatchStats> getBatchStats() {
        try {
            BatchPublishingService.BatchStats stats = batchPublishingService.getBatchStats();
            log.info("Retrieved batch stats: {}", stats);
            return ResponseEntity.ok(stats);
        } catch (Exception ex) {
            log.error("Failed to get batch stats", ex);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Check if batch processing is currently active
     * 
     * @return Status of batch processing
     */
    @GetMapping("/status")
    public ResponseEntity<BatchStatus> getBatchStatus() {
        try {
            BatchPublishingService.BatchStats stats = batchPublishingService.getBatchStats();
            BatchStatus status = new BatchStatus(
                stats.activeBatches() > 0,
                stats.activeBatches(),
                stats.pendingOrders(),
                stats.batchSize()
            );
            return ResponseEntity.ok(status);
        } catch (Exception ex) {
            log.error("Failed to get batch status", ex);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Batch status information
     */
    public record BatchStatus(
        boolean isProcessing,
        int activeBatches,
        long pendingOrders,
        int batchSize
    ) {}
    
    /**
     * Batch progress information
     */
    public record BatchProgress(
        boolean isProcessing,
        int activeBatches,
        long remainingOrders,
        int batchSize,
        String status
    ) {}
}
