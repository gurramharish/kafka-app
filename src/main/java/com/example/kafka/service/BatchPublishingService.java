package com.example.kafka.service;

import com.example.kafka.entity.OrderToPublish;
import com.example.kafka.model.Order;
import com.example.kafka.producer.OrderProducerService;
import com.example.kafka.repository.OrderToPublishRepository;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class BatchPublishingService {
    
    private final OrderToPublishRepository orderToPublishRepository;
    private final OrderProducerService orderProducerService;
    
    @Value("${app.batch.publish.size:100}")
    private int batchSize;
    
    @Value("${app.batch.publish.max-retries:3}")
    private int maxRetries;
    
    private final AtomicInteger activeBatches = new AtomicInteger(0);
    
    /**
     * Process all pending orders in batches until none are left
     * This method can be called via REST endpoint
     */
    public CompletableFuture<PublishingResult> processPendingOrders() {
        if (activeBatches.get() > 0) {
            log.debug("Another batch is already processing, skipping this run");
            return CompletableFuture.completedFuture(
                new PublishingResult(0, 0, 0, "Another batch is already processing")
            );
        }
        
        try {
            activeBatches.incrementAndGet();
            
            long totalPendingCount = orderToPublishRepository.countPendingOrders();
            if (totalPendingCount == 0) {
                log.info("No pending orders to process");
                return CompletableFuture.completedFuture(
                    new PublishingResult(0, 0, 0, "No pending orders to process")
                );
            }
            
            log.info("Starting processing of {} pending orders (batch size: {})", totalPendingCount, batchSize);
            
            return processAllBatches(0, 0, 0, totalPendingCount);
            
        } catch (Exception ex) {
            log.error("Error in batch processing", ex);
            return CompletableFuture.completedFuture(
                new PublishingResult(0, 0, 0, "Error: " + ex.getMessage())
            );
        } finally {
            activeBatches.decrementAndGet();
        }
    }
    
    /**
     * Recursively process all batches until no pending orders remain
     */
    private CompletableFuture<PublishingResult> processAllBatches(int totalProcessed, 
                                                              int totalSuccess, 
                                                              int totalFailure, 
                                                              long initialPendingCount) {
        long currentPendingCount = orderToPublishRepository.countPendingOrders();
        
        if (currentPendingCount == 0) {
            log.info("All pending orders have been processed. Total: {}, Success: {}, Failures: {}", 
                    totalProcessed, totalSuccess, totalFailure);
            return CompletableFuture.completedFuture(
                new PublishingResult(totalProcessed, totalSuccess, totalFailure, 
                    "All pending orders processed successfully")
            );
        }
        
        log.info("Processing next batch. Remaining pending orders: {}", currentPendingCount);
        
        List<OrderToPublish> orders = orderToPublishRepository.findPendingOrdersForBatch(
            PageRequest.of(0, batchSize));
        
        if (orders.isEmpty()) {
            log.info("No orders found in current batch, completing processing");
            return CompletableFuture.completedFuture(
                new PublishingResult(totalProcessed, totalSuccess, totalFailure, 
                    "All available orders processed")
            );
        }
        
        return processBatch(orders)
            .thenCompose(batchResult -> {
                int newTotalProcessed = totalProcessed + orders.size();
                int newTotalSuccess = totalSuccess + batchResult.successCount();
                int newTotalFailure = totalFailure + batchResult.failureCount();
                
                log.info("Batch completed. Processed: {}, Success: {}, Failures: {}. Total so far - Processed: {}, Success: {}, Failures: {}", 
                        orders.size(), batchResult.successCount(), batchResult.failureCount(),
                        newTotalProcessed, newTotalSuccess, newTotalFailure);
                
                // Continue with next batch
                return processAllBatches(newTotalProcessed, newTotalSuccess, newTotalFailure, initialPendingCount);
            });
    }
    
    /**
     * Process a batch of orders
     */
    @Transactional
    public CompletableFuture<BatchResult> processBatch(List<OrderToPublish> orders) {
        log.info("Processing batch of {} orders", orders.size());
        
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        
        List<CompletableFuture<Void>> futures = orders.stream()
            .map(order -> processSingleOrder(order, successCount, failureCount))
            .toList();
        
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(result -> new BatchResult(successCount.get(), failureCount.get()))
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Batch processing completed with errors", ex);
                } else {
                    log.info("Batch processing completed. Success: {}, Failures: {}", 
                            successCount.get(), failureCount.get());
                }
            });
    }
    
    /**
     * Process a single order
     */
    private CompletableFuture<Void> processSingleOrder(OrderToPublish order, 
                                                       AtomicInteger successCount, 
                                                       AtomicInteger failureCount) {
        log.debug("Processing order: {}", order.getOrderId());
        
        Order kafkaOrder = convertToKafkaOrder(order);
        
        return orderProducerService.sendOrder(kafkaOrder)
            .thenAccept(result -> {
                int partition = result.getRecordMetadata().partition();
                long offset = result.getRecordMetadata().offset();
                
                updateOrderAsSuccess(order, partition, offset);
                successCount.incrementAndGet();
                
                log.info("Successfully published order {} to partition {} with offset {}", 
                        order.getOrderId(), partition, offset);
            })
            .exceptionally(ex -> {
                log.error("Failed to publish order {}: {}", order.getOrderId(), ex.getMessage(), ex);
                updateOrderAsFailed(order, ex.getMessage());
                failureCount.incrementAndGet();
                return null;
            });
    }
    
    /**
     * Update order as successfully published
     */
    @Transactional
    public void updateOrderAsSuccess(OrderToPublish order, int partition, long offset) {
        try {
            int updated = orderToPublishRepository.updatePublishingResult(
                order.getId(), 
                OrderToPublish.PublishedStatus.SUCCESS, 
                partition, 
                offset, 
                null
            );
            
            if (updated == 0) {
                log.warn("No rows updated for order {} - might have been updated by another process", 
                        order.getOrderId());
            } else {
                log.debug("Updated order {} as SUCCESS with partition {} and offset {}", 
                         order.getOrderId(), partition, offset);
            }
        } catch (Exception ex) {
            log.error("Failed to update order {} as SUCCESS: {}", order.getOrderId(), ex.getMessage(), ex);
        }
    }
    
    /**
     * Update order as failed
     */
    @Transactional
    public void updateOrderAsFailed(OrderToPublish order, String errorMessage) {
        try {
            String truncatedError = errorMessage != null && errorMessage.length() > 500 
                ? errorMessage.substring(0, 500) + "..." 
                : errorMessage;
                
            int updated = orderToPublishRepository.updatePublishingResult(
                order.getId(), 
                OrderToPublish.PublishedStatus.FAILED, 
                null, 
                null, 
                truncatedError
            );
            
            if (updated == 0) {
                log.warn("No rows updated for order {} - might have been updated by another process", 
                        order.getOrderId());
            } else {
                log.debug("Updated order {} as FAILED with error: {}", order.getOrderId(), truncatedError);
            }
        } catch (Exception ex) {
            log.error("Failed to update order {} as FAILED: {}", order.getOrderId(), ex.getMessage(), ex);
        }
    }
    
    /**
     * Convert OrderToPublish to Order model for Kafka
     */
    private Order convertToKafkaOrder(OrderToPublish order) {
        return Order.builder()
            .orderId(order.getOrderId())
            .customerId(order.getCustomerId())
            .amount(order.getAmount())
            .currency(order.getCurrency())
            .status(order.getStatus())
            .paymentMethod(order.getPaymentMethod())
            .description(order.getDescription())
            .build();
    }
    
    /**
     * Get batch processing statistics
     */
    public BatchStats getBatchStats() {
        long pendingCount = orderToPublishRepository.countPendingOrders();
        return new BatchStats(pendingCount, activeBatches.get(), batchSize);
    }
    
    /**
     * Statistics record for batch processing
     */
    public record BatchStats(long pendingOrders, int activeBatches, int batchSize) {}
    
    /**
     * Result of batch processing
     */
    public record BatchResult(int successCount, int failureCount) {}
    
    /**
     * Result of publishing operation
     */
    public record PublishingResult(int totalProcessed, int successCount, int failureCount, String message) {}
}
