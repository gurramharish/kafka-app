package com.example.kafka.service;

import com.example.kafka.entity.OrderConsumed;
import com.example.kafka.model.Order;
import com.example.kafka.repository.OrderConsumedRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderConsumedService {
    
    private final OrderConsumedRepository orderConsumedRepository;
    
    /**
     * Save consumed order to database
     */
    @Transactional
    public OrderConsumed saveConsumedOrder(Order order, int partition, long offset) {
        try {
            // Check if order already exists
            OrderConsumed existingOrder = orderConsumedRepository.findByOrderId(order.orderId());
            
            if (existingOrder != null) {
                log.warn("Order {} already exists in consumed table, updating partition and offset", 
                        order.orderId());
                existingOrder.setConsumedFromPartition(partition);
                existingOrder.setConsumedFromOffset(offset);
                existingOrder.setConsumedAt(LocalDateTime.now());
                return orderConsumedRepository.save(existingOrder);
            }
            
            OrderConsumed orderConsumed = OrderConsumed.builder()
                .orderId(order.orderId())
                .customerId(order.customerId())
                .amount(order.amount())
                .currency(order.currency())
                .status(order.status())
                .paymentMethod(order.paymentMethod())
                .description(order.description())
                .consumedStatus(OrderConsumed.ConsumedStatus.PENDING)
                .consumedFromPartition(partition)
                .consumedFromOffset(offset)
                .consumedAt(LocalDateTime.now())
                .build();
            
            OrderConsumed saved = orderConsumedRepository.save(orderConsumed);
            log.info("Saved consumed order {} from partition {} with offset {}", 
                    order.orderId(), partition, offset);
            
            return saved;
            
        } catch (Exception ex) {
            log.error("Failed to save consumed order {}: {}", order.orderId(), ex.getMessage(), ex);
            throw new RuntimeException("Failed to save consumed order", ex);
        }
    }
    
    /**
     * Mark order as successfully processed
     */
    @Transactional
    public void markOrderAsSuccess(String orderId) {
        try {
            OrderConsumed order = orderConsumedRepository.findByOrderId(orderId);
            if (order != null) {
                int updated = orderConsumedRepository.updateProcessingResult(
                    order.getId(), 
                    OrderConsumed.ConsumedStatus.SUCCESS, 
                    null
                );
                
                if (updated > 0) {
                    log.info("Marked order {} as successfully processed", orderId);
                } else {
                    log.warn("No rows updated for order {} when marking as success", orderId);
                }
            } else {
                log.warn("Order {} not found in consumed table", orderId);
            }
        } catch (Exception ex) {
            log.error("Failed to mark order {} as success: {}", orderId, ex.getMessage(), ex);
        }
    }
    
    /**
     * Mark order as failed with error message
     */
    @Transactional
    public void markOrderAsFailed(String orderId, String errorMessage) {
        try {
            OrderConsumed order = orderConsumedRepository.findByOrderId(orderId);
            if (order != null) {
                String truncatedError = errorMessage != null && errorMessage.length() > 500 
                    ? errorMessage.substring(0, 500) + "..." 
                    : errorMessage;
                    
                int updated = orderConsumedRepository.updateProcessingResult(
                    order.getId(), 
                    OrderConsumed.ConsumedStatus.FAILED, 
                    truncatedError
                );
                
                if (updated > 0) {
                    log.info("Marked order {} as failed with error: {}", orderId, truncatedError);
                } else {
                    log.warn("No rows updated for order {} when marking as failed", orderId);
                }
            } else {
                log.warn("Order {} not found in consumed table", orderId);
            }
        } catch (Exception ex) {
            log.error("Failed to mark order {} as failed: {}", orderId, ex.getMessage(), ex);
        }
    }
    
    /**
     * Get consumption statistics
     */
    public ConsumptionStats getConsumptionStats() {
        long pendingCount = orderConsumedRepository.countByConsumedStatus(OrderConsumed.ConsumedStatus.PENDING);
        long successCount = orderConsumedRepository.countByConsumedStatus(OrderConsumed.ConsumedStatus.SUCCESS);
        long failedCount = orderConsumedRepository.countByConsumedStatus(OrderConsumed.ConsumedStatus.FAILED);
        
        return new ConsumptionStats(pendingCount, successCount, failedCount);
    }
    
    /**
     * Statistics record for consumption
     */
    public record ConsumptionStats(long pending, long success, long failed) {}
}
