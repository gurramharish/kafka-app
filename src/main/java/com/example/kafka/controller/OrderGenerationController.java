package com.example.kafka.controller;

import com.example.kafka.entity.OrderToPublish;
import com.example.kafka.repository.OrderToPublishRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

@Slf4j
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderGenerationController {
    
    private final OrderToPublishRepository orderToPublishRepository;
    private final Random random = new Random();
    
    /**
     * Generate random orders and insert them into orders_to_publish table
     * 
     * @param count Number of orders to generate (default: 100)
     * @return Summary of generated orders
     */
    @PostMapping("/generate")
    @Transactional
    public ResponseEntity<GenerationResponse> generateOrders(
            @RequestParam(defaultValue = "100") int count) {
        
        log.info("Starting generation of {} random orders", count);
        
        if (count <= 0 || count > 10000) {
            return ResponseEntity.badRequest()
                .body(new GenerationResponse(0, 0, "Count must be between 1 and 10000"));
        }
        
        try {
            List<OrderToPublish> orders = generateRandomOrders(count);
            List<OrderToPublish> savedOrders = orderToPublishRepository.saveAll(orders);
            
            log.info("Successfully generated {} orders", savedOrders.size());
            
            return ResponseEntity.ok(new GenerationResponse(
                count, 
                savedOrders.size(), 
                "Successfully generated orders"
            ));
            
        } catch (Exception ex) {
            log.error("Failed to generate orders: {}", ex.getMessage(), ex);
            return ResponseEntity.internalServerError()
                .body(new GenerationResponse(0, 0, "Failed to generate orders: " + ex.getMessage()));
        }
    }
    
    /**
     * Generate orders asynchronously for large batches
     */
    @PostMapping("/generate-async")
    public CompletableFuture<ResponseEntity<GenerationResponse>> generateOrdersAsync(
            @RequestParam(defaultValue = "1000") int count) {
        
        log.info("Starting async generation of {} random orders", count);
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                List<OrderToPublish> orders = generateRandomOrders(count);
                List<OrderToPublish> savedOrders = orderToPublishRepository.saveAll(orders);
                
                log.info("Successfully generated {} orders asynchronously", savedOrders.size());
                
                return ResponseEntity.ok(new GenerationResponse(
                    count, 
                    savedOrders.size(), 
                    "Successfully generated orders asynchronously"
                ));
                
            } catch (Exception ex) {
                log.error("Failed to generate orders asynchronously: {}", ex.getMessage(), ex);
                return ResponseEntity.internalServerError()
                    .body(new GenerationResponse(0, 0, "Failed to generate orders: " + ex.getMessage()));
            }
        });
    }
    
    /**
     * Get statistics about orders in the database
     */
    @GetMapping("/stats")
    public ResponseEntity<OrderStats> getOrderStats() {
        try {
            long totalOrders = orderToPublishRepository.count();
            long pendingOrders = orderToPublishRepository.countPendingOrders();
            
            return ResponseEntity.ok(new OrderStats(totalOrders, pendingOrders));
            
        } catch (Exception ex) {
            log.error("Failed to get order stats: {}", ex.getMessage(), ex);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Generate a list of random orders
     */
    private List<OrderToPublish> generateRandomOrders(int count) {
        List<OrderToPublish> orders = new ArrayList<>();
        
        for (int i = 1; i <= count; i++) {
            String orderId = "ORD-" + System.currentTimeMillis() + "-" + i;
            String customerId = "CUST-" + String.format("%04d", random.nextInt(10000));
            
            OrderToPublish order = OrderToPublish.builder()
                .orderId(orderId)
                .customerId(customerId)
                .amount(generateRandomAmount())
                .currency(generateRandomCurrency())
                .status(generateRandomStatus())
                .paymentMethod(generateRandomPaymentMethod())
                .description("Generated order " + i + " for testing batch publishing")
                .publishedStatus(OrderToPublish.PublishedStatus.PENDING)
                .build();
            
            orders.add(order);
        }
        
        return orders;
    }
    
    /**
     * Generate random amount between 10.00 and 2000.00
     */
    private BigDecimal generateRandomAmount() {
        double amount = 10.0 + (random.nextDouble() * 1990.0);
        return BigDecimal.valueOf(amount).setScale(2, RoundingMode.HALF_UP);
    }
    
    /**
     * Generate random currency
     */
    private String generateRandomCurrency() {
        String[] currencies = {"USD", "EUR", "GBP", "JPY", "CAD", "AUD"};
        return currencies[random.nextInt(currencies.length)];
    }
    
    /**
     * Generate random status
     */
    private String generateRandomStatus() {
        String[] statuses = {"PENDING", "PROCESSING", "COMPLETED", "CANCELLED"};
        return statuses[random.nextInt(statuses.length)];
    }
    
    /**
     * Generate random payment method
     */
    private String generateRandomPaymentMethod() {
        String[] methods = {"CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "BANK_TRANSFER", "CRYPTO"};
        return methods[random.nextInt(methods.length)];
    }
    
    /**
     * Response object for order generation
     */
    public record GenerationResponse(int requested, int generated, String message) {}
    
    /**
     * Statistics about orders
     */
    public record OrderStats(long total, long pending) {}
}
