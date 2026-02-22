package com.example.kafka.controller;

import com.example.kafka.model.Order;
import com.example.kafka.producer.OrderProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;

@Slf4j
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
@Validated
public class OrderController {
    
    private final OrderProducerService orderProducerService;
    
    /**
     * Send a single order to Kafka
     */
    @PostMapping
    public ResponseEntity<OrderResponse> sendOrder(@Valid @RequestBody OrderRequest request) {
        log.info("Received request to send order: {}", request.orderId());
        
        Order order = Order.builder()
                .orderId(request.orderId())
                .customerId(request.customerId())
                .amount(request.amount())
                .currency(request.currency() != null ? request.currency() : "USD")
                .status(request.status() != null ? request.status() : "PENDING")
                .paymentMethod(request.paymentMethod())
                .orderDate(request.orderDate() != null ? request.orderDate() : LocalDateTime.now())
                .description(request.description())
                .build();
        
        try {
            CompletableFuture<SendResult<String, Object>> future = orderProducerService.sendOrder(order);
            
            // For simplicity, we'll wait for the result
            SendResult<String, Object> result = future.get();
            
            OrderResponse response = new OrderResponse(
                    order.orderId(),
                    "SENT",
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset(),
                    result.getRecordMetadata().timestamp(),
                    "Order sent successfully"
            );
            
            return ResponseEntity.ok(response);
            
        } catch (Exception ex) {
            log.error("Failed to send order: {}", request.orderId(), ex);
            
            OrderResponse response = new OrderResponse(
                    order.orderId(),
                    "FAILED",
                    null,
                    null,
                    null,
                    "Failed to send order: " + ex.getMessage()
            );
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * Send a simple order with minimal fields
     */
    @PostMapping("/simple")
    public ResponseEntity<OrderResponse> sendSimpleOrder(@RequestBody SimpleOrderRequest request) {
        log.info("Received simple order request: {}", request.orderId());
        
        Order order = Order.builder()
                .orderId(request.orderId())
                .customerId(request.customerId())
                .amount(request.amount())
                .currency("USD")
                .status("PENDING")
                .orderDate(LocalDateTime.now())
                .build();
        
        try {
            SendResult<String, Object> result = orderProducerService.sendOrderSync(order);
            
            OrderResponse response = new OrderResponse(
                    order.orderId(),
                    "SENT",
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset(),
                    result.getRecordMetadata().timestamp(),
                    "Order sent successfully"
            );
            
            return ResponseEntity.ok(response);
            
        } catch (Exception ex) {
            log.error("Failed to send simple order: {}", request.orderId(), ex);
            
            OrderResponse response = new OrderResponse(
                    order.orderId(),
                    "FAILED",
                    null,
                    null,
                    null,
                    "Failed to send order: " + ex.getMessage()
            );
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    // DTOs as records
    public record OrderRequest(
            @NotBlank(message = "Order ID is required")
            String orderId,
            
            @NotBlank(message = "Customer ID is required")
            String customerId,
            
            @NotNull(message = "Amount is required")
            @Positive(message = "Amount must be positive")
            BigDecimal amount,
            
            String currency,
            String status,
            String paymentMethod,
            LocalDateTime orderDate,
            String description
    ) {}
    
    public record SimpleOrderRequest(
            @NotBlank(message = "Order ID is required")
            String orderId,
            
            @NotBlank(message = "Customer ID is required")
            String customerId,
            
            @NotNull(message = "Amount is required")
            @Positive(message = "Amount must be positive")
            BigDecimal amount
    ) {}
    
    public record OrderResponse(
            String orderId,
            String status,
            Integer partition,
            Long offset,
            Long timestamp,
            String message
    ) {}
}
