package com.example.kafka.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "orders_consumed")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderConsumed {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name = "order_id", nullable = false, length = 50)
    private String orderId;
    
    @Column(name = "customer_id", nullable = false, length = 50)
    private String customerId;
    
    @Column(name = "amount", nullable = false, precision = 10, scale = 2)
    private BigDecimal amount;
    
    @Column(name = "currency", length = 3)
    private String currency = "USD";
    
    @Column(name = "status", length = 20)
    private String status = "PENDING";
    
    @Column(name = "payment_method", length = 20)
    private String paymentMethod;
    
    @Column(name = "description", columnDefinition = "TEXT")
    private String description;
    
    @Enumerated(EnumType.STRING)
    @Column(name = "consumed_status", length = 20)
    private ConsumedStatus consumedStatus = ConsumedStatus.PENDING;
    
    @Column(name = "consumed_from_partition")
    private Integer consumedFromPartition;
    
    @Column(name = "consumed_from_offset")
    private Long consumedFromOffset;
    
    @Column(name = "consumed_at", nullable = false)
    private LocalDateTime consumedAt = LocalDateTime.now();
    
    @Column(name = "processed_at")
    private LocalDateTime processedAt;
    
    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;
    
    public enum ConsumedStatus {
        PENDING,
        SUCCESS,
        FAILED
    }
}
