package com.example.kafka.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "orders_to_publish")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderToPublish {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name = "order_id", nullable = false, unique = true, length = 50)
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
    @Column(name = "published_status", length = 20)
    private PublishedStatus publishedStatus = PublishedStatus.PENDING;
    
    @Column(name = "partition_number")
    private Integer partitionNumber;
    
    @Column(name = "offset_number")
    private Long offsetNumber;
    
    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;
    
    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;
    
    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;
    
    public enum PublishedStatus {
        PENDING,
        SUCCESS,
        FAILED
    }
}
