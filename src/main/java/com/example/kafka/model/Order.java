package com.example.kafka.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record Order(
        @NotBlank(message = "Order ID is required")
        String orderId,
        
        @NotBlank(message = "Customer ID is required")
        String customerId,
        
        @NotNull(message = "Amount is required")
        @Positive(message = "Amount must be positive")
        BigDecimal amount,
        
        String currency,
        
        @NotBlank(message = "Status is required")
        String status,
        
        String paymentMethod,
        
        @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
        LocalDateTime orderDate,
        
        String description
) {
    
    /**
     * Compact constructor to set default values
     */
    public Order {
        if (currency == null) currency = "USD";
        if (orderDate == null) orderDate = LocalDateTime.now();
    }
    
    /**
     * Get partition key for consistent hashing
     * Returns orderId to ensure same order always goes to same partition
     */
    public String getPartitionKey() {
        return orderId != null ? orderId : customerId;
    }
    
    /**
     * Builder pattern for compatibility with existing code
     */
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private String orderId;
        private String customerId;
        private BigDecimal amount;
        private String currency;
        private String status;
        private String paymentMethod;
        private LocalDateTime orderDate;
        private String description;
        
        public Builder orderId(String orderId) {
            this.orderId = orderId;
            return this;
        }
        
        public Builder customerId(String customerId) {
            this.customerId = customerId;
            return this;
        }
        
        public Builder amount(BigDecimal amount) {
            this.amount = amount;
            return this;
        }
        
        public Builder currency(String currency) {
            this.currency = currency;
            return this;
        }
        
        public Builder status(String status) {
            this.status = status;
            return this;
        }
        
        public Builder paymentMethod(String paymentMethod) {
            this.paymentMethod = paymentMethod;
            return this;
        }
        
        public Builder orderDate(LocalDateTime orderDate) {
            this.orderDate = orderDate;
            return this;
        }
        
        public Builder description(String description) {
            this.description = description;
            return this;
        }
        
        public Order build() {
            return new Order(orderId, customerId, amount, currency, status, paymentMethod, orderDate, description);
        }
    }
}
