package com.example.kafka.repository;

import com.example.kafka.entity.OrderConsumed;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OrderConsumedRepository extends JpaRepository<OrderConsumed, Long> {
    
    /**
     * Find by order ID
     */
    OrderConsumed findByOrderId(String orderId);
    
    /**
     * Check if order ID exists
     */
    boolean existsByOrderId(String orderId);
    
    /**
     * Update consumed order with processing results
     */
    @Modifying
    @Query("UPDATE OrderConsumed o SET o.consumedStatus = :status, o.processedAt = CURRENT_TIMESTAMP, " +
           "o.errorMessage = :errorMessage WHERE o.id = :id")
    int updateProcessingResult(@Param("id") Long id, 
                              @Param("status") OrderConsumed.ConsumedStatus status,
                              @Param("errorMessage") String errorMessage);
    
    /**
     * Find orders by consumed status
     */
    List<OrderConsumed> findByConsumedStatus(OrderConsumed.ConsumedStatus status);
    
    /**
     * Count consumed orders by status
     */
    @Query("SELECT COUNT(o) FROM OrderConsumed o WHERE o.consumedStatus = :status")
    long countByConsumedStatus(@Param("status") OrderConsumed.ConsumedStatus status);
}
