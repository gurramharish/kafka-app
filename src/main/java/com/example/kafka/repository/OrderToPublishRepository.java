package com.example.kafka.repository;

import com.example.kafka.entity.OrderToPublish;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OrderToPublishRepository extends JpaRepository<OrderToPublish, Long> {
    
    /**
     * Find orders with PENDING status for batch processing
     */
    @Query("SELECT o FROM OrderToPublish o WHERE o.publishedStatus = 'PENDING' ORDER BY o.createdAt ASC")
    List<OrderToPublish> findPendingOrdersForBatch(Pageable pageable);
    
    /**
     * Update batch of orders with publishing results
     */
    @Modifying
    @Query("UPDATE OrderToPublish o SET o.publishedStatus = :status, o.partitionNumber = :partition, " +
           "o.offsetNumber = :offset, o.errorMessage = :errorMessage WHERE o.id = :id")
    int updatePublishingResult(@Param("id") Long id, 
                              @Param("status") OrderToPublish.PublishedStatus status,
                              @Param("partition") Integer partition, 
                              @Param("offset") Long offset,
                              @Param("errorMessage") String errorMessage);
    
    /**
     * Count pending orders
     */
    @Query("SELECT COUNT(o) FROM OrderToPublish o WHERE o.publishedStatus = 'PENDING'")
    long countPendingOrders();
    
    /**
     * Find by order ID
     */
    OrderToPublish findByOrderId(String orderId);
    
    /**
     * Check if order ID exists
     */
    boolean existsByOrderId(String orderId);
}
