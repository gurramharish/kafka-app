package com.example.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Custom partitioner for consistent hashing based on order ID
 * Ensures same order ID always goes to same partition
 */
@Slf4j
public class ConsistentHashPartitioner implements Partitioner {
    
    private final AtomicInteger counter = new AtomicInteger(0);
    
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        
        // If no key, use round-robin
        if (key == null) {
            int nextPartition = counter.getAndIncrement() % numPartitions;
            log.debug("No key provided, using round-robin partition: {}", nextPartition);
            return nextPartition;
        }
        
        // Use consistent hashing for key
        int partition = Math.abs(Utils.murmur2(keyBytes)) % numPartitions;
        log.debug("Key '{}' hashed to partition {} (total partitions: {})", key, partition, numPartitions);
        
        return partition;
    }
    
    @Override
    public void close() {
        log.info("ConsistentHashPartitioner closed");
    }
    
    @Override
    public void configure(Map<String, ?> configs) {
        log.info("ConsistentHashPartitioner configured with configs: {}", configs);
    }
}
