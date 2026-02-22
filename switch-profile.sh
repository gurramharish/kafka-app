#!/bin/bash

# Kafka Property-Based Profile Switcher
# Usage: ./switch-profile.sh [performance-profile]

PERFORMANCE_PROFILE=${1:-balanced}

echo "=== Starting Kafka App ==="
echo "Performance Profile: $PERFORMANCE_PROFILE"
echo "=========================="

case $PERFORMANCE_PROFILE in
  "high-throughput"|"ht")
    echo "🚀 High Throughput Profile"
    echo "   - Optimized for: Maximum throughput"
    echo "   - Performance: 64KB batches, 20ms linger, 64MB buffer"
    export APP_PRODUCER_PERFORMANCE_PROFILE=high-throughput
    mvn spring-boot:run
    ;;
    
  "low-latency"|"ll")
    echo "⚡ Low Latency Profile"
    echo "   - Optimized for: Real-time processing"
    echo "   - Performance: 4KB batches, 0ms linger, 16MB buffer"
    export APP_PRODUCER_PERFORMANCE_PROFILE=low-latency
    mvn spring-boot:run
    ;;
    
  "balanced"|"b"|*)
    echo "⚖️ Balanced Profile (Default)"
    echo "   - Optimized for: General purpose"
    echo "   - Performance: 16KB batches, 5ms linger, 32MB buffer"
    export APP_PRODUCER_PERFORMANCE_PROFILE=balanced
    mvn spring-boot:run
    ;;
esac

echo ""
echo "=== Property Set ==="
echo "APP_PRODUCER_PERFORMANCE_PROFILE=$APP_PRODUCER_PERFORMANCE_PROFILE"
echo "============================"
echo ""
echo "📊 Available Profiles:"
echo "  - high-throughput (Maximum throughput)"
echo "  - low-latency (Real-time processing)"
echo "  - balanced (Default - general purpose)"
echo "================================"
