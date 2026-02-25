# Bruno API Collection for Kafka App

This directory contains a Bruno collection for testing all the Kafka application endpoints.

## Setup

1. Install Bruno from [https://www.usebruno.com/](https://www.usebruno.com/)
2. Open Bruno and import this collection
3. Set the environment variable `baseUrl` to your application URL (default: `http://localhost:8080`)

## Collection Structure

### Order Management APIs
- **Send Full Order** - POST `/api/orders` - Send a complete order with all fields
- **Send Simple Order** - POST `/api/orders/simple` - Send a minimal order
- **Generate Random Orders** - POST `/api/orders/generate` - Generate test orders
- **Generate Orders Async** - POST `/api/orders/generate-async` - Generate orders asynchronously
- **Get Order Statistics** - GET `/api/orders/stats` - Get order count statistics

### Batch Processing APIs
- **Publish Pending Orders** - POST `/api/batch/publish` - Trigger batch publishing
- **Get Batch Progress** - GET `/api/batch/progress` - Monitor batch processing progress
- **Get Batch Statistics** - GET `/api/batch/stats` - Get batch processing stats
- **Get Batch Status** - GET `/api/batch/status` - Check batch processing status

### Monitoring APIs
- **Get Monitoring Stats** - GET `/api/monitoring/stats` - Comprehensive monitoring data
- **Get Batch Stats (Monitoring)** - GET `/api/monitoring/batch-stats` - Batch monitoring stats
- **Get Consumption Stats** - GET `/api/monitoring/consumption-stats` - Consumer statistics

### Performance Management APIs
- **Get Current Performance Profile** - GET `/api/performance/profile` - Current profile info
- **Get Performance Recommendations** - GET `/api/performance/recommendation` - Get recommendations
- **Get Available Profiles** - GET `/api/performance/profiles` - List all profiles
- **Switch Performance Profile** - POST `/api/performance/profile/switch` - Change performance profile

## Usage Examples

### Basic Workflow
1. **Generate test orders**: Use "Generate Random Orders" to create test data
2. **Check statistics**: Use "Get Order Statistics" to see pending orders
3. **Publish orders**: Use "Publish Pending Orders" to start batch processing
4. **Monitor progress**: Use "Get Batch Progress" to track processing
5. **Check results**: Use "Get Monitoring Stats" for comprehensive results

### Performance Testing
1. **Generate large batch**: Use "Generate Orders Async" with count=5000+
2. **Set performance profile**: Use "Switch Performance Profile" to "high-throughput"
3. **Publish and monitor**: Use batch publishing endpoints while monitoring
4. **Analyze performance**: Use monitoring endpoints to measure throughput

### Environment Configuration
The collection uses environment variables:
- `baseUrl`: Base URL of the application (default: `http://localhost:8080`)

To modify:
1. Open Bruno
2. Select the "Kafka App" collection
3. Go to Environment tab
4. Edit `baseUrl` as needed

## Testing Scenarios

### Scenario 1: Basic Order Processing
```bash
# 1. Send a single order
Send Full Order

# 2. Check order stats
Get Order Statistics

# 3. Publish pending orders
Publish Pending Orders

# 4. Monitor progress
Get Batch Progress
```

### Scenario 2: High Volume Testing
```bash
# 1. Generate 1000 orders
Generate Orders Async (count=1000)

# 2. Switch to high-throughput profile
Switch Performance Profile (profile=high-throughput)

# 3. Publish all orders
Publish Pending Orders

# 4. Monitor comprehensive stats
Get Monitoring Stats
```

### Scenario 3: Performance Comparison
```bash
# 1. Generate test data
Generate Random Orders (count=500)

# 2. Test with low-latency profile
Switch Performance Profile (profile=low-latency)
Publish Pending Orders
Get Monitoring Stats

# 3. Test with high-throughput profile
Switch Performance Profile (profile=high-throughput)
Generate Random Orders (count=500)
Publish Pending Orders
Get Monitoring Stats
```

## Response Examples

Each request includes expected response formats in the main README.md file.

## Tips
- Use the "Get Batch Progress" endpoint to monitor long-running batch operations
- The "Get Monitoring Stats" endpoint provides the most comprehensive view
- For performance testing, use async generation for large batches
- Check "Get Available Profiles" to see all performance options
- Use environment variables to easily switch between different deployment environments
