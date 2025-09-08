![MQTT Logo](https://mqtt.org/assets/img/mqtt-logo.svg)

# MQTT Client Library

The MQTT Client Library is a Python library that provides MQTT functionality with support for multiple payload formats including JSON, compressed JSON with ZSTD, and SparkplugB protocol.

## Features

- **Multiple Payload Formats**: JSON, JSON with ZSTD compression, and SparkplugB protocol
- **Batching Support**: Configurable metric batching for improved throughput  
- **Field Precision Control**: Numeric field rounding to reduce payload size
- **SparkplugB Protocol**: Support for encoding/decoding SparkplugB messages
- **Simple Dictionary Interface**: Works with plain Python dictionaries

## Quick Start

### Basic JSON Usage

```python
from rdp_mqtt.mqtt_client import MqttClient, MqttSettings

# Configure client
settings = MqttSettings(
    host="localhost",
    port=1883,
    ssl=False,
    topic="sensors/+",
    payload_parser="json"
)

# Create and setup client
client = MqttClient(settings)
await client.setup()

# Subscribe to messages
async for message_dict in client.subscribe():
    print(f"Received: {message_dict}")

# Publish a message
await client.publish({
    "sensor": "temperature",
    "value": 23.5,
    "unit": "celsius"
}, topic="sensors/temperature")
```

### SparkplugB Usage

```python
settings = MqttSettings(
    host="localhost",
    port=1883,
    ssl=False,
    payload_parser="sparkplug",
    sparkplug_group_id="MyGroup",
    sparkplug_node_id="MyNode", 
    sparkplug_device_id="MyDevice",
    batch_size=10,
    batch_timeout=5.0
)

client = MqttClient(settings)
await client.setup()

# Publish metric (will be encoded as SparkplugB dataset)
# For SparkplugB, a "timestamp" field is required
await client.publish({
    "timestamp": 1737800200000,  # Unix timestamp in milliseconds
    "voltage": 230.0,
    "current": 10.5,
    "status": "online"
})
```

## Configuration

### MqttSettings

The `MqttSettings` class combines multiple parameter groups:

#### Connection Parameters

- **`host`**: `str` - MQTT broker hostname (default: `"localhost"`)
- **`port`**: `int` - MQTT broker port (default: `8883`)
- **`username`**: `Optional[str]` - Username for authentication (default: `None`)
- **`password`**: `Optional[SecretStr]` - Password for authentication (default: `None`)
- **`ssl`**: `bool` - Enable SSL/TLS (default: `True`)
- **`validate_certificate`**: `bool` - Validate SSL certificate (default: `True`)
- **`identifier`**: `str` - MQTT client ID (default: `"RDP_" + uuid`)
- **`topic`**: `str` - Topic to subscribe to, supports wildcards (default: `"#"`)
- **`qos`**: `int` - QoS level 0-2 (default: `0`)
- **`subscribe`**: `bool` - Whether to subscribe to topic (default: `True`)

#### Payload Parameters

- **`payload_parser`**: `Literal["sparkplug", "json", "json_zstd"]` - Payload format (default: `"json"`)
- **`field_precision`**: `Optional[int]` - Round numeric fields to N decimal places (default: `None`)

#### Batching Parameters

- **`batch_size`**: `int` - Number of metrics to batch (0 = no batching) (default: `0`)
- **`batch_timeout`**: `float` - Max seconds to wait for partial batch (default: `1.0`)

#### SparkplugB Parameters

- **`sparkplug_group_id`**: `str` - Group ID for SparkplugB topics (default: `"RDP_group_" + uuid`)
- **`sparkplug_node_id`**: `str` - Node ID for SparkplugB topics (default: `"RDP_node_" + uuid`)
- **`sparkplug_device_id`**: `str` - Device ID for SparkplugB topics (default: `"RDP_device_" + uuid`)

## Data Formats

### JSON Format

The library works with plain Python dictionaries. No special structure is required. 

```python
# Simple message
{
    "sensor_id": "temp_001",
    "value": 23.5,
    "unit": "celsius"
}

# Complex message
{
    "device": "weather_station", 
    "measurements": {
        "temperature": 23.5,
        "humidity": 65.2,
        "pressure": 1013.25
    },
    "location": "Vienna",
    "timestamp": "2025-01-15T10:30:00Z"
}

# Array of messages
[
    {"sensor": "temp_1", "value": 23.5},
    {"sensor": "temp_2", "value": 24.1}
]
```

### Field Precision

When `field_precision` is set, all float values in the dictionary are rounded:

```python
settings = MqttSettings(field_precision=2)

# Input
{"value": 23.456789, "precise": 1.23456}

# Output (automatically rounded)
{"value": 23.46, "precise": 1.23}
```

### SparkplugB Requirements

For SparkplugB payload parsing, dictionaries **must** contain a `timestamp` field or the current time will be used.

```python
# Valid SparkplugB message
{
    "timestamp": 1737800200000,  # Unix timestamp in milliseconds or iso time string
    "temperature": 25.5,
    "humidity": 60.0,
    "status": "active"
}
```

### SparkplugB Encoding/Decoding

#### Receiving SparkplugB

The library can decode different types of SparkplugB messages into dictionaries:

**Simple SparkplugB Data**:
```python
# Received dictionary from simple SparkplugB message
{
    "temperature": 25.5,
    "pressure": 1013.25,
    # Plus any timestamp information extracted by the decoder
}
```

**Generic DataSet**:
```python
# Received dictionary from dataset message
{
    "Time": 1737800200000,
    "Value": 42.7,
    "Quality": 1.0,
    # Each dataset column becomes a dictionary key
}
```

**Dewesoft DataSet**:
```python
# Received dictionary from Dewesoft dataset  
{
    "voltage_reading": 230.5,  # Field name extracted from metric name
    # Plus timestamp and other extracted data
}
```

#### Sending SparkplugB

All outgoing SparkplugB messages are encoded as datasets:

```python
# Input dictionary
{
    "timestamp": 1737800200000,  # Required!
    "voltage": 230.0,
    "current": 15.2,
    "power": 3450.0
}

# Gets encoded as SparkplugB dataset message with:
# - Columns: ["voltage", "current", "power"] 
# - Row data: [230.0, 15.2, 3450.0]
# - Timestamp: 1737800200000
```

## Batching

When `batch_size > 0`, dictionaries are collected and sent together:

```python
settings = MqttSettings(
    batch_size=5,         # Send when 5 messages collected
    batch_timeout=2.0,    # Or after 2 seconds
    payload_parser="json"
)

# Multiple publish calls get batched
await client.publish({"sensor": "temp1", "value": 23.5})
await client.publish({"sensor": "temp2", "value": 24.1}) 
await client.publish({"sensor": "temp3", "value": 22.8})
# ... batched and sent together when size/timeout reached
```

## Usage Examples

### JSON Publisher

```python
# Publisher
settings = MqttSettings(
    host="broker.example.com",
    payload_parser="json",
    subscribe=False  # Publisher only
)

client = MqttClient(settings)
await client.setup()

await client.publish({
    "device_id": "sensor_001",
    "temperature": 25.3,
    "timestamp": "2025-01-15T10:30:00Z"
}, topic="sensors/temperature")
```
### JSON Subscriber

```python
# Subscriber  
settings = MqttSettings(
    host="broker.example.com", 
    topic="sensors/+",
    payload_parser="json"
)

client = MqttClient(settings)
await client.setup()

async for message in client.subscribe():
    print(f"Device: {message.get('device_id')}")
    print(f"Temperature: {message.get('temperature')}")
```

### Compressed JSON

```python
settings = MqttSettings(
    payload_parser="json_zstd",  # Automatic compression
    batch_size=10,
    field_precision=2
)

client = MqttClient(settings)
await client.setup()

# Large messages automatically compressed
await client.publish({
    "device": "data_logger",
    "large_dataset": list(range(1000)),  # Will be compressed
    "metadata": "compressed_transmission"
})
```

### SparkplugB with Batching

```python
settings = MqttSettings(
    payload_parser="sparkplug",
    sparkplug_group_id="FactoryFloor",
    sparkplug_node_id="Line1", 
    sparkplug_device_id="Sensor01",
    batch_size=20,
    batch_timeout=5.0
)

client = MqttClient(settings)
await client.setup()

# Send multiple readings - will be batched into single SparkplugB message
for i in range(25):
    await client.publish({
        "timestamp": int(time.time() * 1000),  # Current time in ms
        "reading_id": i,
        "value": 100 + i * 0.5,
        "quality": 1.0 if i % 10 != 0 else 0.8
    })
```

## Error Handling

```python
try:
    client = MqttClient(settings)
    await client.setup()
    
    async for message in client.subscribe():
        # Process raw dictionary
        process_message(message)
        
except ConnectionError:
    print("MQTT broker connection failed")
except ValueError as e:
    print(f"Invalid configuration: {e}")
finally:
    await client.shutdown()
```

## API Reference

### MqttClient Methods

#### `__init__(settings: MqttSettings)`
Create MQTT client with configuration.

#### `async setup() -> None` 
Initialize connection and prepare client.

#### `async publish(data: Dict[str, Any], topic: Optional[str] = None) -> None`
Publish a dictionary to MQTT.
- For SparkplugB: `data` must contain `timestamp` field else the current time is used.
- Field precision applied automatically if configured
- Batching handled automatically if configured

#### `async subscribe() -> AsyncGenerator[Dict[str, Any], None]`
Subscribe and yield received dictionaries.
- JSON: Returns parsed dictionary  
- JSON_ZSTD: Returns decompressed and parsed dictionary
- SparkplugB: Returns decoded dictionary with extracted fields

#### `async shutdown() -> None`
Gracefully shutdown the client and flush any pending batches.

### Utility Functions

#### `get_sparkplug_topic(group_id: str, node_id: str, message_type: str) -> str`
Generate SparkplugB topic string.

#### `encode_data_message(metrics: List[Dict], alias_map: Dict = None, use_aliases_only: bool = False) -> bytes`
Encode dictionaries as SparkplugB DDATA message and use aliases if required from alias_map.

#### `encode_birth_message(metrics: List[Dict], alias_map: Dict = None) -> bytes`
Encode dictionaries as SparkplugB DBIRTH message and store aliases in alias_map.

#### `decode_mqtt_message(message: MQTTMessage) -> List[Dict[str, Any]]`
Decode SparkplugB MQTT message to list of dictionaries.

## Hints

- json_zstd is typically smaller than sparkplug even if the message is big
- Complex jsons can only be sent with json and json_zstd