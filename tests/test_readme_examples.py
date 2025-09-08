"""
Tests to verify that the examples in the README.md file work correctly.
"""

import asyncio
import time
from unittest.mock import MagicMock, patch

import orjson
import pytest
import zstandard as zstd

from rdp_mqtt.mqtt_client import MqttClient, MqttSettings
from rdp_mqtt.sparkplug.generated import sparkplug_b_pb2


class TestReadmeExamples:
    """Test cases for README.md examples to ensure they work as documented"""

    @pytest.mark.asyncio
    @patch("rdp_mqtt.mqtt_client.mqtt.Client")
    async def test_basic_json_usage_example(self, mock_mqtt_client_class):
        """Test the basic JSON usage example from README"""
        # Setup mock
        mock_paho_client = MagicMock()
        mock_mqtt_client_class.return_value = mock_paho_client

        # Configure client (from README example)
        settings = MqttSettings(host="localhost", port=1883, ssl=False, topic="sensors/+", payload_parser="json")

        # Create and setup client
        client = MqttClient(settings)
        await client.setup()

        # Verify setup worked
        mock_mqtt_client_class.assert_called_once()
        mock_paho_client.connect_async.assert_called_once_with("localhost", 1883, keepalive=5)

        # Test publish example from README
        test_message = {"sensor": "temperature", "value": 23.5, "unit": "celsius"}

        # Capture the actual publish call through run_in_executor
        captured_payload = None
        captured_topic = None

        def capture_run_in_executor(executor, func, *args):
            # Extract the partial function arguments
            if hasattr(func, "func") and hasattr(func, "args"):
                # It's a functools.partial object
                nonlocal captured_topic, captured_payload
                captured_topic = func.args[0]  # topic is first arg
                captured_payload = func.args[1]  # payload is second arg
            # Return a completed future
            future = asyncio.Future()
            future.set_result(MagicMock(rc=0))
            return future

        with patch("asyncio.get_running_loop") as mock_get_loop:
            mock_loop = MagicMock()
            mock_get_loop.return_value = mock_loop
            mock_loop.run_in_executor.side_effect = capture_run_in_executor

            await client.publish(test_message, topic="sensors/temperature")
            mock_loop.run_in_executor.assert_called_once()

        # Verify the actual data sent
        assert captured_topic == "sensors/temperature"
        assert captured_payload is not None

        # Decode and verify JSON payload
        decoded_data = orjson.loads(captured_payload)
        assert isinstance(decoded_data, dict)  # Single message, not wrapped in array
        assert decoded_data["sensor"] == "temperature"
        assert decoded_data["value"] == 23.5
        assert decoded_data["unit"] == "celsius"

        await client.shutdown()

    @pytest.mark.asyncio
    @patch("rdp_mqtt.mqtt_client.mqtt.Client")
    async def test_sparkplug_usage_example(self, mock_mqtt_client_class):
        """Test the SparkplugB usage example from README"""
        # Setup mock
        mock_paho_client = MagicMock()
        mock_mqtt_client_class.return_value = mock_paho_client

        # SparkplugB settings from README example
        settings = MqttSettings(
            host="localhost",
            port=1883,
            ssl=False,
            payload_parser="sparkplug",
            sparkplug_group_id="MyGroup",
            sparkplug_node_id="MyNode",
            sparkplug_device_id="MyDevice",
            batch_size=10,
            batch_timeout=5.0,
        )

        client = MqttClient(settings)
        await client.setup()

        # Verify sparkplug settings are applied
        assert client.settings.payload_parser == "sparkplug"
        assert client.settings.sparkplug_group_id == "MyGroup"
        assert client.settings.sparkplug_node_id == "MyNode"
        assert client.settings.sparkplug_device_id == "MyDevice"
        assert client.settings.batch_size == 10
        assert client.settings.batch_timeout == 5.0

        # Test publish metric from README example
        test_metric = {
            "timestamp": 1737800200000,  # Unix timestamp in milliseconds
            "voltage": 230.0,
            "current": 10.5,
            "status": "online",
        }

        # Since batch_size=10, this should add to pending metrics
        await client.publish(test_metric)
        assert len(client._pending_metrics) == 1

        # Capture SparkplugB publish calls during shutdown
        captured_payloads = []
        captured_topics = []

        def capture_sparkplug_run_in_executor(executor, func, *args):
            # Extract the partial function arguments
            if hasattr(func, "func") and hasattr(func, "args"):
                # It's a functools.partial object
                captured_topics.append(func.args[0])  # topic is first arg
                captured_payloads.append(func.args[1])  # payload is second arg
            # Return a completed future
            future = asyncio.Future()
            future.set_result(MagicMock(rc=0))
            return future

        with patch("asyncio.get_running_loop") as mock_get_loop:
            mock_loop = MagicMock()
            mock_get_loop.return_value = mock_loop
            mock_loop.run_in_executor.side_effect = capture_sparkplug_run_in_executor

            # Force batch send to verify SparkplugB encoding
            await client.shutdown()

        # Should have sent DBIRTH and DDATA messages
        assert len(captured_topics) >= 2
        assert len(captured_payloads) >= 2

        # Verify DBIRTH topic
        dbirth_topic = "spBv1.0/MyGroup/DBIRTH/MyNode/MyDevice"
        assert dbirth_topic in captured_topics

        # Verify DDATA topic
        ddata_topic = "spBv1.0/MyGroup/DDATA/MyNode/MyDevice"
        assert ddata_topic in captured_topics

        # Find and decode the DDATA payload
        ddata_index = captured_topics.index(ddata_topic)
        ddata_payload = captured_payloads[ddata_index]

        # Decode SparkplugB payload
        sparkplug_payload = sparkplug_b_pb2.Payload()
        sparkplug_payload.ParseFromString(ddata_payload)

        # Verify it's a SparkplugB payload with our data
        assert len(sparkplug_payload.metrics) >= 1

        # Verify at least one metric has dataset type
        has_dataset = False
        for metric in sparkplug_payload.metrics:
            if metric.datatype == 16:  # DataSet type
                has_dataset = True
                dataset = metric.dataset_value
                # Verify we have data columns
                assert dataset.num_of_columns > 0
                # Verify we have at least one row
                assert len(dataset.rows) >= 1
                break

        assert has_dataset, "Should have at least one dataset metric"

    @pytest.mark.asyncio
    @patch("rdp_mqtt.mqtt_client.mqtt.Client")
    async def test_json_publisher_example(self, mock_mqtt_client_class):
        """Test the JSON publisher example from README"""
        # Setup mock
        mock_paho_client = MagicMock()
        mock_mqtt_client_class.return_value = mock_paho_client

        # Publisher settings from README
        settings = MqttSettings(host="broker.example.com", payload_parser="json", subscribe=False)  # Publisher only

        client = MqttClient(settings)
        await client.setup()

        # Verify settings
        assert client.settings.subscribe is False
        assert client.settings.payload_parser == "json"

        # Test publish from README example
        test_message = {"device_id": "sensor_001", "temperature": 25.3, "timestamp": "2025-01-15T10:30:00Z"}

        # Capture the actual publish call through run_in_executor
        captured_payload = None
        captured_topic = None

        def capture_run_in_executor(executor, func, *args):
            # Extract the partial function arguments
            if hasattr(func, "func") and hasattr(func, "args"):
                # It's a functools.partial object
                nonlocal captured_topic, captured_payload
                captured_topic = func.args[0]  # topic is first arg
                captured_payload = func.args[1]  # payload is second arg
            # Return a completed future
            future = asyncio.Future()
            future.set_result(MagicMock(rc=0))
            return future

        with patch("asyncio.get_running_loop") as mock_get_loop:
            mock_loop = MagicMock()
            mock_get_loop.return_value = mock_loop
            mock_loop.run_in_executor.side_effect = capture_run_in_executor

            await client.publish(test_message, topic="sensors/temperature")
            mock_loop.run_in_executor.assert_called_once()

        # Verify the actual data sent
        assert captured_topic == "sensors/temperature"
        assert captured_payload is not None

        # Decode and verify JSON payload
        decoded_data = orjson.loads(captured_payload)
        assert isinstance(decoded_data, dict)  # Single message, not wrapped in array
        assert decoded_data["device_id"] == "sensor_001"
        assert decoded_data["temperature"] == 25.3
        assert decoded_data["timestamp"] == "2025-01-15T10:30:00Z"

        await client.shutdown()

    @pytest.mark.asyncio
    @patch("rdp_mqtt.mqtt_client.mqtt.Client")
    async def test_json_subscriber_example(self, mock_mqtt_client_class):
        """Test the JSON subscriber example from README"""
        # Setup mock
        mock_paho_client = MagicMock()
        mock_mqtt_client_class.return_value = mock_paho_client

        # Subscriber settings from README
        settings = MqttSettings(host="broker.example.com", topic="sensors/+", payload_parser="json")

        client = MqttClient(settings)
        await client.setup()

        # Verify subscriber settings
        assert client.settings.topic == "sensors/+"
        assert client.settings.payload_parser == "json"
        assert client.settings.subscribe is True  # Default

        # Simulate successful connection which triggers subscription
        # This is what happens in a real connection
        from paho.mqtt import client as mqtt

        MqttClient._on_connect(mock_paho_client, client, {}, mqtt.MQTT_ERR_SUCCESS, None)

        # Verify subscription was called after connection
        mock_paho_client.subscribe.assert_called()

        await client.shutdown()

    @pytest.mark.asyncio
    @patch("rdp_mqtt.mqtt_client.mqtt.Client")
    async def test_compressed_json_example(self, mock_mqtt_client_class):
        """Test the compressed JSON example from README"""
        # Setup mock
        mock_paho_client = MagicMock()
        mock_mqtt_client_class.return_value = mock_paho_client

        # Compressed JSON settings from README
        settings = MqttSettings(payload_parser="json_zstd", batch_size=10, field_precision=2)  # Automatic compression

        client = MqttClient(settings)
        await client.setup()

        # Verify settings
        assert client.settings.payload_parser == "json_zstd"
        assert client.settings.batch_size == 10
        assert client.settings.field_precision == 2

        # Test large message from README example
        test_message = {
            "device": "data_logger",
            "large_dataset": list(range(100)),  # Smaller for test
            "metadata": "compressed_transmission",
        }

        # Should add to pending metrics due to batch_size=10
        await client.publish(test_message)
        assert len(client._pending_metrics) == 1

        # Capture compressed publish call on shutdown
        captured_payloads = []
        captured_topics = []

        def capture_compressed_run_in_executor(executor, func, *args):
            # Extract the partial function arguments
            if hasattr(func, "func") and hasattr(func, "args"):
                # It's a functools.partial object
                captured_topics.append(func.args[0])  # topic is first arg
                captured_payloads.append(func.args[1])  # payload is second arg
            # Return a completed future
            future = asyncio.Future()
            future.set_result(MagicMock(rc=0))
            return future

        with patch("asyncio.get_running_loop") as mock_get_loop:
            mock_loop = MagicMock()
            mock_get_loop.return_value = mock_loop
            mock_loop.run_in_executor.side_effect = capture_compressed_run_in_executor

            # Force batch send to verify compression
            await client.shutdown()

        # Verify data was sent
        assert len(captured_topics) > 0
        assert len(captured_payloads) > 0

        # Find the compressed payload (should be bytes)
        compressed_payload = None
        for payload in captured_payloads:
            if isinstance(payload, bytes):
                compressed_payload = payload
                break

        assert compressed_payload is not None

        # Decompress and verify content
        decompressor = zstd.ZstdDecompressor()
        decompressed_data = decompressor.decompress(compressed_payload)

        # Should be JSON after decompression
        decoded_data = orjson.loads(decompressed_data)

        # Could be a single object or a list depending on batch size
        if isinstance(decoded_data, list):
            assert len(decoded_data) == 1
            sent_message = decoded_data[0]
        else:
            sent_message = decoded_data

        assert sent_message["device"] == "data_logger"
        assert sent_message["metadata"] == "compressed_transmission"
        assert len(sent_message["large_dataset"]) == 100
        assert sent_message["large_dataset"][0] == 0
        assert sent_message["large_dataset"][-1] == 99

    @pytest.mark.asyncio
    @patch("rdp_mqtt.mqtt_client.mqtt.Client")
    async def test_sparkplug_with_batching_example(self, mock_mqtt_client_class):
        """Test the SparkplugB with batching example from README"""
        # Setup mock
        mock_paho_client = MagicMock()
        mock_mqtt_client_class.return_value = mock_paho_client

        # SparkplugB with batching settings from README
        settings = MqttSettings(
            payload_parser="sparkplug",
            sparkplug_group_id="FactoryFloor",
            sparkplug_node_id="Line1",
            sparkplug_device_id="Sensor01",
            batch_size=20,
            batch_timeout=5.0,
        )

        client = MqttClient(settings)
        await client.setup()

        # Verify settings
        assert client.settings.sparkplug_group_id == "FactoryFloor"
        assert client.settings.sparkplug_node_id == "Line1"
        assert client.settings.sparkplug_device_id == "Sensor01"
        assert client.settings.batch_size == 20

        # Test the batching loop from README (sending multiple readings)
        current_time = int(time.time() * 1000)
        test_readings = []

        for i in range(5):  # Send fewer messages for test
            test_reading = {
                "timestamp": current_time + i,  # Current time in ms
                "reading_id": i,
                "value": 100 + i * 0.5,
                "quality": 1.0 if i % 10 != 0 else 0.8,
            }
            test_readings.append(test_reading)
            await client.publish(test_reading)

        # Should have 5 pending metrics (batch_size=20, so not sent yet)
        assert len(client._pending_metrics) == 5

        # Capture SparkplugB batching publish calls during shutdown
        captured_payloads = []
        captured_topics = []

        def capture_batching_run_in_executor(executor, func, *args):
            # Extract the partial function arguments
            if hasattr(func, "func") and hasattr(func, "args"):
                # It's a functools.partial object
                captured_topics.append(func.args[0])  # topic is first arg
                captured_payloads.append(func.args[1])  # payload is second arg
            # Return a completed future
            future = asyncio.Future()
            future.set_result(MagicMock(rc=0))
            return future

        with patch("asyncio.get_running_loop") as mock_get_loop:
            mock_loop = MagicMock()
            mock_get_loop.return_value = mock_loop
            mock_loop.run_in_executor.side_effect = capture_batching_run_in_executor

            # Force batch send to verify batching
            await client.shutdown()

        # Should have sent DBIRTH and DDATA messages
        assert len(captured_topics) >= 2

        # Verify DBIRTH topic
        dbirth_topic = "spBv1.0/FactoryFloor/DBIRTH/Line1/Sensor01"
        assert dbirth_topic in captured_topics

        # Verify DDATA topic
        ddata_topic = "spBv1.0/FactoryFloor/DDATA/Line1/Sensor01"
        assert ddata_topic in captured_topics

        # Find and decode the DDATA payload (batched data)
        ddata_index = captured_topics.index(ddata_topic)
        ddata_payload = captured_payloads[ddata_index]

        # Decode SparkplugB payload
        sparkplug_payload = sparkplug_b_pb2.Payload()
        sparkplug_payload.ParseFromString(ddata_payload)

        # Verify it's a SparkplugB payload with batched data
        assert len(sparkplug_payload.metrics) >= 1

        # Verify we have dataset metrics with our batch data
        total_rows = 0
        for metric in sparkplug_payload.metrics:
            if metric.datatype == 16:  # DataSet type
                dataset = metric.dataset_value
                # Count total rows across all dataset metrics
                total_rows += len(dataset.rows)
                # Verify each row has data
                for row in dataset.rows:
                    assert len(row.elements) > 0

        # Should have our 5 readings represented somewhere in the metrics
        assert total_rows >= 5, f"Expected at least 5 rows, got {total_rows}"

    def test_mqtt_settings_examples(self):
        """Test various MqttSettings configurations from README"""
        # Test default settings mentioned in README
        default_settings = MqttSettings()
        assert default_settings.host == "localhost"
        assert default_settings.port == 8883
        assert default_settings.ssl is True
        assert default_settings.payload_parser == "json"
        assert default_settings.batch_size == 0
        assert default_settings.qos == 0

        # Test custom settings example from README
        custom_settings = MqttSettings(
            host="custom.broker.com",
            port=1883,
            ssl=False,
            topic="custom/topic",
            payload_parser="sparkplug",
            batch_size=100,
            batch_timeout=5.0,
            sparkplug_group_id="TestGroup",
        )

        assert custom_settings.host == "custom.broker.com"
        assert custom_settings.port == 1883
        assert custom_settings.ssl is False
        assert custom_settings.topic == "custom/topic"
        assert custom_settings.payload_parser == "sparkplug"
        assert custom_settings.batch_size == 100
        assert custom_settings.batch_timeout == 5.0
        assert custom_settings.sparkplug_group_id == "TestGroup"
