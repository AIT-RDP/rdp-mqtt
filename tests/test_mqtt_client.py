import asyncio
import datetime
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import orjson
import pytest

from rdp_mqtt.mqtt_client import MqttClient, MqttSettings
from rdp_mqtt.sparkplug.generated import sparkplug_b_pb2
from rdp_mqtt.sparkplug.sparkplug_decode import decode_mqtt_message
from rdp_mqtt.sparkplug.sparkplug_encode import (
    encode_data_message,
    get_sparkplug_topic,
)

logger = logging.getLogger(__name__)


class TestMqttSettings:
    """Test MQTT settings configuration"""

    def test_mqtt_settings_defaults(self):
        """Test default MQTT settings values"""
        settings = MqttSettings()
        assert settings.host == "localhost"
        assert settings.port == 8883
        assert settings.ssl is True
        assert settings.payload_parser == "json"
        assert settings.batch_size == 0
        assert settings.qos == 0

    def test_mqtt_settings_custom_values(self):
        """Test custom MQTT settings"""
        settings = MqttSettings(
            host="custom.broker.com",
            port=1883,
            ssl=False,
            username="testuser",
            topic="custom/topic",
            payload_parser="sparkplug",
            batch_size=100,
            batch_timeout=5.0,
            sparkplug_group_id="TestGroup",
        )

        assert settings.host == "custom.broker.com"
        assert settings.port == 1883
        assert settings.ssl is False
        assert settings.username == "testuser"
        assert settings.topic == "custom/topic"
        assert settings.payload_parser == "sparkplug"
        assert settings.batch_size == 100
        assert settings.batch_timeout == 5.0
        assert settings.sparkplug_group_id == "TestGroup"


class TestMqttClient:
    """Test core MQTT client functionality"""

    @pytest.fixture
    def mqtt_settings(self):
        """Basic MQTT settings for testing"""
        return MqttSettings(host="test.broker.com", port=1883, ssl=False, topic="test/topic")

    @pytest.fixture
    def mqtt_client(self, mqtt_settings):
        """MQTT client instance for testing"""
        return MqttClient(mqtt_settings)

    def test_mqtt_client_creation(self, mqtt_client):
        """Test MQTT client instantiation"""
        assert mqtt_client._client is None
        assert mqtt_client._incoming is not None
        assert isinstance(mqtt_client._pending_metrics, list)
        assert len(mqtt_client._pending_metrics) == 0

    @pytest.mark.asyncio
    @patch("rdp_mqtt.mqtt_client.mqtt.Client")
    async def test_mqtt_client_setup(self, mock_mqtt_client_class, mqtt_client):
        """Test MQTT client setup process"""
        mock_paho_client = MagicMock()
        mock_mqtt_client_class.return_value = mock_paho_client

        await mqtt_client.setup()

        # Verify client creation and configuration
        mock_mqtt_client_class.assert_called_once()
        mock_paho_client.connect_async.assert_called_once_with("test.broker.com", 1883, keepalive=5)
        mock_paho_client.loop_start.assert_called_once()

        # Verify callbacks are set
        assert mock_paho_client.on_connect is not None
        assert mock_paho_client.on_disconnect is not None
        assert mock_paho_client.on_message is not None

    @pytest.mark.asyncio
    @patch("rdp_mqtt.mqtt_client.mqtt.Client")
    async def test_mqtt_client_ssl_setup(self, mock_mqtt_client_class):
        """Test MQTT client SSL configuration"""
        settings = MqttSettings(ssl=True, validate_certificate=False)
        client = MqttClient(settings)

        mock_paho_client = MagicMock()
        mock_mqtt_client_class.return_value = mock_paho_client

        with patch("rdp_mqtt.mqtt_client.ssl.create_default_context") as mock_ssl_context:
            mock_context = MagicMock()
            mock_ssl_context.return_value = mock_context

            await client.setup()

            mock_ssl_context.assert_called_once()
            mock_context.check_hostname = False
            mock_context.verify_mode = False  # ssl.CERT_NONE
            mock_paho_client.tls_set_context.assert_called_once_with(mock_context)

    @pytest.mark.asyncio
    @patch("rdp_mqtt.mqtt_client.mqtt.Client")
    async def test_mqtt_client_auth_setup(self, mock_mqtt_client_class):
        """Test MQTT client authentication setup"""
        from pydantic import SecretStr

        settings = MqttSettings(username="testuser", password=SecretStr("testpass"))
        client = MqttClient(settings)

        mock_paho_client = MagicMock()
        mock_mqtt_client_class.return_value = mock_paho_client

        await client.setup()

        mock_paho_client.username_pw_set.assert_called_once_with("testuser", "testpass")

    @pytest.mark.asyncio
    @patch("rdp_mqtt.mqtt_client.mqtt.Client")
    async def test_mqtt_client_publish(self, mock_mqtt_client_class, mqtt_client):
        """Test publishing messages"""
        mock_paho_client = MagicMock()
        mock_mqtt_client_class.return_value = mock_paho_client

        await mqtt_client.setup()

        test_data = {"sensor": "temperature", "value": 23.5, "timestamp": "2024-01-01T12:00:00Z"}

        with patch("asyncio.get_running_loop") as mock_get_loop:
            mock_loop = MagicMock()
            mock_get_loop.return_value = mock_loop
            # Make run_in_executor return a future that resolves immediately
            mock_loop.run_in_executor.return_value = asyncio.Future()
            mock_loop.run_in_executor.return_value.set_result(None)

            await mqtt_client.publish(test_data, "test/publish/topic")

            mock_loop.run_in_executor.assert_called_once()

    @pytest.mark.asyncio
    async def test_mqtt_client_shutdown(self, mqtt_client):
        """Test MQTT client shutdown"""
        with patch("rdp_mqtt.mqtt_client.mqtt.Client") as mock_mqtt_client_class:
            mock_paho_client = MagicMock()
            mock_mqtt_client_class.return_value = mock_paho_client

            await mqtt_client.setup()
            await mqtt_client.shutdown()

            mock_paho_client.loop_stop.assert_called_once()
            mock_paho_client.disconnect.assert_called_once()


class TestMqttBatching:
    """Test MQTT batching functionality"""

    @pytest.mark.asyncio
    async def test_batching_disabled(self):
        """Test publishing with batching disabled"""
        settings = MqttSettings(batch_size=0, ssl=False)
        client = MqttClient(settings)

        with patch("rdp_mqtt.mqtt_client.mqtt.Client") as mock_mqtt_client_class:
            mock_paho_client = MagicMock()
            mock_mqtt_client_class.return_value = mock_paho_client

            await client.setup()

            test_data = {"value": 42}

            with patch.object(client, "_send_batch", new_callable=AsyncMock) as mock_send_batch:
                await client.publish(test_data, "test/topic")
                mock_send_batch.assert_called_once_with([test_data], "test/topic")

    @pytest.mark.asyncio
    async def test_batching_enabled(self):
        """Test publishing with batching enabled"""
        settings = MqttSettings(batch_size=3, batch_timeout=1.0, ssl=False)
        client = MqttClient(settings)

        with patch("rdp_mqtt.mqtt_client.mqtt.Client") as mock_mqtt_client_class:
            mock_paho_client = MagicMock()
            mock_mqtt_client_class.return_value = mock_paho_client
            await client.setup()

            # Mock the actual publish call and run_in_executor
            with patch("asyncio.get_running_loop") as mock_get_loop:
                mock_loop = MagicMock()
                mock_get_loop.return_value = mock_loop
                # Make run_in_executor return a completed future
                mock_future = asyncio.Future()
                mock_future.set_result(None)
                mock_loop.run_in_executor.return_value = mock_future

                # Add messages to batch
                await client.publish({"value": 1}, "test/topic")
                await client.publish({"value": 2}, "test/topic")

                # Batch not full yet, should have pending metrics
                assert len(client._pending_metrics) == 2

                # Third message should trigger batch send
                await client.publish({"value": 3}, "test/topic")

                # Should have cleared the batch
                assert len(client._pending_metrics) == 0
                # Should have called run_in_executor (meaning it sent the batch)
                assert mock_loop.run_in_executor.called


class TestMqttSparkplug:
    """Test Sparkplug B protocol functionality"""

    @pytest.fixture
    def sparkplug_settings(self):
        """Sparkplug-enabled MQTT settings"""
        return MqttSettings(
            payload_parser="sparkplug",
            sparkplug_group_id="TestGroup",
            sparkplug_node_id="TestNode",
            sparkplug_device_id="TestDevice",
            ssl=False,
        )

    @pytest.mark.asyncio
    async def test_sparkplug_settings_validation(self, sparkplug_settings):
        """Test Sparkplug settings validation"""
        client = MqttClient(sparkplug_settings)

        # Test that client accepts Sparkplug settings
        assert client.settings.payload_parser == "sparkplug"
        assert client.settings.sparkplug_group_id == "TestGroup"
        assert client.settings.sparkplug_node_id == "TestNode"
        assert client.settings.sparkplug_device_id == "TestDevice"

    def test_sparkplug_decode_basic(self):
        """Test basic Sparkplug message decoding"""
        # Create a minimal Sparkplug message
        payload = sparkplug_b_pb2.Payload()
        payload.timestamp = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)

        # Add a simple metric
        metric = payload.metrics.add()
        metric.name = "Temperature"
        metric.int_value = 25
        metric.datatype = 1  # Int32

        # Create mock MQTT message
        mock_message = MagicMock()
        mock_message.topic = "spBv1.0/TestGroup/NDATA/TestNode/TestDevice"
        mock_message.payload = payload.SerializeToString()

        # Test decode
        result = decode_mqtt_message(mock_message)

        logger.debug(result)

        assert result is not None
        assert len(result) > 0

    def test_sparkplug_decode_dataset(self):
        """Test decoding Sparkplug dataset messages"""
        # Create a sample dataset payload
        payload = sparkplug_b_pb2.Payload()

        # Add a dataset metric
        metric = payload.metrics.add()
        metric.name = "DatasetTest"
        metric.datatype = 16  # DataSet
        metric.timestamp = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)

        # Add columns to dataset
        dataset = metric.dataset_value
        dataset.num_of_columns = 3
        dataset.columns.extend(["Time", "Value", "Quality"])
        dataset.types.extend([8, 10, 10])

        # Add a row of data
        row = dataset.rows.add()
        ts_element = row.elements.add()
        ts_element.long_value = 1662033600000000
        val_element = row.elements.add()
        val_element.double_value = 42.7
        qual_element = row.elements.add()
        qual_element.double_value = 25

        # Create mock MQTT message
        mock_message = MagicMock()
        mock_message.topic = "spBv1.0/TestGroup/NDATA/TestNode/TestDevice"
        mock_message.payload = payload.SerializeToString()

        # Test decode
        result = decode_mqtt_message(mock_message)

        assert result is not None
        assert len(result) > 0
        assert result[0]["name"] == "DatasetTest"
        assert (
            result[0]["timestamp"]
            == datetime.datetime.fromtimestamp(metric.timestamp / 1000, datetime.timezone.utc).isoformat()
        )
        assert result[0]["Time"] == 1662033600000000
        assert result[0]["Value"] == 42.7
        assert result[0]["Quality"] == 25

        logger.debug(result)

    def test_sparkplug_encode_dataset(self):
        """Test basic Sparkplug message encoding"""
        test_metrics = [
            {
                "name": "Temperature Sensor",
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "temperature": 25,
                "humidity": 49.5,
                "status": "OK",
            }
        ]

        # Test encode
        result = encode_data_message(test_metrics)

        logger.debug(sparkplug_b_pb2.Payload().FromString(result))

        assert result is not None
        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_sparkplug_topic_generation_from_settings(self):
        """Test that sparkplug topics are generated correctly based on settings"""
        # Test with default settings (random UUIDs)
        settings = MqttSettings(payload_parser="sparkplug")

        # Test NBIRTH topic
        nbirth_topic = get_sparkplug_topic(settings.sparkplug_group_id, settings.sparkplug_node_id, "NBIRTH")
        expected_nbirth = f"spBv1.0/{settings.sparkplug_group_id}/NBIRTH/{settings.sparkplug_node_id}"
        assert nbirth_topic == expected_nbirth

        # Test NDATA topic
        ndata_topic = get_sparkplug_topic(settings.sparkplug_group_id, settings.sparkplug_node_id, "NDATA")
        expected_ndata = f"spBv1.0/{settings.sparkplug_group_id}/NDATA/{settings.sparkplug_node_id}"
        assert ndata_topic == expected_ndata

        # Test DBIRTH topic (device birth)
        dbirth_topic = get_sparkplug_topic(settings.sparkplug_group_id, settings.sparkplug_node_id, "DBIRTH")
        expected_dbirth = f"spBv1.0/{settings.sparkplug_group_id}/DBIRTH/{settings.sparkplug_node_id}"
        assert dbirth_topic == expected_dbirth

        # Test DDATA topic (device data)
        ddata_topic = get_sparkplug_topic(settings.sparkplug_group_id, settings.sparkplug_node_id, "DDATA")
        expected_ddata = f"spBv1.0/{settings.sparkplug_group_id}/DDATA/{settings.sparkplug_node_id}"
        assert ddata_topic == expected_ddata

    def test_sparkplug_topic_generation_with_custom_settings(self):
        """Test sparkplug topic generation with custom group/node IDs"""
        settings = MqttSettings(
            payload_parser="sparkplug",
            sparkplug_group_id="CustomGroup",
            sparkplug_node_id="CustomNode",
            sparkplug_device_id="CustomDevice",
        )

        # Test various message types with custom IDs
        test_cases = [
            ("NBIRTH", "spBv1.0/CustomGroup/NBIRTH/CustomNode"),
            ("NDEATH", "spBv1.0/CustomGroup/NDEATH/CustomNode"),
            ("DBIRTH", "spBv1.0/CustomGroup/DBIRTH/CustomNode"),
            ("DDEATH", "spBv1.0/CustomGroup/DDEATH/CustomNode"),
            ("NDATA", "spBv1.0/CustomGroup/NDATA/CustomNode"),
            ("DDATA", "spBv1.0/CustomGroup/DDATA/CustomNode"),
            ("NCMD", "spBv1.0/CustomGroup/NCMD/CustomNode"),
            ("DCMD", "spBv1.0/CustomGroup/DCMD/CustomNode"),
        ]

        for message_type, expected_topic in test_cases:
            topic = get_sparkplug_topic(settings.sparkplug_group_id, settings.sparkplug_node_id, message_type)
            assert topic == expected_topic, f"Failed for message type {message_type}"

    def test_sparkplug_topic_format_validation(self):
        """Test that sparkplug topics follow the correct format"""
        settings = MqttSettings(payload_parser="sparkplug", sparkplug_group_id="Group123", sparkplug_node_id="Node456")

        topic = get_sparkplug_topic(settings.sparkplug_group_id, settings.sparkplug_node_id, "NDATA")

        # Validate topic structure
        parts = topic.split("/")
        assert len(parts) == 4, "Sparkplug topic should have 4 parts"
        assert parts[0] == "spBv1.0", "First part should be spBv1.0"
        assert parts[1] == "Group123", "Second part should be group ID"
        assert parts[2] == "NDATA", "Third part should be message type"
        assert parts[3] == "Node456", "Fourth part should be node ID"


class TestMqttPayloadParsers:
    """Test different payload parser implementations"""

    @pytest.mark.asyncio
    async def test_json_payload_parser(self):
        """Test JSON payload parsing"""
        settings = MqttSettings(payload_parser="json", ssl=False)
        client = MqttClient(settings)

        test_message = {"sensor": "temp01", "value": 23.5, "unit": "celsius"}
        test_payload = orjson.dumps(test_message)

        # Test JSON decoding
        result = client._decode_json(test_payload)

        assert result is not None
        assert len(result) == 1
        assert result[0]["sensor"] == "temp01"
        assert result[0]["value"] == 23.5

    @pytest.mark.asyncio
    async def test_json_zstd_payload_parser(self):
        """Test JSON with ZSTD compression"""
        settings = MqttSettings(payload_parser="json_zstd", ssl=False)
        client = MqttClient(settings)

        # Setup compressor/decompressor
        await client.setup()

        test_message = {"sensor": "temp01", "value": 23.5, "data": "x" * 1000}  # Larger data
        json_payload = orjson.dumps(test_message)
        compressed_payload = client._zstd_compressor.compress(json_payload)

        # Test decompression and decoding
        decompressed = client._zstd_decompressor.decompress(compressed_payload)
        result = client._decode_json(decompressed)

        assert result is not None
        assert result[0]["sensor"] == "temp01"
        assert len(compressed_payload) < len(json_payload)  # Should be compressed


class TestMqttCallbacks:
    """Test MQTT callback functions"""

    def test_on_connect_callback_success(self):
        """Test successful connection callback"""
        settings = MqttSettings(ssl=False)
        client = MqttClient(settings)

        mock_paho_client = MagicMock()
        mock_paho_client.subscribe = MagicMock()

        # Test successful connection
        MqttClient._on_connect(mock_paho_client, client, {}, 0, None)

        # Should subscribe to topic
        mock_paho_client.subscribe.assert_called_once()

    def test_on_connect_callback_failure(self):
        """Test failed connection callback"""
        settings = MqttSettings(ssl=False)
        client = MqttClient(settings)

        mock_paho_client = MagicMock()

        # Test failed connection
        MqttClient._on_connect(mock_paho_client, client, {}, 1, None)  # rc=1 is failure

        # Should not subscribe on failure
        mock_paho_client.subscribe.assert_not_called()

    def test_on_disconnect_callback(self):
        """Test disconnection callback"""
        settings = MqttSettings(ssl=False)
        client = MqttClient(settings)

        mock_paho_client = MagicMock()

        # Test unexpected disconnection
        MqttClient._on_disconnect(mock_paho_client, client, 1, None)  # rc=1 unexpected

        # Should attempt reconnection
        mock_paho_client.reconnect.assert_called_once()


# Test fixtures and utilities
@pytest.fixture
def sample_sparkplug_message():
    """Sample Sparkplug message for testing"""
    return {
        "name": "TestSensor",
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "temperature": 23.5,
        "humidity": 67.8,
        "status": "online",
    }


@pytest.fixture
def sample_json_message():
    """Sample JSON message for testing"""
    return {"sensor_id": "temp_001", "value": 25.3, "unit": "celsius", "timestamp": "2024-01-01T12:00:00Z"}
