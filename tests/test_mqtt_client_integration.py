import asyncio
import contextlib
import datetime
import logging
import time
import uuid
from typing import AsyncGenerator

import orjson
import pytest
from paho.mqtt.client import Client as PahoMqttClient

from rdp_mqtt.mqtt_client import MqttClient, MqttSettings
from rdp_mqtt.sparkplug.generated import sparkplug_b_pb2
from rdp_mqtt.sparkplug.sparkplug_decode import decode_mqtt_message
from rdp_mqtt.sparkplug.sparkplug_encode import encode_birth_message, encode_data_message

logger = logging.getLogger(__name__)


class MockSink:
    def __init__(self):
        self.write_queue = asyncio.Queue()

    async def put(self, data):
        await self.write_queue.put(data)


async def create_connected_mqtt_client(broker: str = "test.mosquitto.org", port: int = 1883) -> PahoMqttClient:
    """Create and connect a Paho MQTT client with proper connection handling and retries."""

    max_retries = 3
    retry_delay = 2
    client_id = f"client-{uuid.uuid4()}"

    for attempt in range(max_retries):
        client = PahoMqttClient(client_id=f"{client_id}_{attempt}")
        connected = asyncio.Event()
        connect_failed = asyncio.Event()
        loop = asyncio.get_running_loop()

        def on_connect(
            client, userdata, flags, rc, properties=None, loop=loop, connected=connected, connect_failed=connect_failed
        ):
            if rc == 0:
                if not loop.is_closed():
                    loop.call_soon_threadsafe(connected.set)
            else:
                if not loop.is_closed():
                    loop.call_soon_threadsafe(connect_failed.set)

        client.on_connect = on_connect

        try:
            client.connect(broker, port, 60)
            client.loop_start()

            await asyncio.wait(
                [asyncio.create_task(connected.wait()), asyncio.create_task(connect_failed.wait())],
                timeout=15,  # Shorter timeout per attempt
                return_when=asyncio.FIRST_COMPLETED,
            )

            if connected.is_set() and not connect_failed.is_set():
                # Success - give a moment for the connection to stabilize
                await asyncio.sleep(0.5)
                return client
            else:
                # Failed - clean up and try again
                client.loop_stop()
                client.disconnect()

        except Exception:
            # Connection error - clean up and try again
            try:
                client.loop_stop()
                client.disconnect()
            except Exception:
                pass

        if attempt < max_retries - 1:
            await asyncio.sleep(retry_delay)

    raise ConnectionError(f"MQTT broker connection failed after {max_retries} attempts")


@pytest.fixture(scope="function")
async def mqtt_client_session() -> AsyncGenerator[PahoMqttClient, None]:
    """Session-scoped fixture for a connected Paho MQTT client."""
    client = None
    try:
        client = await create_connected_mqtt_client()
        yield client
    except ConnectionError:
        pytest.fail("MQTT broker connection failed or timed out.")
    finally:
        if client:
            client.loop_stop()
            client.disconnect()


@pytest.fixture
def complex_weather_message():
    """Complex weather station message for testing JSON serialization"""
    return {
        "device": "weather_station_pro_v2",
        "metadata": {
            "firmware_version": "3.2.1",
            "serial_number": "WS-2024-001337",
            "manufacturer": {
                "name": "WeatherTech Solutions",
                "country": "Austria",
                "certifications": ["ISO9001", "CE", "FCC"],
            },
        },
        "measurements": {
            "environmental": {
                "temperature": {"value": 23.5, "unit": "celsius", "precision": 0.1, "calibration_date": "2024-12-01"},
                "humidity": {"value": 65.2, "unit": "percent", "range": {"min": 0, "max": 100}},
                "pressure": {"value": 1013.25, "unit": "hPa", "sea_level_adjusted": True},
            },
            "wind": {
                "speed": {"current": 12.3, "max_gust": 18.7, "unit": "m/s"},
                "direction": {"degrees": 245, "cardinal": "SW", "variance": 15},
            },
            "air_quality": {"pm25": 8.2, "pm10": 15.4, "ozone": 45.1, "no2": 12.8, "aqi_index": 42, "status": "good"},
        },
        "location": {
            "city": "Vienna",
            "coordinates": {"latitude": 48.2082, "longitude": 16.3738, "elevation": 171},
            "timezone": "Europe/Vienna",
            "region": "Österreich",
        },
        "operational_status": {
            "battery_level": 87.3,
            "signal_strength": -45,
            "uptime_hours": 1337,
            "last_maintenance": "2024-11-15T09:30:00Z",
            "alerts": [
                {"type": "low_battery_warning", "threshold": 20, "active": False},
                {"type": "sensor_drift", "sensor": "temperature", "deviation": 0.05, "active": False},
            ],
        },
        "data_quality": {
            "completeness": 99.8,
            "accuracy_score": 0.97,
            "missing_readings": 0,
            "error_count": 1,
            "validation_flags": {"range_check": True, "consistency_check": True, "temporal_check": True},
        },
        "timestamp": "2025-01-15T10:30:00Z",
        "data_version": "1.4",
        "tags": ["outdoor", "urban", "primary_station", "real_time"],
        "processing_info": {
            "collected_at": "2025-01-15T10:29:58.123Z",
            "processed_at": "2025-01-15T10:30:00.456Z",
            "processing_time_ms": 2333,
            "pipeline_version": "v2.1.0",
        },
    }


@pytest.mark.timeout(40)
async def test_mqtt_connection(mqtt_client_session: PahoMqttClient):
    """Test that the MqttClient can connect to the broker."""
    assert mqtt_client_session.is_connected()


@pytest.mark.timeout(60)
async def test_write_json_metric(mqtt_client_session: PahoMqttClient):
    """Test writing a single metric to a unique topic and verifying it."""
    topic = f"test/write/{uuid.uuid4()}"
    metric = {"name": "test_metric", "value": 123, "timestamp": "2025-07-19T12:00:00Z"}

    loop = asyncio.get_running_loop()
    received_message = asyncio.Future()

    def on_message(client, userdata, msg):
        if msg.topic == topic and not loop.is_closed():
            loop.call_soon_threadsafe(received_message.set_result, msg.payload)

    mqtt_client_session.subscribe(topic)
    mqtt_client_session.on_message = on_message

    settings = MqttSettings(
        host="test.mosquitto.org",
        port=1883,
        ssl=False,
        topic=topic,
        identifier=f"writer-{uuid.uuid4()}",
        payload_parser="json",
    )
    writer_device = MqttClient(settings=settings)
    await writer_device.setup()

    # Wait for MQTT client to connect before attempting to write
    await asyncio.sleep(2)  # Give time for connection

    try:
        await writer_device.publish(metric, topic=topic)

        payload = await asyncio.wait_for(received_message, timeout=30)

        data = orjson.loads(payload)

        assert data["name"] == "test_metric"
        assert data["value"] == 123
        assert data["timestamp"] == "2025-07-19T12:00:00Z"
    finally:
        await writer_device.shutdown()


@pytest.mark.timeout(40)
async def test_read_metric(mqtt_client_session: PahoMqttClient):
    """Test reading a single metric from a unique topic."""
    unique_topic = f"reader/test/{uuid.uuid4()}"
    sent_value = "data_from_reader"
    sent_metric = {"name": "read_metric", "value": sent_value, "timestamp": "2025-07-19T14:00:00Z"}

    # Subscriber setup
    reader_settings = MqttSettings(
        host="test.mosquitto.org",
        port=1883,
        ssl=False,
        topic=unique_topic,
        identifier=f"reader-{uuid.uuid4()}",
    )
    reader_device = MqttClient(settings=reader_settings)
    await reader_device.setup()

    await asyncio.sleep(2)  # Give reader time to connect

    try:
        # Publish the metric
        mqtt_client_session.publish(unique_topic, orjson.dumps(sent_metric))

        # Read the metric from the reader device's incoming queue
        received_metric = await asyncio.wait_for(reader_device._incoming.get(), timeout=10)

        assert received_metric is not None
        assert received_metric["name"] == sent_metric["name"]
        assert received_metric["value"] == sent_value
    except asyncio.TimeoutError:
        pytest.fail("Reader device did not receive metric in time.")
    finally:
        await reader_device.shutdown()


@pytest.mark.timeout(60)
async def test_writer_subscriber_interaction():
    """Test the full flow of writing and subscribing to a metric using a public broker."""
    mock_sink = MockSink()
    unique_topic = f"writer/test/{uuid.uuid4()}"

    # Writer setup
    writer_settings = MqttSettings(
        host="test.mosquitto.org",
        port=1883,
        ssl=False,
        subscribe=False,
        identifier=f"writer-{uuid.uuid4()}",
    )
    writer_device = MqttClient(settings=writer_settings)
    await writer_device.setup()

    # Subscriber setup
    subscriber_settings = MqttSettings(
        host="test.mosquitto.org",
        port=1883,
        ssl=False,
        topic=unique_topic,
        identifier=f"subscriber-{uuid.uuid4()}",
    )
    subscriber_device = MqttClient(settings=subscriber_settings)
    await subscriber_device.setup()

    sent_metric = {"name": "subscriber_metric", "value": "hello", "timestamp": "2025-07-19T13:00:00Z"}

    subscriber_task = None

    try:
        # Start subscriber
        async def subscribe_task():
            async for metric in subscriber_device.subscribe():
                await mock_sink.put(metric)

        subscriber_task = asyncio.create_task(subscribe_task())

        await asyncio.sleep(2)  # Give subscriber time to connect

        await writer_device.publish(sent_metric, topic=unique_topic)

        received_metric = await asyncio.wait_for(mock_sink.write_queue.get(), timeout=20)
        assert received_metric["name"] == "subscriber_metric"
        assert received_metric["value"] == "hello"
    except asyncio.TimeoutError:
        pytest.fail("Mock sink did not receive metric in time.")
    finally:
        if subscriber_task:
            subscriber_task.cancel()
        await writer_device.shutdown()
        await subscriber_device.shutdown()


@pytest.mark.timeout(60)
async def test_sparkplug_batching_size():
    """Test Sparkplug message batching functionality."""
    # Setup writer with batching enabled
    writer_settings, group_id, node_id, device_id, unique_topic_base = create_sparkplug_writer_settings(
        batch_size=4,
        batch_timeout=10,
    )

    # Setup a raw MQTT subscriber to capture the batched message
    subscriber_client = await create_connected_mqtt_client()
    received_messages = []

    def on_message(client, userdata, msg):
        logger.debug(f"Received message: {sparkplug_b_pb2.Payload().FromString(msg.payload)}")
        received_messages.append(msg)

    subscriber_client.subscribe(unique_topic_base + "/#")
    subscriber_client.on_message = on_message

    logger.debug("Subscribed to topic {}".format(unique_topic_base + "/#"))

    writer_device = MqttClient(settings=writer_settings)
    await writer_device.setup()

    try:
        await asyncio.sleep(2)  # Wait for NBIRTH

        # Send 3 metrics to trigger batching
        metrics = [{"name": "batch_metric1", "value": i * 10, "timestamp": f"2025-08-30T15:0{i}:00Z"} for i in range(4)]

        assert len(received_messages) == 1, "Only NBIRTH should be received"

        await writer_device.publish(metrics[0])
        await writer_device.publish(metrics[1])

        await asyncio.sleep(2)
        assert len(received_messages) == 1, "Only NBIRTH should be received"

        await writer_device.publish(metrics[2])

        await asyncio.sleep(2)
        assert len(received_messages) == 1, "Only NBIRTH should be received"

        await writer_device.publish(metrics[3])

        # Wait for the batch to be sent, we should now get DBIRTH and DDATA
        # Should be sent immediately because batch_size is 3
        await asyncio.sleep(2)

        # The message should parse successfully and contain metrics
        logger.debug(f"Received messages topics: {[msg.topic for msg in received_messages]}")

        # Should have received exactly one batched message and one NBIRTH and one DBIRTH
        # It seems like on the public broker some bad people are responding to my DDATA with Node/Rebirth
        # We ignore this by making this >= 3
        assert len(received_messages) >= 3

        decode_mqtt_message(received_messages[0])  # Parse NBIRTH --> does nothing
        decode_mqtt_message(received_messages[1])  # Parse DBIRTH --> fills alias
        parsed_metrics = decode_mqtt_message(received_messages[2])  # Parse DDATA --> has metrics

        assert len(parsed_metrics) == 4

        for i, parsed_metric in enumerate(parsed_metrics):
            assert parsed_metric["name"] == "batch_metric1"
            assert parsed_metric["value"] == i * 10

    except asyncio.TimeoutError:
        pytest.fail("Sparkplug batching integration test timed out")
    finally:
        await writer_device.shutdown()
        subscriber_client.loop_stop()
        subscriber_client.disconnect()


@pytest.mark.timeout(60)
async def test_sparkplug_batch_timeout():
    """Test Sparkplug message batching timeout functionality."""
    # Setup writer with batching enabled and short timeout
    writer_settings, group_id, node_id, device_id, unique_topic_base = create_sparkplug_writer_settings(
        batch_size=20,  # Larger than what we'll send
        batch_timeout=2,  # Short timeout to trigger batch send
    )

    # Setup a raw MQTT subscriber to capture the batched message
    subscriber_client = await create_connected_mqtt_client()
    received_messages = []
    message_received_time = None

    def on_message(client, userdata, msg):
        nonlocal message_received_time
        # Only track timing for DDATA messages (the actual batched data)
        if "/DDATA/" in msg.topic and message_received_time is None:
            message_received_time = time.time()
        logger.debug(f"Received message on topic {msg.topic}: {sparkplug_b_pb2.Payload().FromString(msg.payload)}")
        received_messages.append(msg)

    subscriber_client.subscribe(unique_topic_base + "/#")
    subscriber_client.on_message = on_message

    writer_device = MqttClient(settings=writer_settings)

    try:
        await writer_device.setup()
        await asyncio.sleep(1)

        # Send 2 metrics (less than batch_size)
        metrics = [
            {"name": "batch_metric1", "value": i * 10, "timestamp": f"2025-08-30T15:0{i}:00Z"} for i in range(10)
        ]

        send_time = time.time()

        for metric in metrics:
            await writer_device.publish(metric)

        # Wait for batch timeout to trigger (should be around 2 seconds)
        await asyncio.sleep(4)

        # Verify timing - messages should arrive after batch_timeout but before 2x batch_timeout + some buffer
        assert message_received_time is not None, "No DDATA messages received"
        time_diff = message_received_time - send_time
        assert 1.8 <= time_diff <= 4.0, f"DDATA received after {time_diff}s, expected between 1.8s and 4.0s"

        logger.info(f"Received message in {time_diff}s")

        assert (
            len(received_messages) >= 3
        ), "Should have received exactly one batched message and one NBIRTH and one DBIRTH"

        decode_mqtt_message(received_messages[0])  # Parse NBIRTH --> does nothing
        decode_mqtt_message(received_messages[1])  # Parse DBIRTH --> fills alias
        parsed_metrics = decode_mqtt_message(received_messages[2])  # Parse DATA --> has metrics

        # Verify we got both metrics in the batch
        assert len(parsed_metrics) == 10

        for i, parsed_metric in enumerate(parsed_metrics):
            assert parsed_metric["name"] == "batch_metric1"
            assert parsed_metric["value"] == i * 10

    except asyncio.TimeoutError:
        pytest.fail("Sparkplug batching timeout test failed")
    finally:
        await writer_device.shutdown()
        subscriber_client.loop_stop()
        subscriber_client.disconnect()


@pytest.mark.timeout(60)
async def test_sparkplug_no_alias():
    """Test Sparkplug DBIRTH message handling without aliases."""
    mock_sink = MockSink()
    group_id = f"rdp_test_dbirth_{uuid.uuid4().hex}"
    node_id = f"dbirthnode_{uuid.uuid4().hex[:8]}"
    device_id = f"dbirthdevice_{uuid.uuid4().hex[:8]}"
    unique_topic_base = f"spBv1.0/{group_id}"

    test_metrics = [
        {"name": "voltage", "volts": 220.5},
        {"name": "current", "amps": 10.1},
    ]

    publisher_client = await create_connected_mqtt_client()

    subscriber_settings = create_sparkplug_subscriber_settings(f"{unique_topic_base}/+/{node_id}/{device_id}")
    subscriber_device = MqttClient(settings=subscriber_settings)

    subscriber_task = None

    try:
        await subscriber_device.setup()

        async def subscribe_task_func():
            async for metric in subscriber_device.subscribe():
                await mock_sink.put(metric)

        subscriber_task = asyncio.create_task(subscribe_task_func())

        # Wait for subscriber to connect and be ready
        await asyncio.sleep(8)

        # We explicitly do not send DBIRTH so the receiver has no alias map

        # Send DDATA messages
        # We do not provide an alias map so the normal names are sent
        ddata_payload = encode_data_message(test_metrics)

        logger.debug(f"Published data: {sparkplug_b_pb2.Payload().FromString(ddata_payload)}")

        publisher_client.publish(f"{unique_topic_base}/DDATA/{node_id}/{device_id}", ddata_payload, 0)

        for expected_metric in test_metrics:
            received_metric = await asyncio.wait_for(mock_sink.write_queue.get(), timeout=30)

            assert received_metric["name"] == expected_metric["name"]
            # The other keys are the fields
            for key, value in expected_metric.items():
                if key != "name":
                    assert received_metric[key] == value

    except asyncio.TimeoutError:
        pytest.fail("Sparkplug DBIRTH test timed out")
    finally:
        if subscriber_task:
            subscriber_task.cancel()
        await subscriber_device.shutdown()
        publisher_client.loop_stop()
        publisher_client.disconnect()


@pytest.mark.timeout(60)
async def test_sparkplug_alias():
    """
    Test for Sparkplug alias mapping integration.

    Verifies that Sparkplug alias mapping works correctly. It sets
    up a publisher to send DBIRTH and DDATA messages using alias mappings, and ensures
    that the subscriber resolves metric names properly using the alias definitions. The test
    uses a mock sink to validate the received data after alias resolution.
    """

    mock_sink = MockSink()
    group_id = f"rdp_test_aliasgroup_{uuid.uuid4().hex}"
    node_id = f"aliasnode_{uuid.uuid4().hex[:8]}"
    device_id = f"aliasdevice_{uuid.uuid4().hex[:8]}"
    unique_topic_base = f"spBv1.0/{group_id}"

    # Create metrics with specific alias mapping
    test_metrics = [
        {"name": "temperature_sensor", "celsius": 23.5},
        {"name": "pressure_sensor", "bar": 1.013},
    ]
    alias_map = {}

    # Setup a publisher to send DBIRTH and DDATA with aliases
    publisher_client = await create_connected_mqtt_client()

    # Subscriber setup
    subscriber_settings = create_sparkplug_subscriber_settings(f"{unique_topic_base}/+/{node_id}/{device_id}")
    subscriber_device = MqttClient(settings=subscriber_settings)

    subscriber_task = None

    try:
        await subscriber_device.setup()

        async def subscribe_task_func():
            async for metric in subscriber_device.subscribe():
                await mock_sink.put(metric)

        subscriber_task = asyncio.create_task(subscribe_task_func())

        await asyncio.sleep(8)  # Increased wait time for reliability

        # Send DBIRTH with alias definitions
        dbirth_payload = encode_birth_message(test_metrics, alias_map=alias_map)
        publisher_client.publish(f"{unique_topic_base}/DBIRTH/{node_id}/{device_id}", dbirth_payload, 0)

        logger.debug(f"Published DBIRTH payload: {sparkplug_b_pb2.Payload().FromString(dbirth_payload)}")
        logger.debug(f"Alias Map: {alias_map}")

        for metric in test_metrics:
            # Send temperature_sensor with DDATA only
            ddata_payload = encode_data_message(
                [metric],  # Just send one metric
                alias_map=alias_map,
                use_aliases_only=True,  # This sends empty names and relies on aliases
            )
            publisher_client.publish(f"{unique_topic_base}/DDATA/{node_id}/{device_id}", ddata_payload, 0)

            logger.debug(f"Published DDATA payload: {sparkplug_b_pb2.Payload().FromString(ddata_payload)}")

            received_metric = await asyncio.wait_for(mock_sink.write_queue.get(), timeout=30)

            logger.debug(f"Received metric: {received_metric}")

            assert received_metric["name"] == metric["name"]

            # The other keys are the fields
            for key, value in metric.items():
                if key != "name":
                    assert received_metric[key] == value

        # Now send both in one message
        # This could be regarded as a new test
        ddata_payload = encode_data_message(test_metrics, alias_map=alias_map, use_aliases_only=True)
        publisher_client.publish(f"{unique_topic_base}/DDATA/{node_id}/{device_id}", ddata_payload, 0)

        logger.debug(f"Published combined DDATA payload: {sparkplug_b_pb2.Payload().FromString(ddata_payload)}")

        # Verify both metrics are received
        for expected_metric in test_metrics:
            received_metric = await asyncio.wait_for(mock_sink.write_queue.get(), timeout=30)

            logger.debug(f"Received metric from combined message: {received_metric}")

            assert received_metric["name"] == expected_metric["name"]
            for key, value in expected_metric.items():
                if key != "name":
                    assert received_metric[key] == value

    except asyncio.TimeoutError:
        pytest.fail("Sparkplug alias mapping integration test timed out")
    finally:
        if subscriber_task:
            subscriber_task.cancel()
        await subscriber_device.shutdown()
        publisher_client.loop_stop()
        publisher_client.disconnect()


@pytest.mark.timeout(60)
async def test_sparkplug_dewesoft_dataset():
    """
    Manually try to parse data sent by Dewesoft.
    """
    mock_sink = MockSink()

    device_id = f"Dewesoft_Field_Measurement_{uuid.uuid4().hex[:8]}"
    unique_topic_base = "spBv1.0/dewesoftx"

    # Publisher for sending Dewesoft messages
    manual_publisher_client = await create_connected_mqtt_client()

    # Subscriber setup for Dewesoft Sparkplug
    subscriber_settings = create_sparkplug_subscriber_settings(f"{unique_topic_base}/+/plugin/{device_id}")
    subscriber_device = MqttClient(settings=subscriber_settings)

    subscriber_task = None

    try:
        await subscriber_device.setup()

        # Start subscriber
        async def subscribe_task_func():
            async for metric in subscriber_device.subscribe():
                await mock_sink.put(metric)

        subscriber_task = asyncio.create_task(subscribe_task_func())

        # Wait longer for subscriber to properly connect and subscribe
        await asyncio.sleep(4)

        # Create and send DDATA with DataSet
        data_payload = sparkplug_b_pb2.Payload()
        data_payload.timestamp = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)
        data_metric = data_payload.metrics.add()
        data_metric.name = f"{device_id}_cos_phi_total"
        data_metric.datatype = 16  # DataSet

        # DataSet value
        dataset = data_metric.dataset_value
        dataset.num_of_columns = 2
        dataset.types.extend([8, 10])  # UInt64 (timestamp), Double (value)

        dataset.columns.extend(["Timestamp", "V"])

        row = dataset.rows.add()
        ts_element = row.elements.add()
        ts_element.long_value = 1662033600000000
        val_element = row.elements.add()
        val_element.double_value = 42.7

        manual_publisher_client.publish(
            f"{unique_topic_base}/DDATA/plugin/{device_id}", data_payload.SerializeToString(), 0
        )

        logger.debug(f"Published DDATA data: {sparkplug_b_pb2.Payload().FromString(data_payload.SerializeToString())}")

        # Should receive the parsed Dewesoft metric
        received_metric = await asyncio.wait_for(mock_sink.write_queue.get(), timeout=15)

        assert received_metric["name"] == device_id
        assert received_metric["cos_phi_total"] == 42.7

    except asyncio.TimeoutError:
        pytest.fail("Dewesoft DataSet integration test timed out")
    finally:
        if subscriber_task:
            subscriber_task.cancel()
        await subscriber_device.shutdown()
        manual_publisher_client.loop_stop()
        manual_publisher_client.disconnect()


def create_sparkplug_writer_settings(**kwargs) -> tuple[MqttSettings, str, str, str, str]:
    group_id = f"rdp_test_sparkplug_{uuid.uuid4().hex}"
    node_id = f"testnode_{uuid.uuid4().hex[:8]}"
    device_id = f"testdevice_{uuid.uuid4().hex[:8]}"
    unique_topic_base = f"spBv1.0/{group_id}"

    defaults = {
        "host": "test.mosquitto.org",
        "port": 1883,
        "ssl": False,
        "identifier": f"sparkplug-writer-{uuid.uuid4()}",
        "payload_parser": "sparkplug",
        "sparkplug_group_id": group_id,
        "sparkplug_node_id": node_id,
        "sparkplug_device_id": device_id,
        "subscribe": False,
    }
    defaults.update(kwargs)
    return MqttSettings(**defaults), group_id, node_id, device_id, unique_topic_base


def create_sparkplug_subscriber_settings(topic: str, **kwargs) -> MqttSettings:
    """Create MqttSettings for a Sparkplug subscriber with common defaults."""
    defaults = {
        "host": "test.mosquitto.org",
        "port": 1883,
        "ssl": False,
        "topic": topic,
        "identifier": f"sparkplug-subscriber-{uuid.uuid4()}",
        "payload_parser": "sparkplug",
    }
    defaults.update(kwargs)
    return MqttSettings(**defaults)


async def start_devices_with_delay(*devices: MqttClient, delay: float = 2.0):
    """Start multiple MQTT devices and wait for them to connect."""
    for device in devices:
        await device.setup()
    await asyncio.sleep(delay)

    # Wait for all devices to actually connect by checking their connection status
    max_wait = 15  # Maximum wait time in seconds
    wait_interval = 0.5
    elapsed = 0

    while elapsed < max_wait:
        all_connected = True
        for device in devices:
            if hasattr(device, "_client") and device._client:
                if not device._client.is_connected():
                    all_connected = False
                    break
            else:
                all_connected = False
                break

        if all_connected:
            # Give a bit more time for subscriptions to be processed
            await asyncio.sleep(1)
            break

        await asyncio.sleep(wait_interval)
        elapsed += wait_interval


async def cleanup_tasks(*tasks: asyncio.Task) -> None:
    """Cancel and await multiple asyncio tasks with proper exception handling."""
    for task in tasks:
        if task:
            task.cancel()

    for task in tasks:
        if task:
            with contextlib.suppress(asyncio.CancelledError):
                await task


@pytest.mark.timeout(60)
async def test_sparkplug_birth_message_resend_on_new_metric():
    """Test that a DBIRTH message is sent correctly when new metrics are added after previous ones."""

    # Setup writer and subscriber
    writer_settings, group_id, node_id, device_id, unique_topic_base = create_sparkplug_writer_settings()
    writer_device = MqttClient(settings=writer_settings)

    # Set up raw MQTT subscriber to capture all messages
    subscriber_client = await create_connected_mqtt_client()
    received_messages = []

    def on_message(client, userdata, msg):
        logger.debug(f"Received message on topic {msg.topic}: {sparkplug_b_pb2.Payload().FromString(msg.payload)}")
        received_messages.append((msg.topic, msg.payload))

    subscriber_client.subscribe(f"{unique_topic_base}/+/#")
    subscriber_client.on_message = on_message

    try:
        await writer_device.setup()
        await asyncio.sleep(2)  # Wait for NBIRTH

        # Should have NBIRTH
        assert len(received_messages) >= 1, f"Expected at least 1 messages (NBIRTH), got {len(received_messages)}"

        # Verify first DBIRTH and DDATA
        nbirth_topic, dbirth_payload = received_messages[0]

        assert "/NBIRTH/" in nbirth_topic, f"First message should be NBIRTH, got topic: {nbirth_topic}"

        # Clear messages (should have NBIRTH)
        received_messages.clear()

        # Send first metric with initial fields
        first_metric = {
            "name": "sensor_1",
            "temperature": 25.5,
            "timestamp": "2025-08-30T15:00:00Z",
        }
        await writer_device.publish(first_metric)
        await asyncio.sleep(1)

        # Should have DBIRTH + DDATA
        assert (
            len(received_messages) >= 2
        ), f"Expected at least 2 messages (DBIRTH + DDATA), got {len(received_messages)}"

        # Verify first DBIRTH and DDATA
        dbirth_topic, dbirth_payload = received_messages[0]
        ddata_topic, ddata_payload = received_messages[1]

        assert "/DBIRTH/" in dbirth_topic, f"First message should be DBIRTH, got topic: {dbirth_topic}"
        assert "/DDATA/" in ddata_topic, f"Second message should be DDATA, got topic: {ddata_topic}"

        # Parse DBIRTH to establish alias map
        decode_mqtt_message(type("MockMessage", (), {"topic": dbirth_topic, "payload": dbirth_payload})())
        ddata_parsed = decode_mqtt_message(type("MockMessage", (), {"topic": ddata_topic, "payload": ddata_payload})())

        # Verify first metric was received correctly
        assert len(ddata_parsed) == 1
        assert ddata_parsed[0]["name"] == "sensor_1"
        assert ddata_parsed[0]["temperature"] == 25.5

        # Clear messages for next test
        received_messages.clear()

        # Send a second metric with NEW fields that weren't in the first metric
        second_metric = {
            "name": "sensor_2",
            "pressure": 1013.25,  # New field not in first metric
            "humidity": 65.0,  # New field not in first metric
            "timestamp": "2025-08-30T15:01:00Z",
        }
        await writer_device.publish(second_metric)
        await asyncio.sleep(1)

        # Should have another DBIRTH + DDATA because new fields were introduced
        assert (
            len(received_messages) >= 2
        ), f"Expected at least 2 messages for new metric (DBIRTH + DDATA), got {len(received_messages)}"

        new_dbirth_topic, new_dbirth_payload = received_messages[0]
        new_ddata_topic, new_ddata_payload = received_messages[1]

        assert "/DBIRTH/" in new_dbirth_topic, f"First message should be new DBIRTH, got topic: {new_dbirth_topic}"
        assert "/DDATA/" in new_ddata_topic, f"Second message should be DDATA, got topic: {new_ddata_topic}"

        # Parse the new DBIRTH and DDATA
        decode_mqtt_message(type("MockMessage", (), {"topic": new_dbirth_topic, "payload": new_dbirth_payload})())
        new_ddata_parsed = decode_mqtt_message(
            type("MockMessage", (), {"topic": new_ddata_topic, "payload": new_ddata_payload})()
        )

        # Verify the new metric was received correctly with new fields
        assert len(new_ddata_parsed) == 1
        assert new_ddata_parsed[0]["name"] == "sensor_2"
        assert new_ddata_parsed[0]["pressure"] == 1013.25
        assert new_ddata_parsed[0]["humidity"] == 65.0

        # Clear messages for final test
        received_messages.clear()

        # Send a third metric that only uses fields already seen - should NOT trigger new DBIRTH
        third_metric = {
            "name": "sensor_2",
            "temperature": 23.0,  # Already seen field
            "humidity": 70.0,  # Already seen field
            "timestamp": "2025-08-30T15:02:00Z",
        }
        await writer_device.publish(third_metric)
        await asyncio.sleep(1)

        # Should only have DDATA, no new DBIRTH since no new fields
        assert len(received_messages) >= 1, f"Expected at least 1 message (DDATA only), got {len(received_messages)}"

        # Check that first message is DDATA, not DBIRTH
        final_topic, final_payload = received_messages[0]
        assert "/DDATA/" in final_topic, f"Message should be DDATA only (no new fields), got topic: {final_topic}"

        # Verify no DBIRTH was sent (all messages should be DDATA)
        for topic, _ in received_messages:
            assert "/DBIRTH/" not in topic, f"Unexpected DBIRTH message when no new fields added: {topic}"

        final_parsed = decode_mqtt_message(type("MockMessage", (), {"topic": final_topic, "payload": final_payload})())
        assert len(final_parsed) == 1
        assert final_parsed[0]["name"] == "sensor_2"
        assert final_parsed[0]["temperature"] == 23.0
        assert final_parsed[0]["humidity"] == 70.0

    except asyncio.TimeoutError:
        pytest.fail("Sparkplug birth message resend test timed out")
    finally:
        await writer_device.shutdown()
        subscriber_client.loop_stop()
        subscriber_client.disconnect()


@pytest.mark.timeout(60)
async def test_sparkplug_e2e():
    """Test Sparkplug DBIRTH/DDATA flow through real broker."""
    logger.debug("Test starting...")
    mock_sink = MockSink()
    logger.debug("Mock sink setup complete")

    # Setup devices
    writer_settings, group_id, node_id, device_id, unique_topic_base = create_sparkplug_writer_settings()
    writer_device = MqttClient(settings=writer_settings)

    subscriber_settings = create_sparkplug_subscriber_settings(f"{unique_topic_base}/+/{node_id}/{device_id}")
    subscriber_device = MqttClient(settings=subscriber_settings)

    logger.debug(f"Subscriber topic setting: {subscriber_settings.topic}")
    logger.debug(f"Subscriber subscribe setting: {subscriber_settings.subscribe}")

    subscriber_task = None

    try:
        logger.debug("Starting writer and subscriber devices...")
        logger.debug(f"Writer connecting to {writer_settings.host}:{writer_settings.port}")
        logger.debug(f"Subscriber connecting to {subscriber_settings.host}:{subscriber_settings.port}")

        # Start devices with longer delay for stability
        await start_devices_with_delay(writer_device, subscriber_device, delay=8.0)
        logger.debug("Connection wait complete, proceeding to send metrics...")

        async def subscribe_task_func():
            async for metric in subscriber_device.subscribe():
                await mock_sink.put(metric)

        subscriber_task = asyncio.create_task(subscribe_task_func())

        # Test metric to send
        test_metric = {
            "name": "sparkplug_test_metric",
            "temperature": 25.5,
            "humidity": 60.0,
            "timestamp": "2025-08-30T15:00:00Z",
        }

        logger.debug(f"Subscriber listening on: {unique_topic_base}/DDATA/{node_id}/{device_id}")

        for i in range(3):
            await writer_device.publish(test_metric)
            logger.debug(f"Sent metric {i + 1}/3")

        await asyncio.sleep(1)

        # Should receive the metric with correct device-based naming
        received_metric = await asyncio.wait_for(mock_sink.write_queue.get(), timeout=20)

        # Verify parsing worked correctly
        assert received_metric["name"] == "sparkplug_test_metric"

        assert received_metric["temperature"] == 25.5
        assert received_metric["humidity"] == 60.0

    except asyncio.TimeoutError:
        pytest.fail("Sparkplug integration test timed out")
    finally:
        if subscriber_task:
            subscriber_task.cancel()
        await writer_device.shutdown()
        await subscriber_device.shutdown()


@pytest.mark.timeout(60)
async def test_json_e2e(complex_weather_message):
    """Test complex JSON message publishing and receiving through real broker"""
    topic = f"test/complex/json/{uuid.uuid4()}"

    loop = asyncio.get_running_loop()
    received_message = asyncio.Future()

    # Setup raw MQTT client for receiving
    receiver_client = await create_connected_mqtt_client()

    def on_message(client, userdata, msg):
        if msg.topic == topic and not loop.is_closed():
            loop.call_soon_threadsafe(received_message.set_result, msg.payload)

    receiver_client.subscribe(topic)
    receiver_client.on_message = on_message

    # Setup writer with JSON parser
    writer_settings = MqttSettings(
        host="test.mosquitto.org",
        port=1883,
        ssl=False,
        topic=topic,
        identifier=f"writer-{uuid.uuid4()}",
        payload_parser="json",
    )
    writer_device = MqttClient(settings=writer_settings)
    await writer_device.setup()

    try:
        await asyncio.sleep(2)  # Give time for connections

        # Publish complex message
        await writer_device.publish(complex_weather_message, topic=topic)

        # Receive and verify
        payload = await asyncio.wait_for(received_message, timeout=30)
        decoded_data = orjson.loads(payload)

        assert decoded_data["device"] == "weather_station_pro_v2"
        assert decoded_data["metadata"]["firmware_version"] == "3.2.1"
        assert decoded_data["metadata"]["manufacturer"]["name"] == "WeatherTech Solutions"
        assert decoded_data["metadata"]["manufacturer"]["certifications"] == ["ISO9001", "CE", "FCC"]
        assert decoded_data["measurements"]["environmental"]["temperature"]["value"] == 23.5
        assert decoded_data["measurements"]["environmental"]["humidity"]["value"] == 65.2
        assert decoded_data["measurements"]["environmental"]["pressure"]["value"] == 1013.25
        assert decoded_data["measurements"]["wind"]["speed"]["current"] == 12.3
        assert decoded_data["measurements"]["air_quality"]["pm25"] == 8.2
        assert decoded_data["location"]["city"] == "Vienna"
        assert decoded_data["location"]["coordinates"]["latitude"] == 48.2082
        assert decoded_data["operational_status"]["battery_level"] == 87.3
        assert decoded_data["operational_status"]["alerts"][0]["type"] == "low_battery_warning"
        assert decoded_data["data_quality"]["completeness"] == 99.8
        assert decoded_data["tags"] == ["outdoor", "urban", "primary_station", "real_time"]
        assert decoded_data["timestamp"] == "2025-01-15T10:30:00Z"

    finally:
        await writer_device.shutdown()
        receiver_client.loop_stop()
        receiver_client.disconnect()


@pytest.mark.timeout(60)
async def test_json_zstd_e2e(complex_weather_message):
    """Test complex JSON message with ZSTD compression through real broker"""
    topic = f"test/complex/json_zstd/{uuid.uuid4()}"

    mock_sink = MockSink()

    # Setup writer with JSON ZSTD compression
    writer_settings = MqttSettings(
        host="test.mosquitto.org",
        port=1883,
        ssl=False,
        identifier=f"writer-zstd-{uuid.uuid4()}",
        payload_parser="json_zstd",
        subscribe=False,
    )
    writer_device = MqttClient(settings=writer_settings)

    # Setup subscriber with JSON ZSTD decompression
    subscriber_settings = MqttSettings(
        host="test.mosquitto.org",
        port=1883,
        ssl=False,
        topic=topic,
        identifier=f"subscriber-zstd-{uuid.uuid4()}",
        payload_parser="json_zstd",
    )
    subscriber_device = MqttClient(settings=subscriber_settings)

    subscriber_task = None

    try:
        await writer_device.setup()
        await subscriber_device.setup()
        await asyncio.sleep(2)  # Give time for connections

        async def subscribe_task_func():
            async for metric in subscriber_device.subscribe():
                await mock_sink.put(metric)

        subscriber_task = asyncio.create_task(subscribe_task_func())
        await asyncio.sleep(1)  # Give subscriber time to start

        # Publish complex message with ZSTD compression
        await writer_device.publish(complex_weather_message, topic=topic)

        # Receive and verify decompressed data
        received_metric = await asyncio.wait_for(mock_sink.write_queue.get(), timeout=30)

        assert received_metric["device"] == "weather_station_pro_v2"
        assert received_metric["metadata"]["firmware_version"] == "3.2.1"
        assert received_metric["metadata"]["manufacturer"]["name"] == "WeatherTech Solutions"
        assert received_metric["metadata"]["manufacturer"]["certifications"] == ["ISO9001", "CE", "FCC"]
        assert received_metric["measurements"]["environmental"]["temperature"]["value"] == 23.5
        assert received_metric["measurements"]["environmental"]["humidity"]["value"] == 65.2
        assert received_metric["measurements"]["environmental"]["pressure"]["value"] == 1013.25
        assert received_metric["measurements"]["wind"]["speed"]["current"] == 12.3
        assert received_metric["measurements"]["air_quality"]["pm25"] == 8.2
        assert received_metric["location"]["city"] == "Vienna"
        assert received_metric["location"]["coordinates"]["latitude"] == 48.2082
        assert received_metric["operational_status"]["battery_level"] == 87.3
        assert received_metric["operational_status"]["alerts"][0]["type"] == "low_battery_warning"
        assert received_metric["data_quality"]["completeness"] == 99.8
        assert received_metric["tags"] == ["outdoor", "urban", "primary_station", "real_time"]
        assert received_metric["timestamp"] == "2025-01-15T10:30:00Z"

    except asyncio.TimeoutError:
        pytest.fail("Complex JSON ZSTD integration test timed out")
    finally:
        if subscriber_task:
            subscriber_task.cancel()
        await writer_device.shutdown()
        await subscriber_device.shutdown()
