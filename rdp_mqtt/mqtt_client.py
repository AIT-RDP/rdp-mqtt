"""
This module provides a MQTT client that supports multiple payload formats
including JSON, compressed JSON with ZSTD, and Sparkplug B protocol.
"""

import asyncio
import functools
import logging
import ssl
import time
from typing import Any, AsyncGenerator, Dict, List, Optional

import orjson
import paho.mqtt.client as mqtt
import pydantic
import zstandard
from paho.mqtt.subscribeoptions import SubscribeOptions
from pydantic import BaseModel

from rdp_mqtt.mqtt_parameters import MqttBaseParameters, MqttBatchingParameters, MqttSparkplugParameters
from rdp_mqtt.sparkplug.sparkplug_decode import decode_mqtt_message
from rdp_mqtt.sparkplug.sparkplug_encode import (
    encode_birth_message,
    encode_data_message,
    encode_node_birth_message,
    get_sparkplug_topic,
)


class MqttSettings(MqttBaseParameters, MqttBatchingParameters, MqttSparkplugParameters):
    """MQTT client configuration parameters"""

    pass


class MqttWriteMetadata(BaseModel):
    """Metadata for MQTT write operations"""

    topic: Optional[str] = pydantic.Field(description="The topic to write the metric to", default=None)


class MqttClient:
    """
    This client handles connection management, payload parsing, batching, and Sparkplug protocol.
    """

    def __init__(self, settings: MqttSettings):
        self.settings = settings
        self.logger = logging.getLogger(__name__)

        self._client: mqtt.Client | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._incoming: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()

        # For json_zstd we need to compress the payload before sending it
        self._zstd_compressor: zstandard.ZstdCompressor
        self._zstd_decompressor: zstandard.ZstdDecompressor

        # Batching support
        self._pending_metrics: List[Dict[str, Any]] = []
        self._batch_lock: asyncio.Lock
        self._batch_task: asyncio.Task[None] | None = None
        self._last_batch_time: float = 0

        # Sparkplug state management
        self._sparkplug_alias_map: Dict[str, Any] = {}
        self._sparkplug_node_birth_sent = False
        self._sparkplug_tasks: List[asyncio.Task[None]] = []

    async def setup(self) -> None:
        """Initialize the MQTT client"""
        # For json_zstd we need to compress the payload before sending it
        self._zstd_compressor = zstandard.ZstdCompressor(level=3)
        self._zstd_decompressor = zstandard.ZstdDecompressor()

        self._client = mqtt.Client(client_id=self.settings.identifier, userdata=self, protocol=mqtt.MQTTv5)

        # Auth
        if self.settings.username:
            self._client.username_pw_set(
                self.settings.username,
                self.settings.password.get_secret_value() if self.settings.password else None,
            )

        # TLS
        if self.settings.ssl:
            ctx = ssl.create_default_context()
            if not self.settings.validate_certificate:
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
            self._client.tls_set_context(ctx)

        self._client.reconnect_delay_set(min_delay=1, max_delay=60)

        # Callbacks
        self._client.on_connect = MqttClient._on_connect
        self._client.on_disconnect = MqttClient._on_disconnect
        self._client.on_message = MqttClient._on_message

        self._loop = asyncio.get_running_loop()

        # Initialize batching
        self._pending_metrics = []
        self._batch_lock = asyncio.Lock()
        self._last_batch_time = time.time()

        # Initialize Sparkplug state
        self._sparkplug_alias_map = {}
        self._sparkplug_node_birth_sent = False

        # Start the separate event loop
        self._client.connect_async(self.settings.host, self.settings.port, keepalive=5)
        self._client.loop_start()

        self.logger.info("MQTT client started")

    @staticmethod
    def _on_connect(
        client: mqtt.Client, userdata: "MqttClient", flags: Any, rc: int, properties: Any | None = None
    ) -> None:
        if rc == mqtt.MQTT_ERR_SUCCESS:
            userdata.logger.info("MQTT connected")

            if userdata.settings.subscribe:
                options = SubscribeOptions(qos=userdata.settings.qos, noLocal=True)
                client.subscribe(userdata.settings.topic, options=options)
                userdata.logger.info("Subscribed to %s", userdata.settings.topic)

            # Send NBIRTH for Sparkplug
            if userdata.settings.payload_parser == "sparkplug" and not userdata._sparkplug_node_birth_sent:
                try:
                    # Schedule birth message send in the event loop
                    if userdata._loop:

                        def create_and_track_task() -> asyncio.Task[None]:
                            task = asyncio.create_task(userdata._send_sparkplug_node_birth())
                            userdata._sparkplug_tasks.append(task)
                            return task

                        userdata._loop.call_soon_threadsafe(create_and_track_task)
                except Exception as e:
                    userdata.logger.error("Failed to schedule NBIRTH message: %s", e)
        else:
            userdata.logger.error("MQTT connect failed (rc=%s)", rc)

    @staticmethod
    def _on_disconnect(client: mqtt.Client, userdata: "MqttClient", rc: int, properties: Any | None = None) -> None:
        if rc == mqtt.MQTT_ERR_SUCCESS:
            userdata.logger.info("MQTT disconnected cleanly")
            return

        reason = getattr(mqtt, "error_string", lambda x: str(x))(rc)
        userdata.logger.warning("MQTT lost connection (rc=%s – %s). Reconnecting…", rc, reason)
        try:
            client.reconnect()  # Not blocking, paho keeps retrying with back‑off if this fails
        except Exception:
            userdata.logger.debug("First reconnect attempt failed; will keep retrying.")

    @staticmethod
    def _on_message(client: mqtt.Client, userdata: "MqttClient", message: mqtt.MQTTMessage) -> None:
        self = userdata  # Alias for clarity

        self.logger.debug("Received MQTT message on topic '%s': %s", message.topic, message.payload)

        try:
            metrics: List[Dict[str, Any]] | None
            if self.settings.payload_parser == "sparkplug":
                metrics = decode_mqtt_message(message)
            elif self.settings.payload_parser == "json":
                metrics = MqttClient._decode_json(message.payload.decode("utf‑8"))
            elif self.settings.payload_parser == "json_zstd":
                decompressed_payload = self._zstd_decompressor.decompress(message.payload)
                metrics = MqttClient._decode_json(decompressed_payload)
            else:
                self.logger.error("Unknown payload parser '%s'", self.settings.payload_parser)
                return
        except Exception:
            self.logger.exception("Failed to parse MQTT payload")
            return

        if not metrics:
            return

        # Push into the asyncio queue from the paho thread
        for metric in metrics:
            try:
                if self._loop:
                    # If the loop is running, use call_soon_threadsafe to push the metric into the asyncio queue
                    self._loop.call_soon_threadsafe(self._incoming.put_nowait, metric)
                else:
                    raise ValueError
            except RuntimeError:
                # Fall back to immediate put (rare race during startup)
                self._incoming.put_nowait(metric)

    @staticmethod
    def _decode_json(payload: Any) -> List[Dict[str, Any]] | None:
        """Decode JSON payload into a standardized format"""
        try:
            data = orjson.loads(payload)
            return data if isinstance(data, list) else [data]
        except Exception:
            return None

    async def _send_batch(self, metrics: List[Dict[str, Any]], topic: str) -> None:
        """Send a batch of metrics."""
        if not metrics:
            return

        # Create batch payload
        batch_data = []
        for metric in metrics:
            # Apply field precision if configured
            metric_copy = dict(metric)
            if self.settings.field_precision is not None:
                # Round numeric values
                for key, value in metric_copy.items():
                    if isinstance(value, float):
                        metric_copy[key] = round(value, self.settings.field_precision)
            batch_data.append(metric_copy)

        # Serialize based on payload parser
        payload_object = batch_data[0] if len(batch_data) == 1 else batch_data

        if self.settings.payload_parser == "json_zstd":
            payload = self._zstd_compressor.compress(orjson.dumps(payload_object))
        elif self.settings.payload_parser == "json":
            payload = orjson.dumps(payload_object)
        elif self.settings.payload_parser == "sparkplug":
            await self._send_sparkplug_device_birth_if_needed(metrics)
            payload = encode_data_message(
                metrics,
                alias_map=self._sparkplug_alias_map,
                use_aliases_only=True,
            )
            # DDATA topic for the device
            topic = (
                f"{get_sparkplug_topic(self.settings.sparkplug_group_id, self.settings.sparkplug_node_id, 'DDATA')}"
                f"/{self.settings.sparkplug_device_id}"
            )
        else:
            raise ValueError(f"Unknown payload parser for batch write '{self.settings.payload_parser}'")

        if self._client:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, functools.partial(self._client.publish, topic, payload, 0, False))
            self.logger.debug("Published batch of %d metrics to %s", len(metrics), topic)
        else:
            raise ConnectionError("MQTT client not connected, cannot publish batch")

    async def _flush_batch(self, topic: str) -> None:
        """Flush pending metrics batch."""
        async with self._batch_lock:
            if self._pending_metrics:
                await self._send_batch(self._pending_metrics, topic)
                self._pending_metrics.clear()
                self._last_batch_time = time.time()

    async def _batch_timer(self, topic: str) -> None:
        """Timer task to flush batches periodically."""
        try:
            await asyncio.sleep(self.settings.batch_timeout)
            await self._flush_batch(topic)
        except asyncio.CancelledError:
            pass
        finally:
            self._batch_task = None

    async def publish(self, metric: Dict[str, Any], topic: Optional[str] = None) -> None:
        """Publish a metric to MQTT"""
        publish_topic = topic or self.settings.topic

        if self.settings.batch_size > 0:
            async with self._batch_lock:
                # Add metric to the pending batch
                self._pending_metrics.append(metric)

                # Determine if we should flush the batch
                should_flush = len(self._pending_metrics) >= self.settings.batch_size

                if should_flush:
                    # Cancel existing timer if any
                    if self._batch_task and not self._batch_task.done():
                        self._batch_task.cancel()
                        self._batch_task = None

                    # Send entire batch
                    await self._send_batch(self._pending_metrics, publish_topic)
                    self._pending_metrics.clear()
                    self._last_batch_time = time.time()
                elif not self._batch_task or self._batch_task.done():
                    # Start timer for partial batch
                    self._batch_task = asyncio.create_task(self._batch_timer(publish_topic))
        else:
            # Send immediately (no batching)
            await self._send_batch([metric], publish_topic)

    async def subscribe(self) -> AsyncGenerator[Dict[str, Any], None]:
        """Subscribe to incoming messages"""
        while True:
            metric = await self._incoming.get()
            self.logger.debug(f"Received metric from MQTT: {metric}")
            yield metric

    async def _send_sparkplug_node_birth(self) -> None:
        """Send an empty NBIRTH message to announce the node."""
        if self._sparkplug_node_birth_sent or self.settings.payload_parser != "sparkplug":
            return

        try:
            birth_payload = encode_node_birth_message()

            birth_topic = get_sparkplug_topic(
                group_id=self.settings.sparkplug_group_id,
                node_id=self.settings.sparkplug_node_id,
                message_type="NBIRTH",
            )

            if self._client:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(
                    None, functools.partial(self._client.publish, birth_topic, birth_payload, 0, False)
                )
                self._sparkplug_node_birth_sent = True
                self.logger.info(f"Sent Sparkplug NBIRTH for node {self.settings.sparkplug_node_id} at {birth_topic}")
            else:
                raise ConnectionError("MQTT client not connected, cannot publish NBIRTH")

        except Exception as e:
            self.logger.error(f"Failed to send NBIRTH message: {e}")

    async def _send_sparkplug_device_birth_if_needed(self, metrics: List[Dict[str, Any]]) -> None:
        """Checks for new metrics and sends a DBIRTH for them if new fields are detected."""
        if self.settings.payload_parser != "sparkplug":
            return

        try:
            birth_payload = encode_birth_message(metrics, alias_map=self._sparkplug_alias_map)

            # If no new fields were found, encode_birth_message returns None
            if birth_payload is None:
                return

            sparkplug_topic = get_sparkplug_topic(
                self.settings.sparkplug_group_id, self.settings.sparkplug_node_id, "DBIRTH"
            )
            birth_topic = f"{sparkplug_topic}/{self.settings.sparkplug_device_id}"

            if not self._client:
                raise ConnectionError("MQTT client not connected, cannot publish DBIRTH")

            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None, functools.partial(self._client.publish, birth_topic, birth_payload, 0, False)
            )
            self.logger.info(f"Sent Sparkplug DBIRTH for new fields at {birth_topic}")

        except Exception as e:
            self.logger.error("Failed to send DBIRTH message: %s", e)

    async def shutdown(self) -> None:
        """Shutdown the MQTT client"""
        # Flush any pending batches
        if self.settings.batch_size > 0 and self._pending_metrics:
            topic = self.settings.topic
            await self._flush_batch(topic)

        # Cancel the batch timer if running
        if self._batch_task and not self._batch_task.done():
            self._batch_task.cancel()

        # Clean up Sparkplug tasks
        for task in self._sparkplug_tasks:
            if not task.done():
                task.cancel()
        self._sparkplug_tasks.clear()

        # Disconnect MQTT client
        if self._client:
            self._client.loop_stop()
            self._client.disconnect()

        self.logger.info("MQTT client shut down gracefully")
