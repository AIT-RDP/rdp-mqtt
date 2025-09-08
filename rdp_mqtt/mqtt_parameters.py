import uuid
from typing import Literal, Optional

import pydantic
from pydantic import SecretStr


class MqttConnectionParameters(pydantic.BaseModel):
    """Common MQTT connection parameters"""

    host: str = pydantic.Field(
        description="The address of the MQTT broker",
        default="localhost",
    )

    port: int = pydantic.Field(
        description="The port of the MQTT broker. 1883 is no TLS, 8883 is with TLS",
        default=8883,
    )

    username: Optional[str] = pydantic.Field(
        description="The username for the MQTT broker",
        default=None,
    )

    password: Optional[SecretStr] = pydantic.Field(
        description="The password for the MQTT broker",
        default=None,
    )

    ssl: bool = pydantic.Field(
        description="Whether to use SSL/TLS",
        default=True,
    )

    identifier: str = pydantic.Field(
        description="The identifier for the MQTT broker (client id)",
        default_factory=lambda: "RDP_" + str(uuid.uuid4()),
    )

    topic: str = pydantic.Field(
        description="The topic to subscribe to (supports wildcards like + and #)",
        default="#",
    )

    validate_certificate: bool = pydantic.Field(
        description="Whether to validate the certificate",
        default=True,
    )

    qos: int = pydantic.Field(
        description="QOS level (0, 1, or 2)",
        default=0,
        ge=0,
        le=2,
    )

    subscribe: bool = pydantic.Field(
        description="Whether to subscribe to the topic",
        default=True,
    )


class MqttPayloadParameters(pydantic.BaseModel):
    """Common MQTT payload handling parameters"""

    payload_parser: Literal["sparkplug", "json", "json_zstd"] = pydantic.Field(
        description="The parser to use for the payload (json, json_zstd, sparkplug)",
        default="json",
    )

    field_precision: Optional[int] = pydantic.Field(
        description="Round numeric fields to N decimal places to reduce payload size",
        default=None,
        ge=0,
    )


class MqttBatchingParameters(pydantic.BaseModel):
    """MQTT batching parameters"""

    batch_size: int = pydantic.Field(
        description="Number of metrics to batch together before sending (0 = no batching)",
        default=0,
        ge=0,
    )

    batch_timeout: float = pydantic.Field(
        description="Maximum time in seconds to wait before sending a partial batch",
        default=1.0,
        gt=0,
    )


class MqttSparkplugParameters(pydantic.BaseModel):
    """MQTT spark plugin parameters"""

    sparkplug_group_id: str = pydantic.Field(
        description="Sparkplug group ID for outgoing messages",
        default_factory=lambda: "RDP_group_" + str(uuid.uuid4()),
    )

    sparkplug_node_id: str = pydantic.Field(
        description="Sparkplug node ID for outgoing messages",
        default_factory=lambda: "RDP_node_" + str(uuid.uuid4()),
    )

    sparkplug_device_id: str = pydantic.Field(
        description="Sparkplug device ID for outgoing messages",
        default_factory=lambda: "RDP_device_" + str(uuid.uuid4()),
    )


class MqttBaseParameters(
    MqttConnectionParameters,
    MqttPayloadParameters,
):
    """Base MQTT parameters combining connection, payload"""

    pass
