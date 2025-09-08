import datetime
import re
import xml.etree.ElementTree as XML
from typing import Any, Dict, List, Optional

import paho.mqtt.client as mqtt
from google.protobuf.message import Message

from .generated import sparkplug_b_pb2
from .protobuf_to_dict import protobuf_to_dict
from .sparkplug_shared import DataSetDataType, MetricDataType

GROUP_ID_DEWESOFT = "dewesoftx"
# Global metadata storage for all group IDs
sparkplug_metadata: dict[str, dict[str, dict[str, Any]]] = {}  # {group_id: {metric_name: metadata}}
# Global alias mapping storage for all group IDs
sparkplug_aliases: dict[str, Dict[int, Any]] = {}  # {group_id: {alias_id: metric_name}}


def decode_message(container: Message) -> list[dict[str, Any]]:
    """
    Retrieves a metric from the given container based on the name or alias,
    and parses its value based on its datatype.

    Args:
        container: The protobuf container object holding metrics

    Returns:
        A dictionary containing the metric's details (name, alias, type, value),
        or None if no matching metric exists.
    """
    metrics = []
    # Iterate through the metrics in the container
    for metric in container.metrics:  # type: ignore
        parsed_metric = decode_message_element(metric)
        metrics.append(parsed_metric)

    # Return None if no match is found
    return metrics


def decode_message_element(metric: Message) -> dict[str, Any]:
    """
    Parses the metric object to extract its details based on its datatype.

    Args:
        metric: The metric protobuf object to parse.

    Returns:
        A dictionary containing the metric's name, alias, type, and value.
    """
    parsed_metric = protobuf_to_dict(metric)

    # Parse the value based on the datatype
    if metric.datatype in {  # type: ignore
        MetricDataType.Int8,
        MetricDataType.Int16,
        MetricDataType.Int32,
        MetricDataType.UInt8,
        MetricDataType.UInt16,
        MetricDataType.UInt32,
    }:
        parsed_metric["value"] = parsed_metric.pop("int_value", None)
    elif metric.datatype in {MetricDataType.Int64, MetricDataType.UInt64, MetricDataType.DateTime}:  # type: ignore
        parsed_metric["value"] = parsed_metric.pop("long_value", None)
    elif metric.datatype == MetricDataType.Float:  # type: ignore
        parsed_metric["value"] = parsed_metric.pop("float_value", None)
    elif metric.datatype == MetricDataType.Double:  # type: ignore
        parsed_metric["value"] = parsed_metric.pop("double_value", None)
    elif metric.datatype == MetricDataType.Boolean:  # type: ignore
        parsed_metric["value"] = parsed_metric.pop("boolean_value", None)
    elif metric.datatype in {MetricDataType.String, MetricDataType.Text, MetricDataType.UUID}:  # type: ignore
        parsed_metric["value"] = parsed_metric.pop("string_value", None)
    elif metric.datatype in {MetricDataType.Bytes, MetricDataType.File}:  # type: ignore
        parsed_metric["value"] = parsed_metric.pop("bytes_value", None)
    elif metric.datatype == MetricDataType.Template:  # type: ignore
        parsed_metric["value"] = parsed_metric.pop("template_value", None)
    elif metric.datatype == MetricDataType.DataSet:  # type: ignore
        parsed_metric["value"] = parsed_metric.pop("dataset_value", None)

    return parsed_metric


def decode_mqtt_message(msg: mqtt.MQTTMessage) -> List[Dict[str, Any]] | None:
    tokens = msg.topic.split("/")

    # Validate minimum Sparkplug topic structure (need at least 4 parts: version/group/function/node)
    if len(tokens) < 4:
        return None

    try:
        sparkplug_version = tokens[0]
        group_id = tokens[1]
        sparkplug_function = tokens[2]
        node_id = tokens[3]
        device_id = tokens[4] if len(tokens) > 4 else None
    except IndexError:
        return None

    if sparkplug_version == "spBv1.0":
        try:
            inboundPayload = sparkplug_b_pb2.Payload()  # type: ignore
            inboundPayload.ParseFromString(msg.payload)
        except Exception:
            # Invalid protobuf payload - not a valid Sparkplug message
            return None

        if sparkplug_function == "DDATA" or sparkplug_function == "NDATA":
            return decode_data_message(inboundPayload, node_id, group_id, device_id)

        elif sparkplug_function == "DBIRTH" or sparkplug_function == "NBIRTH":
            decode_birth_message(inboundPayload, node_id, group_id)

        elif (
            sparkplug_function == "DDEATH"
            or sparkplug_function == "NDEATH"
            or sparkplug_function == "DCMD"
            or sparkplug_function == "NCMD"
        ):
            pass
    else:
        print(f"Received message on unexpected sparkplug version: {sparkplug_version}")

    return None


def remove_prefix(name: str, device_id: str) -> str:
    """
    Removes the prefix consisting of the given device_id, an underscore,
    and an optional number followed by another underscore or the end of the prefix.

    Args:
        name (str): The input string to process.
        device_id (str): The device_id to remove from the prefix.

    Returns:
        str: The string without the prefix.
    """
    # Use regex to match the prefix pattern
    pattern = f"^{re.escape(device_id)}(?:_\\d+)?_?"
    # Remove the matched prefix from the string
    return re.sub(pattern, "", name, count=1)


def decode_data_message(
    inbound_payload: Message, node_id: str, group_id: str, device_id: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Unified parser for Sparkplug data messages - handles both dewesoft and standard formats.
    """
    metrics = decode_message(inbound_payload)
    parsed_metrics: List[Dict[str, Any]] = []

    for metric in metrics:
        # For DataSet metrics, the data is in value after parse_metric processing
        metric_value = metric["value"]

        # Handle different metric data types
        if metric["datatype"] == MetricDataType.DataSet:
            columns = metric_value.get("columns", [])
            types = metric_value.get("types", [])
            rows = metric_value.get("rows", [])

            # Resolve alias if present and name is empty
            metric_name = metric.get("name", None)
            if not metric_name and "alias" in metric and metric["alias"] is not None:
                metric_alias = metric["alias"]
                if group_id in sparkplug_aliases and metric_alias in sparkplug_aliases[group_id]:
                    metric_name = sparkplug_aliases[group_id][metric_alias]["name"]

            if group_id == GROUP_ID_DEWESOFT:
                # Dewesoft specific DataSet parsing

                # Get metadata for measurementSR if available
                metric_metadata = sparkplug_metadata.get(group_id, {}).get(metric_name or "", {})
                dt = (
                    1.0 / metric_metadata["measurementSR"] * 1000.0 * 1000.0
                    if "measurementSR" in metric_metadata
                    else 0
                )

                # For Dewesoft there is always Timestamp and V where V is the value
                # Since we dont want a metric called Dewsoft with a field called V we need to get the field name
                # from the metric name
                if metric_name and device_id:
                    field_name = remove_prefix(metric_name, device_id)
                else:
                    field_name = metric_name or "unknown"

                # Process each row in the dataset
                for row in rows:
                    elements = row.get("elements", [])
                    timestamp = None

                    # Extract timestamp and values
                    for col_index, element in enumerate(elements):
                        if col_index < len(columns):
                            value_type = types[col_index]

                            if value_type == DataSetDataType.UInt64:
                                timestamp = element.get("long_value")
                            elif value_type == DataSetDataType.Double:
                                double_value = element.get("double_value")
                                if timestamp is not None and double_value is not None:
                                    # Use measurementSR if dt > 0, otherwise use direct timestamp
                                    sample_timestamp = timestamp + dt * col_index if dt > 0 else timestamp

                                    timestamp_str = datetime.datetime.fromtimestamp(
                                        sample_timestamp / 1000000,  # Convert microseconds to seconds
                                        tz=datetime.timezone.utc,
                                    ).isoformat()

                                    parsed_metrics.append(
                                        {"name": device_id, "timestamp": timestamp_str, field_name: double_value}
                                    )
            else:
                for row in rows:
                    metric_dict = {"name": metric_name, "timestamp": None}
                    elements = row.get("elements", [])

                    for col_index, element in enumerate(elements):
                        if col_index < len(columns):
                            field_name = columns[col_index]
                            if group_id in sparkplug_aliases:
                                field_alias = columns[col_index]
                                try:
                                    alias_key = int(field_alias)
                                except (ValueError, TypeError):
                                    alias_key = field_alias
                                if alias_key in sparkplug_aliases[group_id]:
                                    field_name = sparkplug_aliases[group_id][alias_key]["name"]

                            value_type = types[col_index]

                            value = None
                            if value_type in (
                                DataSetDataType.Int8,
                                DataSetDataType.Int16,
                                DataSetDataType.Int32,
                                DataSetDataType.UInt8,
                                DataSetDataType.UInt16,
                                DataSetDataType.UInt32,
                            ):
                                value = element.get("int_value")
                            elif value_type in (DataSetDataType.Int64, DataSetDataType.UInt64):
                                value = element.get("long_value")
                            elif value_type == DataSetDataType.Float:
                                value = element.get("float_value")
                            elif value_type == DataSetDataType.Double:
                                value = element.get("double_value")
                            elif value_type == DataSetDataType.Boolean:
                                value = element.get("boolean_value")
                            elif value_type in (DataSetDataType.String, DataSetDataType.Text):
                                value = element.get("string_value")

                            if value is not None:
                                metric_dict[field_name] = value

                    if len(metric_dict) > 2:  # Has more than name and timestamp
                        timestamp_from_metric = metric.get("timestamp")
                        if timestamp_from_metric:
                            timestamp_str = datetime.datetime.fromtimestamp(
                                timestamp_from_metric / 1000,
                                tz=datetime.timezone.utc,
                            ).isoformat()
                        else:
                            timestamp_str = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()

                        metric_dict["timestamp"] = timestamp_str
                        parsed_metrics.append(metric_dict)
        else:
            # Handle simple metrics (int, float, bool, string)
            timestamp_str = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()

            # Resolve alias if present and name is empty
            metric_name = metric.get("name", None)
            if not metric_name and "alias" in metric and metric["alias"] is not None:
                metric_name = sparkplug_aliases[group_id][metric["alias"]]["name"]

            value = metric["value"]

            if value is not None:
                parsed_metrics.append({"name": metric_name, "timestamp": timestamp_str, "value": value})

    return parsed_metrics


def decode_birth_message(inbound_payload: Message, node_id: str, group_id: str) -> None:
    """
    Unified parser for Sparkplug birth messages - handles both dewesoft and standard formats.
    """
    metrics = decode_message(inbound_payload)

    # Initialize group metadata and aliases if not exists
    if group_id not in sparkplug_metadata:
        sparkplug_metadata[group_id] = {}
    if group_id not in sparkplug_aliases:
        sparkplug_aliases[group_id] = {}

    for metric in metrics:
        # Store basic metadata for the metric
        sparkplug_metadata[group_id][metric["name"]] = {
            "name": metric["name"],
            "node_id": node_id,
            "datatype": metric.get("datatype", DataSetDataType.String),
            "alias": metric.get("alias"),
            "properties": metric.get("properties", {}),
        }

        alias = metric.get("alias")
        if alias is not None and isinstance(alias, int):
            sparkplug_aliases[group_id][alias] = sparkplug_metadata[group_id][metric["name"]]

        # Handle metadata description
        if "metadata" in metric and "description" in metric["metadata"]:
            if group_id == GROUP_ID_DEWESOFT:
                # Try XML parsing for dewesoft (backward compatibility)
                try:
                    metadata_xml = XML.fromstring(metric["metadata"]["description"])
                    for elem in metadata_xml.iter():
                        if elem.text is not None:
                            try:
                                sparkplug_metadata[group_id][metric["name"]][elem.tag] = float(elem.text)
                            except ValueError:
                                sparkplug_metadata[group_id][metric["name"]][elem.tag] = elem.text
                except XML.ParseError:
                    # If XML parsing fails, store as plain text
                    sparkplug_metadata[group_id][metric["name"]]["description"] = metric["metadata"]["description"]
            else:
                # For non-dewesoft, just store as description (no XML parsing required)
                sparkplug_metadata[group_id][metric["name"]]["description"] = metric["metadata"]["description"]
