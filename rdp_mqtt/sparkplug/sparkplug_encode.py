import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from .generated import sparkplug_b_pb2
from .sparkplug_shared import MetricDataType

seqNum = 0
bdSeq = 0
EXCLUDED_FIELDS = {"timestamp"}


def infer_datatype_from_value(value: Any) -> int:
    """
    Infer Sparkplug datatype from Python value.

    Args:
        value: Python value to analyze

    Returns:
        MetricDataType constant
    """
    if isinstance(value, bool):
        return MetricDataType.Boolean
    elif isinstance(value, int):
        # Use Int64 for most integers to avoid overflow issues
        if -128 <= value <= 127:
            return MetricDataType.Int8
        elif -32768 <= value <= 32767:
            return MetricDataType.Int16
        elif -2147483648 <= value <= 2147483647:
            return MetricDataType.Int32
        else:
            return MetricDataType.Int64
    elif isinstance(value, float):
        return MetricDataType.Double
    elif isinstance(value, str):
        return MetricDataType.String
    else:
        return MetricDataType.String  # Default to string for unknown types


def encode_birth_message(
    metrics: List[Dict[str, Any]],
    alias_map: Optional[Dict[str, int]] = None,
) -> Optional[bytes]:
    """
    Create NBIRTH message with metric definitions and aliases using a DataSet.

    Rules:
      - 'timestamp' field is ignored
      - 'name' key is ignored, but its value is aliased (datatype=String)
      - all other field names are aliased with datatype inferred from value
    """
    global seqNum

    if alias_map is None:
        alias_map = {}

    # Early exit: check if any new aliases are needed
    if not any(
        (m.get("name") and m["name"] not in alias_map)
        or any(f not in EXCLUDED_FIELDS and f not in alias_map for f in m if f != "name")
        for m in metrics
    ):
        return None

    payload = sparkplug_b_pb2.Payload()  # type: ignore
    payload.seq = seqNum
    current_alias = len(alias_map)

    def add_alias(name: str, datatype: int) -> None:
        nonlocal current_alias
        alias_map[name] = current_alias
        metric = sparkplug_b_pb2.Payload.Metric()  # type: ignore
        metric.name, metric.alias, metric.datatype = name, current_alias, datatype
        payload.metrics.append(metric)
        current_alias += 1

    for metric_dict in metrics:
        # Handle 'name' value as alias
        sensor_name = metric_dict.get("name")
        if sensor_name and sensor_name not in alias_map:
            # That's not really a string, but it must be something I guess
            # This is only a reference to a "metric" in our sense
            # and not a metric in Sparkplug`s sense
            add_alias(sensor_name, MetricDataType.String)

        # Handle all other fields (except timestamp)
        for field, value in metric_dict.items():
            if field not in EXCLUDED_FIELDS and field != "name" and field not in alias_map:
                add_alias(field, infer_datatype_from_value(value))

    seqNum += 1

    return payload.SerializeToString()  # type: ignore


def encode_node_birth_message() -> bytes:
    global seqNum

    payload = sparkplug_b_pb2.Payload()  # type: ignore
    payload.seq = seqNum

    seqNum += 1

    return payload.SerializeToString()  # type: ignore


def add_value_to_dataset_value(dataset_value: Any, value: Any, datatype: int) -> None:
    if datatype == MetricDataType.Boolean:
        dataset_value.boolean_value = bool(value)
    elif datatype in {
        MetricDataType.Int8,
        MetricDataType.Int16,
        MetricDataType.Int32,
        MetricDataType.UInt8,
        MetricDataType.UInt16,
        MetricDataType.UInt32,
    }:
        dataset_value.int_value = int(value)
    elif datatype in {MetricDataType.Int64, MetricDataType.UInt64}:
        dataset_value.long_value = int(value)
    elif datatype == MetricDataType.Float:
        dataset_value.float_value = float(value)
    elif datatype == MetricDataType.Double:
        dataset_value.double_value = float(value)
    elif datatype in {MetricDataType.String, MetricDataType.Text}:
        dataset_value.string_value = str(value)


def encode_data_message(
    metrics: List[Dict[str, Any]],
    alias_map: Optional[Dict[str, Any]] = None,
    use_aliases_only: bool = False,
) -> bytes:
    """
    Encode a list of RDP Metrics into a Sparkplug B NDATA payload using DataSet metric.

    Args:
        metrics: List of RDP Metric objects
        alias_map: Dictionary mapping metric and field names to alias IDs
        use_aliases_only: If True, use only aliases (no names) for efficiency

    Returns:
        Serialized Sparkplug B payload as bytes
    """
    global seqNum

    if alias_map is None:
        alias_map = {}

    # Create main payload
    payload = sparkplug_b_pb2.Payload()  # type: ignore
    payload.seq = seqNum

    for metric_dict in metrics:
        # Create a single metric of type DataSet
        sparkplug_metric = sparkplug_b_pb2.Payload.Metric()  # type: ignore

        timestamp_value = metric_dict.get("timestamp")
        if timestamp_value:
            if isinstance(timestamp_value, (int, float)):
                # Handle Unix timestamp in milliseconds
                sparkplug_metric.timestamp = int(timestamp_value)
            else:
                # Handle ISO string format
                dt_object = datetime.fromisoformat(str(timestamp_value).replace("Z", "+00:00"))
                sparkplug_metric.timestamp = int(dt_object.timestamp() * 1000)
        else:
            # Else we set a timestamp to now
            sparkplug_metric.timestamp = int(time.time() * 1000)

        # Use alias if available
        metric_name = metric_dict.get("name", "")

        alias_id = alias_map.get(metric_name)
        if use_aliases_only and alias_id is not None:
            sparkplug_metric.alias = alias_id
            sparkplug_metric.name = ""
        elif alias_id:
            sparkplug_metric.alias = alias_id
            sparkplug_metric.name = metric_name
        else:
            sparkplug_metric.name = metric_name

        sparkplug_metric.datatype = MetricDataType.DataSet

        # Create the DataSet - get fields (exclude name and timestamp)
        field_items = [(k, v) for k, v in metric_dict.items() if k not in ["name", "timestamp"]]

        dataset = sparkplug_b_pb2.Payload.DataSet()  # type: ignore
        dataset.num_of_columns = len(field_items)

        field_aliases = []
        for field_name, _field_value in field_items:
            field_aliases.append(alias_map.get(field_name, field_name))
        dataset.columns.extend([str(a) for a in field_aliases])

        datatypes = [infer_datatype_from_value(field_value) for field_name, field_value in field_items]
        dataset.types.extend(datatypes)

        # Create a single row for the DataSet
        row = dataset.rows.add()
        for i, (_field_name, field_value) in enumerate(field_items):
            dataset_value = row.elements.add()
            add_value_to_dataset_value(dataset_value, field_value, datatypes[i])

        sparkplug_metric.dataset_value.CopyFrom(dataset)
        payload.metrics.append(sparkplug_metric)

    seqNum += 1

    return payload.SerializeToString()  # type: ignore


def get_sparkplug_topic(group_id: str, node_id: str, message_type: str = "NDATA") -> str:
    """
    Generate Sparkplug B topic for publishing.
    """
    return f"spBv1.0/{group_id}/{message_type}/{node_id}"
