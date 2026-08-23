"""The fast metric-to-dict path must be indistinguishable from protobuf_to_dict."""

from rdp_mqtt.sparkplug.generated import sparkplug_b_pb2
from rdp_mqtt.sparkplug.protobuf_to_dict import protobuf_to_dict
from rdp_mqtt.sparkplug.sparkplug_decode import _metric_to_dict_fast, decode_mqtt_message, remove_prefix

# A captured DDATA message: one DataSet metric with Timestamp/V columns and one row.
DATASET_PAYLOAD = bytes.fromhex(
    "080012480a1364657765736f66745f7076325f656e65726779100020108a012c0802120954696d657374616d70"
    "1201561a02080a22160a09108890aafec3b796030a09210000008037108d4018c401"
)


class Msg:
    def __init__(self, topic: str, payload: bytes) -> None:
        self.topic = topic
        self.payload = payload


def test_fast_path_matches_generic_for_dataset_payload():
    payload = sparkplug_b_pb2.Payload()
    payload.ParseFromString(DATASET_PAYLOAD)
    for metric in payload.metrics:
        assert _metric_to_dict_fast(metric) == protobuf_to_dict(metric)


def test_fast_path_matches_generic_for_scalar_metrics():
    payload = sparkplug_b_pb2.Payload()
    m = payload.metrics.add()
    m.name = "x"
    m.timestamp = 123
    m.datatype = 10
    m.double_value = 1.5
    m2 = payload.metrics.add()  # nothing set except datatype
    m2.datatype = 12
    m2.boolean_value = True
    for metric in payload.metrics:
        assert _metric_to_dict_fast(metric) == protobuf_to_dict(metric)


def test_metric_with_metadata_falls_back_to_generic():
    payload = sparkplug_b_pb2.Payload()
    m = payload.metrics.add()
    m.name = "x"
    m.metadata.description = "<a>1</a>"
    assert _metric_to_dict_fast(m) is None  # decode_message_element then uses protobuf_to_dict


def test_decode_mqtt_message_end_to_end():
    msg = Msg("spBv1.0/dewesoftx/DDATA/plugin/dewesoft", DATASET_PAYLOAD)
    metrics = decode_mqtt_message(msg)
    assert metrics == [
        {
            "name": "dewesoft",
            "timestamp": metrics[0]["timestamp"],
            "pv2_energy": 930.027099609375,
        }
    ]
    assert metrics[0]["timestamp"].startswith("2026-")


def test_remove_prefix_cached_pattern():
    assert remove_prefix("dewesoft_pv2_energy", "dewesoft") == "pv2_energy"
    assert remove_prefix("dewesoft_2_x", "dewesoft") == "x"
    assert remove_prefix("other", "dewesoft") == "other"
