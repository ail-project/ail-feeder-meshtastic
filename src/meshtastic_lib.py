#!/usr/bin/env python
"""
Minimal Meshtastic helpers to let meshtastic-ail.py run.
Provides MQTT connection defaults and decoding helpers.
"""

from __future__ import annotations

import base64
import binascii
import json
import os
import struct
import time
from typing import Any, Dict, Optional, Tuple

try:
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
except Exception:
    Cipher = None
    algorithms = None
    modes = None

try:
    from meshtastic.protobuf import mesh_pb2
except Exception:
    mesh_pb2 = None
try:
    from meshtastic.protobuf import portnums_pb2
except Exception:
    portnums_pb2 = None
try:
    from meshtastic.protobuf import mqtt_pb2
except Exception:
    mqtt_pb2 = None
try:
    from meshtastic.protobuf import telemetry_pb2
except Exception:
    telemetry_pb2 = None
try:
    from meshtastic.protobuf import neighborinfo_pb2
except Exception:
    neighborinfo_pb2 = None

try:
    from google.protobuf.json_format import MessageToDict
except Exception:
    MessageToDict = None

# MQTT defaults (same as lightclient)
BROKER = os.getenv("MESH_MQTT_HOST", "mqtt.somewhere.com")
PORT = int(os.getenv("MESH_MQTT_PORT", "1883"))
USERNAME = os.getenv("MESH_MQTT_USER", "user")
PASSWORD = os.getenv("MESH_MQTT_PASS", "password")
TOPIC = os.getenv("MESH_MQTT_TOPIC", "msh/EU/#")
CHANNEL_FILTER = os.getenv("MESH_CHANNEL", "LongFast")
CLIENT_ID = os.getenv("MESH_MQTT_CLIENT_ID", f"lightclient-{int(time.time())}")

DEFAULT_CHANNEL_NAMES = {
    "ShortTurbo",
    "ShortSlow",
    "ShortFast",
    "MediumSlow",
    "MediumFast",
    "LongSlow",
    "LongFast",
    "LongTurbo",
    "LongMod",
}

DEFAULT_PSK = bytes(
    [
        0xD4,
        0xF1,
        0xBB,
        0x3A,
        0x20,
        0x29,
        0x07,
        0x59,
        0xF0,
        0xBC,
        0xFF,
        0xAB,
        0xCF,
        0x4E,
        0x69,
        0x01,
    ]
)

def _normalize_channel(name: Optional[str]) -> Optional[str]:
    """Trim and normalize channel name; return None if missing."""
    if not name:
        return None
    return str(name).strip()


def apply_config(cfg: Dict[str, Any]) -> None:
    """Apply MQTT/config overrides from a dict (no PSK support here)."""
    global BROKER, PORT, USERNAME, PASSWORD, TOPIC, CHANNEL_FILTER, CLIENT_ID
    if not cfg:
        return
    BROKER = cfg.get("broker", BROKER)
    if "port" in cfg:
        try:
            PORT = int(cfg["port"])
        except Exception:
            pass
    USERNAME = cfg.get("username", USERNAME)
    PASSWORD = cfg.get("password", PASSWORD)
    TOPIC = cfg.get("topic", TOPIC)
    CHANNEL_FILTER = cfg.get("channel", CHANNEL_FILTER)
    CLIENT_ID = cfg.get("client_id", CLIENT_ID)


TEXT_PORTNUM = 1
TELEMETRY_PORTNUM = 67
POSITION_PORTNUM = 3
NODEINFO_PORTNUM = 4
ROUTING_PORTNUM = 5
WAYPOINT_PORTNUM = 8
NEIGHBORINFO_PORTNUM = 71
MAP_REPORT_PORTNUM = 73


def _safe_json(payload_bytes: bytes) -> Optional[Dict[str, Any]]:
    """Decode UTF-8 JSON payload into dict; return None on failure."""
    try:
        text = payload_bytes.decode("utf-8")
    except UnicodeDecodeError:
        return None
    try:
        obj = json.loads(text)
    except json.JSONDecodeError:
        return None
    if isinstance(obj, dict):
        return obj
    return None


def _find_channel_from_topic(topic: str) -> Optional[str]:
    """Extract channel name from MQTT topic segments."""
    chan = _extract_channel_from_topic(topic)
    if chan:
        return chan
    parts = topic.split("/")
    for part in parts:
        if part.lower() == "longfast":
            return "LongFast"
    return None


def _find_channel_from_json(payload: Dict[str, Any]) -> Optional[str]:
    """Look for channel name within likely keys of a JSON payload."""
    direct_keys = [
        "channel",
        "channel_name",
        "channelName",
        "chan",
        "channelId",
        "channel_id",
    ]
    for key in direct_keys:
        val = payload.get(key)
        if isinstance(val, str):
            return val
    decoded = payload.get("decoded")
    if isinstance(decoded, dict):
        for key in direct_keys:
            val = decoded.get(key)
            if isinstance(val, str):
                return val
    for subkey in ["payload", "user", "from", "to"]:
        val = payload.get(subkey)
        if isinstance(val, dict):
            for key in direct_keys:
                v2 = val.get(key)
                if isinstance(v2, str):
                    return v2
    return None


def _extract_channel_from_topic(topic: str) -> Optional[str]:
    """Return the MQTT segment immediately before a '!nodeid' if present, else the last segment."""
    parts = [p for p in topic.split("/") if p]
    for idx, part in enumerate(parts):
        if part.startswith("!"):  # take the label right before the node marker
            return parts[idx - 1] if idx > 0 else None
    return parts[-1] if parts else None


def _maybe_decode_ascii_payload(payload_bytes: bytes) -> Tuple[bytes, bool]:
    """If payload looks like hex/base64 ASCII, decode it; return (data, changed?)."""
    try:
        s = payload_bytes.decode("ascii")
    except Exception:
        return payload_bytes, False
    if len(s) % 2 == 0 and all(c in "0123456789abcdefABCDEF" for c in s):
        try:
            return binascii.unhexlify(s), True
        except Exception:
            pass
    if all(c.isalnum() or c in "+/=" for c in s):
        try:
            return base64.b64decode(s, validate=True), True
        except Exception:
            pass
    return payload_bytes, False


def _portnum_name(portnum: int) -> Optional[str]:
    """Return human-readable portnum name if protobufs are available."""
    if portnums_pb2 is None:
        return None
    try:
        return portnums_pb2.PortNum.Name(int(portnum))
    except Exception:
        return None


def _decode_text_payload(portnum: int, payload: bytes) -> Optional[str]:
    """Decode UTF-8 text if payload belongs to TEXT_PORTNUM."""
    if portnum != TEXT_PORTNUM or not payload:
        return None
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError:
        return None


def _decode_telemetry_payload(portnum: int, payload: bytes) -> Optional[Dict[str, Any]]:
    """Decode Telemetry protobuf to dict when possible."""
    if telemetry_pb2 is None or portnum != TELEMETRY_PORTNUM or not payload:
        return None
    msg = telemetry_pb2.Telemetry()
    try:
        msg.ParseFromString(payload)
    except Exception:
        return None
    if MessageToDict:
        return MessageToDict(msg, preserving_proto_field_name=True)
    return {"telemetry": str(msg)}


def _decode_proto_message(msg_cls: Any, payload: bytes) -> Optional[Dict[str, Any]]:
    """Generic protobuf -> dict decoder helper."""
    if msg_cls is None or not payload:
        return None
    msg = msg_cls()
    try:
        msg.ParseFromString(payload)
    except Exception:
        return None
    if MessageToDict:
        return MessageToDict(msg, preserving_proto_field_name=True)
    return {"message": str(msg)}


def _decode_generic_payload(portnum: int, payload: bytes) -> Optional[Dict[str, Any]]:
    """Decode various known portnum payloads into dicts."""
    if portnum == POSITION_PORTNUM:
        return _decode_proto_message(mesh_pb2.Position if mesh_pb2 else None, payload)
    if portnum == NODEINFO_PORTNUM:
        return _decode_proto_message(mesh_pb2.NodeInfo if mesh_pb2 else None, payload)
    if portnum == ROUTING_PORTNUM:
        return _decode_proto_message(mesh_pb2.Routing if mesh_pb2 else None, payload)
    if portnum == WAYPOINT_PORTNUM:
        return _decode_proto_message(mesh_pb2.Waypoint if mesh_pb2 else None, payload)
    if portnum == NEIGHBORINFO_PORTNUM:
        return _decode_proto_message(
            neighborinfo_pb2.NeighborInfo if neighborinfo_pb2 else None, payload
        )
    if portnum == MAP_REPORT_PORTNUM:
        return _decode_proto_message(mqtt_pb2.MapReport if mqtt_pb2 else None, payload)
    return None


def _aes_ctr_decrypt(key: bytes, nonce: bytes, data: bytes) -> Optional[bytes]:
    """AES-CTR decrypt helper; returns plaintext or None on failure."""
    if Cipher is None or algorithms is None or modes is None:
        return None
    if len(nonce) != 16:
        return None
    try:
        cipher = Cipher(algorithms.AES(key), modes.CTR(nonce))
        decryptor = cipher.decryptor()
        return decryptor.update(data) + decryptor.finalize()
    except Exception:
        return None


def _try_decrypt_default_channel(packet_info: Dict[str, Any], channel_id: Optional[str]) -> Optional[Dict[str, Any]]:
    """Attempt decryption using hardcoded default PSKs for known channels."""
    channel_id = _normalize_channel(channel_id)
    if not channel_id or channel_id not in DEFAULT_CHANNEL_NAMES:
        return None
    encrypted_hex = packet_info.get("encrypted_hex")
    if not isinstance(encrypted_hex, str):
        return None
    try:
        encrypted = binascii.unhexlify(encrypted_hex)
    except Exception:
        return None
    from_hex = packet_info.get("from")
    pkt_id = packet_info.get("id")
    if not isinstance(from_hex, str) or pkt_id is None:
        return None
    try:
        from_num = int(from_hex, 16)
        pkt_id_num = int(pkt_id)
    except Exception:
        return None

    nonce = struct.pack("<Q", pkt_id_num) + struct.pack("<I", from_num) + b"\x00\x00\x00\x00"
    keys = [DEFAULT_PSK] + [DEFAULT_PSK[:-1] + bytes([(DEFAULT_PSK[-1] + i) & 0xFF]) for i in range(1, 10)]
    for key in keys:
        plain = _aes_ctr_decrypt(key, nonce, encrypted)
        if plain is None:
            continue
        decoded = _decode_data_message(plain)
        if decoded is not None:
            decoded["decrypted"] = True
            return decoded
    return None


def _decode_data_message(payload: bytes) -> Optional[Dict[str, Any]]:
    """Decode mesh Data message into dict of text/telemetry/decoded payload."""
    if mesh_pb2 is None or not payload:
        return None
    data = mesh_pb2.Data()
    try:
        data.ParseFromString(payload)
    except Exception:
        return None
    portnum = int(getattr(data, "portnum", 0))
    info: Dict[str, Any] = {"portnum": portnum}
    pname = _portnum_name(portnum)
    if pname:
        info["portnum_name"] = pname
    pl = getattr(data, "payload", b"")
    text = _decode_text_payload(portnum, pl)
    if text is not None:
        info["text"] = text
    telemetry = _decode_telemetry_payload(portnum, pl)
    if telemetry is not None:
        info["telemetry"] = telemetry
    generic = _decode_generic_payload(portnum, pl)
    if generic is not None:
        info["decoded_payload"] = generic
    if (
        pl
        and "text" not in info
        and "telemetry" not in info
        and "decoded_payload" not in info
    ):
        info["payload_hex"] = pl.hex()
    return info


def _decode_meshpacket(payload_bytes: bytes) -> Optional[Dict[str, Any]]:
    """Decode MeshPacket; if encrypted, keep encrypted_hex for later decryption."""
    if mesh_pb2 is None:
        return None
    pkt = mesh_pb2.MeshPacket()
    try:
        consumed = pkt.MergeFromString(payload_bytes)
    except Exception:
        return None
    if consumed <= 0:
        return None
    from_field = getattr(pkt, "from_", None)
    if from_field in (None, 0):
        from_field = getattr(pkt, "from", 0)
    to_field = getattr(pkt, "to", 0)
    info: Dict[str, Any] = {
        "from": f"{from_field:08x}" if from_field else None,
        "to": f"{to_field:08x}" if to_field else None,
        "id": getattr(pkt, "id", None),
        "channel": getattr(pkt, "channel", None),
    }
    if pkt.HasField("decoded"):
        data = pkt.decoded
        portnum = int(getattr(data, "portnum", 0))
        info["portnum"] = portnum
        pname = _portnum_name(portnum)
        if pname:
            info["portnum_name"] = pname
        payload = getattr(data, "payload", b"")
        text = _decode_text_payload(portnum, payload)
        if text is not None:
            info["text"] = text
        telemetry = _decode_telemetry_payload(portnum, payload)
        if telemetry is not None:
            info["telemetry"] = telemetry
        generic = _decode_generic_payload(portnum, payload)
        if generic is not None:
            info["decoded_payload"] = generic
        if (
            payload
            and "text" not in info
            and "telemetry" not in info
            and "decoded_payload" not in info
        ):
            info["payload_hex"] = payload.hex()
    else:
        encrypted = getattr(pkt, "encrypted", b"")
        if encrypted:
            info["encrypted_hex"] = encrypted.hex()
    if consumed < len(payload_bytes):
        info["trailing_hex"] = payload_bytes[consumed:].hex()
    return info


def _decode_service_envelope(payload_bytes: bytes) -> Optional[Dict[str, Any]]:
    """Decode ServiceEnvelope, then try decrypting packet with default PSKs."""
    if mqtt_pb2 is None:
        return None
    env = mqtt_pb2.ServiceEnvelope()
    try:
        consumed = env.MergeFromString(payload_bytes)
    except Exception:
        return None
    if consumed <= 0:
        return None
    info: Dict[str, Any] = {}
    if getattr(env, "channel_id", None):
        info["channel_id"] = env.channel_id
    if getattr(env, "gateway_id", None):
        info["gateway_id"] = env.gateway_id
    if getattr(env, "packet", None):
        pkt_bytes = env.packet.SerializeToString()
        pkt_info = _decode_meshpacket(pkt_bytes)
        if pkt_info is not None:
            info["packet"] = pkt_info
            # Try decrypting encrypted payloads using default channel PSKs only.
            if isinstance(pkt_info, dict) and "encrypted_hex" in pkt_info and "text" not in pkt_info:
                decrypted = _try_decrypt_default_channel(pkt_info, info.get("channel_id"))
                if decrypted is not None:
                    pkt_info.update(decrypted)
                # Also annotate channel_id on packet to keep context for downstream consumers.
                if info.get("channel_id") is not None:
                    pkt_info.setdefault("channel_id", info.get("channel_id"))
    if consumed < len(payload_bytes):
        info["trailing_hex"] = payload_bytes[consumed:].hex()
    return info
