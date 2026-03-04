#!/usr/bin/env python
"""
AIL feeder for meshtastic MQTT messages
"""

from __future__ import annotations

import json
import uuid
import threading
import time
import signal
import sqlite3
import os
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

try:
    import paho.mqtt.client as mqtt
except Exception as exc:  # pragma: no cover - runtime dependency
    print(
        "Missing dependency: paho-mqtt. Install with: pip install paho-mqtt",
        file=sys.stderr,
    )
    raise

# Ensure local modules are importable when run from repo root.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import meshtastic_lib as lc

DB_PATH = os.getenv("MESH_AIL_DB", "meshtastic_ail.sqlite")
CACHE: Dict[str, Dict[str, Any]] = {}
CACHE_LOCK = threading.Lock()
DEFAULT_FLUSH_INTERVAL = 5
DEFAULT_EXPIRY_SECONDS = 15
FLUSH_INTERVAL = DEFAULT_FLUSH_INTERVAL
EXPIRY_SECONDS = DEFAULT_EXPIRY_SECONDS


def _utc_now() -> str:
    """Return current UTC timestamp in ISO-8601 with Z suffix (second precision)."""
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _init_db(path: str) -> sqlite3.Connection:
    """Open (and create if missing) the SQLite DB and ensure the uids table exists."""
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS uids (
            uid TEXT PRIMARY KEY,
            first_seen TEXT NOT NULL,
            last_seen TEXT NOT NULL,
            count INTEGER NOT NULL DEFAULT 1,
            lat REAL,
            lon REAL,
            alt REAL
        )
        """
    )
    cols = {row[1] for row in conn.execute("PRAGMA table_info(uids)")}
    for col in ("lat", "lon", "alt"):
        if col not in cols:
            conn.execute(f"ALTER TABLE uids ADD COLUMN {col} REAL")
    conn.commit()
    return conn


def _load_config() -> Dict[str, Any]:
    """Load YAML config from config.yaml next to this file; return empty dict on failure."""
    paths = [Path(__file__).resolve().parent / "config.yaml"]
    for p in paths:
        if p.exists():
            try:
                import yaml  # type: ignore
            except Exception:
                print(
                    "Warning: config.yaml present but PyYAML not installed; skipping config.",
                    file=sys.stderr,
                )
                return {}
            try:
                data = yaml.safe_load(p.read_text()) or {}
                if isinstance(data, dict):
                    return data
            except Exception as exc:
                print(f"Warning: failed to parse {p}: {exc}", file=sys.stderr)
                return {}
    return {}


def _record_uid(
    conn: sqlite3.Connection,
    uid: Optional[str],
    position: Optional[Dict[str, Any]] = None,
) -> None:
    """Upsert a UID row with timestamps and optional lat/lon/alt."""
    if not uid:
        return
    now = _utc_now()
    lat = position.get("latitude") if isinstance(position, dict) else None
    lon = position.get("longitude") if isinstance(position, dict) else None
    alt = position.get("altitude") if isinstance(position, dict) else None
    conn.execute(
        """
        INSERT INTO uids (uid, first_seen, last_seen, count, lat, lon, alt)
        VALUES (?, ?, ?, 1, ?, ?, ?)
        ON CONFLICT(uid) DO UPDATE SET
            last_seen=excluded.last_seen,
            count=count+1,
            lat=COALESCE(excluded.lat, lat),
            lon=COALESCE(excluded.lon, lon),
            alt=COALESCE(excluded.alt, alt)
        """,
        (uid, now, now, lat, lon, alt),
    )
    conn.commit()


def _fetch_uid(
    conn: sqlite3.Connection, uid: Optional[str]
) -> Optional[Dict[str, Any]]:
    """Fetch persisted metadata for a UID (first/last seen, coords, count)."""
    if not uid:
        return None
    cur = conn.execute(
        "SELECT first_seen, last_seen, lat, lon, alt, count FROM uids WHERE uid = ?",
        (uid,),
    )
    row = cur.fetchone()
    if not row:
        return None
    first_seen, last_seen, lat, lon, alt, count = row
    return {
        "first_seen": first_seen,
        "last_seen": last_seen,
        "lat": lat,
        "lon": lon,
        "alt": alt,
        "count": count,
    }


def _extract_relays(payload: Dict[str, Any]) -> List[str]:
    """Collect relay IDs from various likely keys in the decoded payload."""
    relay_keys = ("relay", "relays", "via", "hops", "path", "route")
    relays: List[str] = []
    for key in relay_keys:
        val = payload.get(key)
        if isinstance(val, list):
            relays.extend(str(item) for item in val if item is not None)
        elif isinstance(val, str):
            relays.extend([v for v in val.split(",") if v])
    return [r for r in relays if r]


def _pick_channel(topic: str, payload_json: Optional[Dict[str, Any]]) -> Optional[str]:
    """Pick channel from topic, then payload JSON, else fall back to configured filter."""
    chan = lc._find_channel_from_topic(topic)
    if payload_json:
        from_json = lc._find_channel_from_json(payload_json)
        if from_json:
            chan = from_json
    return chan or lc.CHANNEL_FILTER


def _extract_from(
    payload: Dict[str, Any], fallback: Optional[str] = None
) -> Optional[str]:
    """Extract sender ID from common keys; return fallback if absent."""
    for key in ("from", "sender", "gateway_id", "gateway"):
        val = payload.get(key)
        if isinstance(val, str):
            return val
    return fallback


def _topic_base_and_uid(topic: str) -> tuple[str, Optional[str]]:
    """Split MQTT topic into base URL portion and trailing UID (if present after '!')."""
    uid = None
    if "!" in topic:
        uid = topic.split("!")[-1].strip("/")
    base = topic
    marker = "/e/"
    idx = topic.find(marker)
    if idx != -1:
        base = topic[: idx + len(marker)]
    return base, uid


def _generate_uid(sender: Optional[str], text: Optional[str]) -> Optional[str]:
    """Derive deterministic UID from sender+text using UUIDv5; None if missing inputs."""
    if not sender or not text:
        return None
    raw = f"{sender}:{text}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, raw))


def _emit_json(obj: Dict[str, Any]) -> None:
    """Print JSON to stdout, stripping telemetry blob to keep lines small."""
    try:
        if "telemetry" in obj:
            obj = {k: v for k, v in obj.items() if k != "telemetry"}
        print(json.dumps(obj, ensure_ascii=False))
        sys.stdout.flush()
    except BrokenPipeError:
        sys.exit(0)


def _merge_relays(existing: List[str], incoming: List[str]) -> List[str]:
    """Merge relay ID lists while preserving order and uniqueness."""
    seen = set()
    merged: List[str] = []
    for r in existing + incoming:
        if r and r not in seen:
            seen.add(r)
            merged.append(r)
    return merged


def _enrich_event(conn: sqlite3.Connection, event: Dict[str, Any]) -> Dict[str, Any]:
    """Attach first/last seen and location metadata to an event if known."""
    enriched = event.copy()
    info = _fetch_uid(conn, event.get("from"))
    if info:
        enriched["first_seen"] = info.get("first_seen")
        enriched["last_seen"] = info.get("last_seen")
        if info.get("lat") is not None:
            enriched["lat"] = info.get("lat")
        if info.get("lon") is not None:
            enriched["lon"] = info.get("lon")
        if info.get("alt") is not None:
            enriched["alt"] = info.get("alt")
        enriched["seen_count"] = info.get("count")
    return enriched


def _print_event(conn: sqlite3.Connection, event: Dict[str, Any]) -> None:
    """Print a single event after enrichment."""
    _emit_json(_enrich_event(conn, event))


def _cache_event(conn: sqlite3.Connection, event: Dict[str, Any]) -> None:
    """Coalesce identical UID events briefly, then flush enriched outputs."""
    uid = event.get("uid")
    if not uid:
        _print_event(conn, event)
        return
    now_ts = time.time()
    with CACHE_LOCK:
        current = CACHE.get(uid)
        if current is None:
            CACHE[uid] = {
                "event": event,
                "first_seen_ts": now_ts,
                "last_seen_ts": now_ts,
            }
            return
        current_event = current["event"]
        merged = current_event.copy()
        merged["relay"] = _merge_relays(
            current_event.get("relay", []) or [], event.get("relay", []) or []
        )
        merged["text"] = event.get("text") or current_event.get("text")
        merged["telemetry"] = event.get("telemetry") or current_event.get("telemetry")
        merged["channel"] = current_event.get("channel") or event.get("channel")
        merged["from"] = current_event.get("from") or event.get("from")
        merged["url"] = current_event.get("url") or event.get("url")
        current["event"] = merged
        current["last_seen_ts"] = now_ts


def _flush_cache(conn: sqlite3.Connection, expiry_seconds: float = 15.0) -> None:
    """Flush cached events whose last_seen_ts exceeds expiry_seconds."""
    now_ts = time.time()
    to_flush: list[Dict[str, Any]] = []
    with CACHE_LOCK:
        stale_keys = []
        for uid, entry in CACHE.items():
            last_seen_ts = entry.get("last_seen_ts", 0)
            if now_ts - last_seen_ts >= expiry_seconds:
                to_flush.append(entry["event"])
                stale_keys.append(uid)
        for uid in stale_keys:
            CACHE.pop(uid, None)
    for ev in to_flush:
        _print_event(conn, ev)


def _decode_payload(
    topic: str, payload_bytes: bytes
) -> tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Try plain JSON, then ServiceEnvelope, then MeshPacket decoding paths."""
    payload_json = lc._safe_json(payload_bytes)
    if payload_json is not None:
        return payload_json, None
    decoded_env = lc._decode_service_envelope(payload_bytes)
    if decoded_env is not None:
        pkt = decoded_env.get("packet") or {}
        # attach channel_id to decoded packet for downstream filtering/enrichment
        if isinstance(pkt, dict) and decoded_env.get("channel_id") and "channel_id" not in pkt:
            pkt["channel_id"] = decoded_env.get("channel_id")
        return None, pkt
    decoded_pkt = lc._decode_meshpacket(payload_bytes)
    return None, decoded_pkt


def _extract_position(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extract latitude/longitude/altitude from decoded payload variants."""
    if not isinstance(payload, dict):
        return None
    pos = (
        payload.get("decoded_payload")
        or payload.get("position")
        or payload.get("payload")
    )
    if not isinstance(pos, dict):
        return None
    lat_i = pos.get("latitude_i")
    lon_i = pos.get("longitude_i")
    lat = pos.get("latitude")
    lon = pos.get("longitude")
    if lat is None and isinstance(lat_i, (int, float)):
        lat = float(lat_i) / 1e7
    if lon is None and isinstance(lon_i, (int, float)):
        lon = float(lon_i) / 1e7
    if lat is None or lon is None:
        return None
    alt = pos.get("altitude") or pos.get("altitude_m")
    return {"latitude": lat, "longitude": lon, "altitude": alt}


def _handle_message(conn: sqlite3.Connection, topic: str, payload_bytes: bytes) -> None:
    """Process one MQTT message: decode, enrich, dedupe, persist, and emit."""
    payload_json, decoded = _decode_payload(topic, payload_bytes)
    channel = _pick_channel(topic, payload_json)
    if not channel and decoded and isinstance(decoded, dict):
        channel = decoded.get("channel") or decoded.get("channel_id") or decoded.get("channelId")
    base_url, uid_from_topic = _topic_base_and_uid(topic)
    source = decoded or payload_json or {}
    from_id = _extract_from(source, fallback=None)
    relays = [uid_from_topic] if uid_from_topic else _extract_relays(source)

    text_val: Optional[str] = None
    telemetry_val: Optional[Dict[str, Any]] = None
    if payload_json:
        msg_type = payload_json.get("type")
        payload_obj = payload_json.get("payload")
        if msg_type == "text":
            if isinstance(payload_obj, dict) and "text" in payload_obj:
                text_val = payload_obj.get("text")
            elif isinstance(payload_obj, str):
                text_val = payload_obj
            elif payload_obj is not None:
                text_val = json.dumps(payload_obj, ensure_ascii=False)
        if msg_type == "telemetry" and isinstance(payload_json, dict):
            telemetry_val = payload_json

    position: Optional[Dict[str, Any]] = None
    if decoded:
        if isinstance(decoded.get("text"), str):
            text_val = decoded["text"]
        if isinstance(decoded.get("telemetry"), dict):
            telemetry_val = decoded["telemetry"]
        position = _extract_position(decoded)
        if position:
            telemetry_val = position

    def _has_location(t: Dict[str, Any]) -> bool:
        return (
            isinstance(t, dict)
            and ("latitude" in t or "lat" in t)
            and ("longitude" in t or "lon" in t)
        )

    if telemetry_val and not _has_location(telemetry_val):
        telemetry_val = None

    if text_val is None and telemetry_val is not None:
        position_for_db = telemetry_val if isinstance(telemetry_val, dict) else None
        _record_uid(conn, from_id, position_for_db)
        for rid in relays:
            if rid:
                _record_uid(conn, rid, position_for_db)
        return

    if text_val is None:
        return

    position_for_db = telemetry_val if isinstance(telemetry_val, dict) else position
    _record_uid(conn, from_id, position_for_db)
    for rid in relays:
        if rid:
            _record_uid(conn, rid, position_for_db)

    uid_val = _generate_uid(from_id, text_val)
    event: Dict[str, Any] = {
        "url": f"{lc.BROKER}@{base_url}",
        "channel": channel,
        "from": from_id,
        "relay": relays,
        "uid": uid_val,
        "text": text_val,
        "telemetry": telemetry_val,
        "timestamp": _utc_now(),
    }
    _cache_event(conn, event)


def main() -> int:
    """Connect to MQTT, stream messages, flush cache periodically."""
    try:
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    except Exception:
        pass
    cfg = _load_config()
    lc.apply_config(cfg)
    global FLUSH_INTERVAL, EXPIRY_SECONDS
    if cfg:
        FLUSH_INTERVAL = int(cfg.get("flush_interval", FLUSH_INTERVAL))
        EXPIRY_SECONDS = int(cfg.get("expiry_seconds", EXPIRY_SECONDS))

    conn = _init_db(DB_PATH)

    def on_connect(
        client: mqtt.Client,
        userdata: Any,
        connect_flags: Any,
        reason_code: Any,
        properties: Any = None,
    ) -> None:
        rc = getattr(reason_code, "value", reason_code)
        if rc == 0:
            client.subscribe(lc.TOPIC)
        else:
            print(f"Connection failed: reason={reason_code}", file=sys.stderr)

    def on_message(client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:
        payload_bytes, _ = lc._maybe_decode_ascii_payload(msg.payload)
        try:
            _handle_message(conn, msg.topic, payload_bytes)
        except Exception as exc:
            print(f"Error processing message: {exc}", file=sys.stderr)

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=lc.CLIENT_ID,
        clean_session=True,
    )
    client.username_pw_set(lc.USERNAME, lc.PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(lc.BROKER, lc.PORT, keepalive=60)
    client.loop_start()
    try:
        while True:
            time.sleep(FLUSH_INTERVAL)
            _flush_cache(conn, expiry_seconds=EXPIRY_SECONDS)
    except KeyboardInterrupt:
        pass
    finally:
        _flush_cache(conn, expiry_seconds=0)
        client.loop_stop()
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
