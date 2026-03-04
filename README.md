# Meshtastic AIL Feeder (Autonomous)

## Todo
Upload to AIL  

Small script to acquire meshtastic MQTT relayed messages.

## Files
- `meshtastic-ail.py`: main runner.  
- `meshtastic_lib.py`: MQTT/decoding helpers (default PSK only).  
- `config.yaml`: local MQTT config.  
- `meshtastic_ail.sqlite`: SQLite DB of seen UIDs.  

## Requirements
- Python 3.11+  
- Dependencies: `paho-mqtt`, `protobuf`, `cryptography`, `PyYAML`.

## Quick install
```bash
cd ail-feeder-meshtastic/src
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt  # or: pip install paho-mqtt protobuf cryptography pyyaml
```

## Configuration
Edit `config.yaml` (source `config.yaml.sample`):
```yaml
broker: mqtt.server.address
port: 1883
username: username
password: password
topic: msh/EU868/#
channel: LongFast
client_id: lightclient
flush_interval: 5
expiry_seconds: 15
```

## Run
```bash
cd ail-feeder-meshtastic/src
python3 meshtastic-ail.py
```

### JSON output example
Each line is one event, for example:
```json
{
  "url": "[REDACTED]@[REDACTED]/EKB/2/e/",
  "channel": "MediumFast",
  "from": "b2a70ee0",
  "relay": [
    "b2a70ee0",
    "6984cb10",
    "699c7b30",
    "698550cc",
    "699ae0e0",
    "69851998",
    "69842190",
    "6983e670",
    "6984afa8",
    "a69610d4",
    "b2a77ec4",
    "aca9541c",
    "25a77f70",
    "db51dc18",
    "0400b19c",
    "75e06e58",
    "6984adf0",
    "9e772860",
    "698444e0",
    "9ea1fef8",
    "b29f6fd0",
    "db579a7c",
    "e5e99410",
    "acab7884",
    "48fff55e",
    "02ee4fdc",
    "b2a74e6c",
    "9e762904",
    "699c9f60"
  ],
  "uid": "7eb9fdae-e10d-56bc-8f99-92b9c8e0aeef",
  "text": "🤖 jep1 принял в 20:01 \\nХопов 2 \\nSNR: -4.3 | RSSI: -84",
  "timestamp": "2026-03-04T15:01:46Z",
  "first_seen": "2026-03-04T14:36:09Z",
  "last_seen": "2026-03-04T15:02:17Z",
  "lat": 56.86272,
  "lon": 60.6326784,
  "alt": 282.0,
  "seen_count": 99
}
```

## Notes
- Decryption: only Meshtastic default PSKs for known channels;  
- Cache: events are coalesced for couple of time per UID to merge text/telemetry/relays.  
- DB: table `uids` stores first/last seen and position when provided for UIDS.  
