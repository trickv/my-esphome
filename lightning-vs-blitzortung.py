#!/usr/bin/env python3
"""Compare AS3935 lightning sensor detections against Blitzortung network data.

Runs two concurrent tasks:
1. Tails a lightning2 ESPHome log file for AS3935 events
2. Streams real-time Blitzortung strikes via WebSocket

Both streams are logged to a SQLite database and correlated to produce a
report showing what the sensor detected, missed, or misclassified.
"""

import asyncio
import json
import math
import os
import re
import signal
import sqlite3
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    import websockets
except ImportError:
    sys.exit("Missing dependency: pip install websockets")

# ---------------------------------------------------------------------------
# Configuration — override via environment variables or edit here
# ---------------------------------------------------------------------------
LAT = float(os.environ.get("LAT", "0.0"))  # your latitude
LON = float(os.environ.get("LON", "0.0"))  # your longitude
RADIUS_KM = float(os.environ.get("RADIUS_KM", "50"))
LOG_FILE = os.environ.get("LOG_FILE", "logs/lightning2.log")
DB_FILE = os.environ.get("DB_FILE", "lightning_data.db")
BLITZORTUNG_WS = "wss://ws1.blitzortung.org/"
# Correlation window: how close in time (seconds) a Blitzortung strike and
# sensor event must be to count as "matched".
CORRELATION_WINDOW_S = float(os.environ.get("CORRELATION_WINDOW_S", "2.0"))

# ---------------------------------------------------------------------------
# Geo helpers
# ---------------------------------------------------------------------------

def bounding_box(lat, lon, radius_km):
    R = 6371.0
    dlat = math.degrees(radius_km / R)
    dlon = math.degrees(radius_km / (R * math.cos(math.radians(lat))))
    return {
        "south": round(lat - dlat, 2),
        "north": round(lat + dlat, 2),
        "west": round(lon - dlon, 2),
        "east": round(lon + dlon, 2),
    }


def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1))
         * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def decode_blitzortung(b):
    """Decode Blitzortung's LZW-style compressed WebSocket payload."""
    e = {}
    d = list(b)
    c = d[0]
    f = c
    g = [c]
    l = 256
    h = l
    for a in range(1, len(d)):
        k = ord(d[a])
        if h > k:
            k = d[a]
        elif k in e:
            k = e[k]
        else:
            k = f + c
        g.append(k)
        c = k[0]
        e[l] = f + c
        l += 1
        f = k
    return ''.join(g)

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def init_db(db_path):
    db = sqlite3.connect(db_path)
    db.execute("""
        CREATE TABLE IF NOT EXISTS blitzortung_strikes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            distance_km REAL NOT NULL,
            stations INTEGER
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS sensor_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            event_type TEXT NOT NULL,
            distance_km REAL,
            energy INTEGER,
            raw_line TEXT
        )
    """)
    db.commit()
    return db

# ---------------------------------------------------------------------------
# ESPHome log parser
# ---------------------------------------------------------------------------

# Log lines look like:
#   2026-04-03T02:08:16.597Z [02:08:16.597][I][as3935:052]: Disturber was detected ...
#   2026-04-03T02:08:16.597Z [02:08:16.597][I][as3935:054]: Lightning has been detected!
#   2026-04-03T02:08:16.597Z [02:08:16.597][I][as3935:050]: Noise was detected ...
# The capture_logs.sh script prepends a UTC ISO timestamp.

# Match the prepended UTC timestamp
LOG_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z)")
# Match AS3935 events from the ESPHome log portion
DISTURBER_RE = re.compile(r"\[as3935:\d+\]: Disturber was detected")
LIGHTNING_RE = re.compile(r"\[as3935:\d+\]: Lightning has been detected")
NOISE_RE = re.compile(r"\[as3935:\d+\]: Noise was detected")
# Sensor state publications (distance and energy are logged by ESPHome sensor component)
DISTANCE_RE = re.compile(r"Storm Distance.*?(\d+(?:\.\d+)?)")
ENERGY_RE = re.compile(r"Lightning Energy.*?(\d+(?:\.\d+)?)")


def parse_log_line(line):
    """Parse an ESPHome log line. Returns (timestamp_str, event_type, extras) or None."""
    ts_match = LOG_TS_RE.match(line)
    if not ts_match:
        return None
    ts_str = ts_match.group(1)

    event_type = None
    if DISTURBER_RE.search(line):
        event_type = "disturber"
    elif LIGHTNING_RE.search(line):
        event_type = "lightning"
    elif NOISE_RE.search(line):
        event_type = "noise"

    if event_type is None:
        return None

    return (ts_str, event_type, line.strip())

# ---------------------------------------------------------------------------
# Log file tailer
# ---------------------------------------------------------------------------

async def tail_log(log_path, db, stop_event):
    """Tail the ESPHome log file and record AS3935 events."""
    path = Path(log_path)

    # Wait for the file to exist
    warned = False
    while not path.exists():
        if stop_event.is_set():
            return
        if not warned:
            print(f"Waiting for log file: {log_path}")
            warned = True
        await asyncio.sleep(2)
    if warned:
        print(f"Log file appeared: {log_path}")

    last_distance = None
    last_energy = None

    with open(path, "r") as f:
        # Start from end of file (only process new lines)
        f.seek(0, 2)
        while not stop_event.is_set():
            line = f.readline()
            if not line:
                await asyncio.sleep(0.1)
                continue

            # Track distance/energy values that follow a lightning detection
            d_match = DISTANCE_RE.search(line)
            if d_match:
                last_distance = float(d_match.group(1))
                continue
            e_match = ENERGY_RE.search(line)
            if e_match:
                last_energy = int(float(e_match.group(1)))
                continue

            parsed = parse_log_line(line)
            if parsed is None:
                continue

            ts_str, event_type, raw = parsed
            distance = last_distance if event_type == "lightning" else None
            energy = last_energy if event_type == "lightning" else None
            if event_type == "lightning":
                last_distance = None
                last_energy = None

            db.execute(
                "INSERT INTO sensor_events (timestamp, event_type, distance_km, energy, raw_line) VALUES (?, ?, ?, ?, ?)",
                (ts_str, event_type, distance, energy, raw),
            )
            db.commit()
            print(f"[SENSOR] {ts_str} {event_type}"
                  + (f" dist={distance}km energy={energy}" if event_type == "lightning" else ""))

# ---------------------------------------------------------------------------
# Blitzortung stream
# ---------------------------------------------------------------------------

async def stream_blitzortung(db, stop_event):
    """Connect to Blitzortung WebSocket and record nearby strikes.

    Uses subscription {"a": 111} for global strikes, filters client-side by
    bounding box + haversine. Payloads are LZW-encoded and must be decoded.
    """
    print(f"Blitzortung: subscribed globally, filtering to {RADIUS_KM}km radius")

    connect_count = 0
    total_msgs = 0
    while not stop_event.is_set():
        try:
            async with websockets.connect(BLITZORTUNG_WS) as ws:
                await ws.send(json.dumps({"a": 111}))
                connect_count += 1
                if connect_count == 1:
                    print("Blitzortung: connected, waiting for strikes...")
                elif connect_count % 50 == 0:
                    print(f"Blitzortung: reconnected ({connect_count} times, {total_msgs} messages received)")

                if connect_count == 5 and total_msgs == 0:
                    print("WARNING: Blitzortung connected 5x but delivered no messages.")

                async for message in ws:
                    if stop_event.is_set():
                        return
                    total_msgs += 1
                    try:
                        decoded = decode_blitzortung(message)
                        strike = json.loads(decoded)
                    except (ValueError, json.JSONDecodeError):
                        continue
                    if "lat" not in strike:
                        continue

                    dist = haversine(LAT, LON, strike["lat"], strike["lon"])
                    if dist > RADIUS_KM:
                        continue

                    ts = datetime.fromtimestamp(
                        strike["time"] / 1e9, tz=timezone.utc
                    )
                    ts_str = ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                    sig = strike.get("sig", [])
                    stations = len(sig) if isinstance(sig, list) else sig

                    db.execute(
                        "INSERT INTO blitzortung_strikes (timestamp, lat, lon, distance_km, stations) VALUES (?, ?, ?, ?, ?)",
                        (ts_str, strike["lat"], strike["lon"], round(dist, 1), stations),
                    )
                    db.commit()
                    print(f"[BLITZ]  {ts_str} lat={strike['lat']:.3f} lon={strike['lon']:.3f} dist={dist:.1f}km stations={stations}")

        except (websockets.exceptions.ConnectionClosed, OSError) as e:
            if stop_event.is_set():
                return
            print(f"Blitzortung: connection lost ({e}), reconnecting in 5s...")
            await asyncio.sleep(5)

# ---------------------------------------------------------------------------
# Correlation / report
# ---------------------------------------------------------------------------

def generate_report(db):
    """Correlate Blitzortung strikes with sensor events and print a summary."""
    strikes = db.execute(
        "SELECT timestamp, distance_km FROM blitzortung_strikes ORDER BY timestamp"
    ).fetchall()
    events = db.execute(
        "SELECT timestamp, event_type FROM sensor_events ORDER BY timestamp"
    ).fetchall()

    if not strikes and not events:
        print("\nNo data recorded yet.")
        return

    def parse_ts(s):
        # Handle variable fractional seconds
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except ValueError:
            return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)

    strike_times = [(parse_ts(s[0]), s[1]) for s in strikes]
    event_list = [(parse_ts(e[0]), e[1]) for e in events]
    lightning_events = [e for e in event_list if e[1] == "lightning"]
    disturber_events = [e for e in event_list if e[1] == "disturber"]
    noise_events = [e for e in event_list if e[1] == "noise"]

    window = timedelta(seconds=CORRELATION_WINDOW_S)

    matched = 0
    misclassified = 0
    missed = 0

    for st_time, st_dist in strike_times:
        # Check if any lightning detection is within the time window
        has_lightning = any(abs(et - st_time) <= window for et, _ in lightning_events)
        has_disturber = any(abs(et - st_time) <= window for et, _ in disturber_events)

        if has_lightning:
            matched += 1
        elif has_disturber:
            misclassified += 1
        else:
            missed += 1

    # Disturbers not near any Blitzortung strike
    false_disturbers = 0
    for dt, _ in disturber_events:
        near_strike = any(abs(dt - st) <= window for st, _ in strike_times)
        if not near_strike:
            false_disturbers += 1

    # Time range
    all_times = [t for t, _ in strike_times] + [t for t, _ in event_list]
    if all_times:
        t_min = min(all_times).strftime("%Y-%m-%d %H:%M UTC")
        t_max = max(all_times).strftime("%Y-%m-%d %H:%M UTC")
    else:
        t_min = t_max = "N/A"

    print("\n" + "=" * 60)
    print(f"  Storm session: {t_min} - {t_max}")
    print(f"  Blitzortung strikes within {RADIUS_KM}km: {len(strikes)}")
    print(f"  Sensor lightning detections: {len(lightning_events)}")
    print(f"  Sensor disturber events: {len(disturber_events)}")
    print(f"  Sensor noise events: {len(noise_events)}")
    print(f"  ---")
    print(f"  Matched (strike + detection):    {matched}")
    print(f"  Misclassified (strike + disturber): {misclassified}")
    print(f"  Missed (strike, no sensor event): {missed}")
    print(f"  False disturbers (no strike):    {false_disturbers}")
    print("=" * 60)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    if LAT == 0.0 and LON == 0.0:
        sys.exit("Set LAT and LON environment variables to your location.")

    print(f"Location: {LAT}, {LON} | Radius: {RADIUS_KM}km")
    print(f"Log file: {LOG_FILE}")
    print(f"Database: {DB_FILE}")
    print()

    db = init_db(DB_FILE)
    stop_event = asyncio.Event()

    def handle_signal():
        print("\nShutting down...")
        stop_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)

    tasks = [
        asyncio.create_task(tail_log(LOG_FILE, db, stop_event)),
        asyncio.create_task(stream_blitzortung(db, stop_event)),
    ]

    # Wait until stopped
    await stop_event.wait()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    # Final report
    generate_report(db)
    db.close()


if __name__ == "__main__":
    asyncio.run(main())
