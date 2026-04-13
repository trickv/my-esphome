#!/bin/bash
# Capture ESPHome logs from lightning2 to a daily log file.
# Run in background: ./capture_logs.sh &

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"

LOGFILE="$LOG_DIR/lightning2-$(date +%Y-%m-%d).log"

echo "Capturing lightning2 logs to $LOGFILE"

docker run --rm --net=host \
  -v "$SCRIPT_DIR":/config \
  esphome/esphome:stable logs lightning2.yaml \
  2>&1 | while IFS= read -r line; do
    echo "$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ) $line" >> "$LOGFILE"
done
