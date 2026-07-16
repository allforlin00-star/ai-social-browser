#!/bin/bash
# 启动 browser-relay：Chrome + WebSocket 中继
# 用法: ./start.sh [--port 9222] [--width 390] [--height 844]
# 停止: Ctrl+C 或 ./stop.sh

DIR="$(cd "$(dirname "$0")" && pwd)"
PID_FILE="$DIR/.relay.pid"

if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "relay 已经在运行 (PID $(cat "$PID_FILE"))，先 ./stop.sh 再启动"
  exit 1
fi

echo "启动 browser-relay..."
python3 "$DIR/relay.py" "$@" &
echo $! > "$PID_FILE"
echo "PID: $(cat "$PID_FILE")"
echo "按 Ctrl+C 停止，或运行 ./stop.sh"
wait
rm -f "$PID_FILE"
