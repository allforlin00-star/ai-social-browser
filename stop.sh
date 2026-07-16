#!/bin/bash
DIR="$(cd "$(dirname "$0")" && pwd)"
PID_FILE="$DIR/.relay.pid"

if [ -f "$PID_FILE" ]; then
  PID=$(cat "$PID_FILE")
  if kill -0 "$PID" 2>/dev/null; then
    kill "$PID"
    echo "已停止 relay (PID $PID)"
  else
    echo "进程已不存在"
  fi
  rm -f "$PID_FILE"
else
  echo "没有找到运行中的 relay"
fi
