#!/usr/bin/env bash
# 内存守护：内存/交换快见底时，先杀掉最能吃的自动化进程（这里是 playwright），
# 再重启浏览器服务。"牺牲浏览器保全机器"。
# 安装到 /usr/local/bin/browser-memory-guard.sh，配合 browser-memory-guard.service 使用。
set -uo pipefail

MIN_AVAILABLE_KB="${MIN_AVAILABLE_KB:-350000}"
MIN_SWAP_FREE_KB="${MIN_SWAP_FREE_KB:-131072}"
COOLDOWN_SECONDS="${COOLDOWN_SECONDS:-120}"
LOG_TAG="browser-memory-guard"

last_action=0

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

meminfo_value() {
  awk -v key="$1" '$1 == key ":" { print $2 }' /proc/meminfo
}

# 换成你自己机器上"内存压力时第一个牺牲"的进程
kill_playwright_mcp() {
  pkill -TERM -f '(@playwright/mcp|playwright-mcp)' 2>/dev/null || true
  sleep 3
  pkill -KILL -f '(@playwright/mcp|playwright-mcp)' 2>/dev/null || true
}

snapshot_top_memory() {
  ps -eo pid,ppid,stat,pcpu,pmem,rss,args --sort=-rss | head -18
}

log "$LOG_TAG started min_available_kb=$MIN_AVAILABLE_KB min_swap_free_kb=$MIN_SWAP_FREE_KB"

while true; do
  available_kb="$(meminfo_value MemAvailable)"
  swap_total_kb="$(meminfo_value SwapTotal)"
  swap_free_kb="$(meminfo_value SwapFree)"
  available_kb="${available_kb:-0}"
  swap_total_kb="${swap_total_kb:-0}"
  swap_free_kb="${swap_free_kb:-0}"

  pressure=0
  reasons=()
  if [ "$available_kb" -gt 0 ] && [ "$available_kb" -lt "$MIN_AVAILABLE_KB" ]; then
    pressure=1
    reasons+=("MemAvailable=${available_kb}KB")
  fi
  if [ "$swap_total_kb" -gt 0 ] && [ "$swap_free_kb" -lt "$MIN_SWAP_FREE_KB" ]; then
    pressure=1
    reasons+=("SwapFree=${swap_free_kb}KB")
  fi

  now="$(date +%s)"
  if [ "$pressure" -eq 1 ] && [ $((now - last_action)) -ge "$COOLDOWN_SECONDS" ]; then
    log "memory pressure: ${reasons[*]}; sacrificing browser/playwright"
    snapshot_top_memory
    kill_playwright_mcp
    systemctl restart browser-relay.service || true
    last_action="$now"
  fi

  sleep 15
done
