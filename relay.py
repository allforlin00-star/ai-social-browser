#!/usr/bin/env python3
"""
Browser Relay — 把一台机器上 headed Chrome 的画面推给手机网页，手机上的触摸/键盘操作传回去。

Mac 本地和 Linux 服务器（xvfb）都能跑；Playwright 等自动化工具可以通过
CDP 端口共享同一个浏览器实例，人和脚本看到的是同一个现场。
"""

import asyncio
import hashlib
import hmac
import json
import os
import signal
import sys
import subprocess
import urllib.request
from pathlib import Path
from shutil import which

try:
    from websockets.asyncio.server import serve
    from websockets.asyncio.client import connect as ws_connect
    from websockets import Response
    from websockets.datastructures import Headers
except ImportError:
    print("需要安装: pip3 install websockets")
    sys.exit(1)

DEFAULT_PORT = int(os.environ.get("BROWSER_RELAY_PORT", "8271"))
DEFAULT_HOST = os.environ.get("BROWSER_RELAY_HOST", "127.0.0.1")
CDP_PORT = int(os.environ.get("BROWSER_RELAY_CDP_PORT", "9333"))
DEFAULT_WIDTH = 390
DEFAULT_HEIGHT = 844
PROFILE_DIR = Path(os.environ.get("BROWSER_RELAY_PROFILE", str(Path.home() / ".browser-relay-profile"))).expanduser()
SCREENCAST_QUALITY = 85
_PASS = os.environ.get("BROWSER_RELAY_PASS", "")
# 登录凭证：客户端发 sha256(密码)；会话 cookie 是另一个派生值，由服务器以 HttpOnly 下发，
# 页面里的 JS 拿不到，也推不回密码哈希。
AUTH_TOKEN = hashlib.sha256(_PASS.encode()).hexdigest()
COOKIE_TOKEN = hashlib.sha256(f"relay-cookie-v1:{_PASS}".encode()).hexdigest()
COOKIE_NAME = "relay_auth"
COOKIE_MAX_AGE = 30 * 24 * 3600


def find_chrome():
    env_bin = os.environ.get("CHROME_BIN")
    if env_bin and Path(env_bin).exists():
        return env_bin

    normal = Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
    if normal.exists():
        return str(normal)

    for name in ("google-chrome", "google-chrome-stable", "chromium", "chromium-browser"):
        path = which(name)
        if path:
            return path

    pw_cache = Path.home() / "Library/Caches/ms-playwright"
    for d in sorted(pw_cache.glob("chromium-*"), reverse=True):
        app = d / "chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing"
        if app.exists():
            return str(app)
    return None


class BrowserRelay:
    def __init__(self, width=DEFAULT_WIDTH, height=DEFAULT_HEIGHT):
        self.width = width
        self.height = height
        self.clients = set()
        self.chrome_proc = None
        self.cdp_ws = None
        self._msg_id = 0
        self._pending = {}  # msg_id -> Future
        self._running = True
        self._last_frame = None
        self._send_lock = asyncio.Lock()
        self._reader_task = None
        self._cdp_alive = asyncio.Event()
        self._switching = False

    def _next_id(self):
        self._msg_id += 1
        return self._msg_id

    async def _cdp_send(self, method, params=None):
        """Send a CDP message (fire-and-forget)."""
        async with self._send_lock:
            mid = self._next_id()
            msg = {"id": mid, "method": method}
            if params:
                msg["params"] = params
            await self.cdp_ws.send(json.dumps(msg))
            return mid

    async def cdp_call(self, method, params=None):
        """Send CDP command and wait for response."""
        fut = asyncio.get_event_loop().create_future()
        mid = await self._cdp_send(method, params)
        self._pending[mid] = fut
        try:
            return await asyncio.wait_for(fut, timeout=10)
        except asyncio.TimeoutError:
            self._pending.pop(mid, None)
            raise

    async def cdp_fire(self, method, params=None):
        """Fire-and-forget CDP command."""
        await self._cdp_send(method, params)

    async def _cdp_reader(self):
        """Continuously read CDP WebSocket, dispatch responses and events."""
        try:
            async for raw in self.cdp_ws:
                data = json.loads(raw)

                msg_id = data.get("id")
                if msg_id and msg_id in self._pending:
                    fut = self._pending.pop(msg_id)
                    if not fut.done():
                        if "error" in data:
                            fut.set_result(data["error"])
                        else:
                            fut.set_result(data.get("result", {}))
                    continue

                if data.get("method") == "Page.screencastFrame":
                    params = data["params"]
                    await self.cdp_fire("Page.screencastFrameAck", {
                        "sessionId": params.get("sessionId", 0),
                    })
                    frame_data = json.dumps({
                        "type": "frame",
                        "data": params["data"],
                    })
                    self._last_frame = frame_data
                    if self.clients:
                        await asyncio.gather(
                            *[self._safe_send(c, frame_data) for c in self.clients],
                            return_exceptions=True,
                        )
        except Exception as e:
            if self._running:
                print(f"CDP 读取断开: {e}")
        finally:
            self._cdp_alive.clear()
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(ConnectionError("CDP disconnected"))
            self._pending.clear()
            if self._running and not self._switching:
                asyncio.create_task(self._reconnect_loop())

    async def _reconnect_loop(self):
        """CDP 断了以后持续尝试重连。"""
        while self._running:
            await asyncio.sleep(2)
            pages = self._get_pages()
            if pages:
                ws_url = pages[0]["webSocketDebuggerUrl"]
                try:
                    await self._connect_cdp(ws_url)
                    print(f"自动重连成功: {pages[0].get('title', '')}")
                    return
                except Exception as e:
                    print(f"重连失败: {e}")

    async def _connect_cdp(self, ws_url):
        """Connect to a CDP page WebSocket and start reader + screencast."""
        self._switching = True
        try:
            if self.cdp_ws:
                await self.cdp_ws.close()
        except Exception:
            pass

        if self._reader_task and not self._reader_task.done():
            self._reader_task.cancel()
            try:
                await self._reader_task
            except (asyncio.CancelledError, Exception):
                pass

        self.cdp_ws = await ws_connect(ws_url, max_size=50 * 1024 * 1024, ping_interval=None)
        self._switching = False
        self._cdp_alive.set()
        self._reader_task = asyncio.create_task(self._cdp_reader())

        await self.cdp_call("Page.enable")
        await self.cdp_call("Emulation.setDeviceMetricsOverride", {
            "width": self.width, "height": self.height,
            "deviceScaleFactor": 2, "mobile": True,
        })

        await self.cdp_fire("Page.startScreencast", {
            "format": "jpeg",
            "quality": SCREENCAST_QUALITY,
            "maxWidth": self.width * 2,
            "maxHeight": self.height * 2,
            "everyNthFrame": 1,
        })

        result = await self.cdp_call("Page.captureScreenshot", {
            "format": "jpeg", "quality": 60,
        })
        if "data" in result:
            self._last_frame = json.dumps({"type": "frame", "data": result["data"]})

    async def start_browser(self):
        chrome_bin = find_chrome()
        if not chrome_bin:
            print("找不到 Chrome")
            sys.exit(1)

        PROFILE_DIR.mkdir(parents=True, exist_ok=True)

        subprocess.run(["pkill", "-f", f"--user-data-dir={PROFILE_DIR}"], capture_output=True)
        await asyncio.sleep(1)

        args = [
            chrome_bin,
            f"--remote-debugging-port={CDP_PORT}",
            "--remote-debugging-address=127.0.0.1",
            f"--user-data-dir={PROFILE_DIR}",
            f"--window-size={self.width},{self.height}",
            "--disable-blink-features=AutomationControlled",
            "--disable-backgrounding-occluded-windows",
            "--disable-renderer-backgrounding",
            "--disable-background-timer-throttling",
            "--no-first-run",
            "--no-default-browser-check",
            "--noerrdialogs",
            "--disable-session-crashed-bubble",
            "--restore-last-session=false",
            "--profile-directory=Default",
            "about:blank",
        ]
        if sys.platform.startswith("linux"):
            args.insert(-1, "--no-sandbox")
            args.insert(-1, "--disable-dev-shm-usage")

        self.chrome_proc = subprocess.Popen(args)

        if sys.platform == "darwin":
            asyncio.get_event_loop().call_later(2, lambda: subprocess.Popen([
                "osascript", "-e",
                'tell application "Google Chrome" to set miniaturized of front window to true',
            ]))

        page_ws_url = None
        for _ in range(30):
            try:
                resp = urllib.request.urlopen(f"http://127.0.0.1:{CDP_PORT}/json")
                pages = json.loads(resp.read())
                for p in pages:
                    if p.get("type") == "page":
                        page_ws_url = p["webSocketDebuggerUrl"]
                        break
                if page_ws_url:
                    break
            except Exception:
                pass
            await asyncio.sleep(0.3)

        if not page_ws_url:
            print("Chrome 启动超时")
            sys.exit(1)

        await self._connect_cdp(page_ws_url)
        await self.cdp_call("Page.navigate", {"url": "https://www.google.com"})

        print(f"浏览器已启动 viewport={self.width}x{self.height}")

    async def _safe_send(self, ws, data):
        try:
            await ws.send(data)
        except Exception:
            self.clients.discard(ws)

    async def handle_client(self, ws):
        self.clients.add(ws)
        remote = ws.remote_address
        print(f"客户端连接: {remote}")

        await ws.send(json.dumps({
            "type": "init",
            "width": self.width,
            "height": self.height,
        }))

        if self._last_frame:
            await self._safe_send(ws, self._last_frame)

        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    reply = await self._handle_input(msg)
                    if reply:
                        await self._safe_send(ws, reply)
                except json.JSONDecodeError:
                    pass
        except Exception:
            pass
        finally:
            self.clients.discard(ws)
            print(f"客户端断开: {remote}")

    def _get_pages(self):
        try:
            resp = urllib.request.urlopen(f"http://127.0.0.1:{CDP_PORT}/json")
            return [p for p in json.loads(resp.read()) if p.get("type") == "page"]
        except Exception:
            return []

    async def _handle_input(self, msg):
        t = msg.get("type")

        if t == "list_tabs":
            pages = self._get_pages()
            tabs = [{"id": p["id"], "title": p.get("title", ""), "url": p.get("url", "")} for p in pages]
            return json.dumps({"type": "tabs", "tabs": tabs})

        if not self._cdp_alive.is_set():
            return

        if t == "switch_tab":
            target_id = msg.get("id", "")
            pages = self._get_pages()
            for p in pages:
                if p["id"] == target_id:
                    await self._connect_cdp(p["webSocketDebuggerUrl"])
                    print(f"切换到: {p.get('title', '')}")
                    break

        elif t == "tap":
            x, y = msg["x"], msg["y"]
            await self.cdp_fire("Input.dispatchMouseEvent", {
                "type": "mousePressed", "x": x, "y": y, "button": "left", "clickCount": 1,
            })
            await asyncio.sleep(0.02)
            await self.cdp_fire("Input.dispatchMouseEvent", {
                "type": "mouseReleased", "x": x, "y": y, "button": "left", "clickCount": 1,
            })

        elif t == "scroll":
            x, y = msg.get("x", self.width // 2), msg.get("y", self.height // 2)
            dx, dy = msg.get("deltaX", 0), msg.get("deltaY", 0)
            await self.cdp_fire("Input.dispatchMouseEvent", {
                "type": "mouseWheel", "x": x, "y": y, "deltaX": dx, "deltaY": dy,
            })

        elif t == "keypress":
            key = msg.get("key", "")
            await self.cdp_fire("Input.dispatchKeyEvent", {"type": "keyDown", "key": key})
            await self.cdp_fire("Input.dispatchKeyEvent", {"type": "keyUp", "key": key})

        elif t == "type":
            for char in msg.get("text", ""):
                await self.cdp_fire("Input.dispatchKeyEvent", {"type": "char", "text": char})

        elif t == "navigate":
            url = msg.get("url", "")
            if url:
                await self.cdp_fire("Page.navigate", {"url": url})

        elif t == "resize":
            w, h = msg.get("width", self.width), msg.get("height", self.height)
            self.width, self.height = w, h
            await self.cdp_call("Emulation.setDeviceMetricsOverride", {
                "width": w, "height": h, "deviceScaleFactor": 2, "mobile": True,
            })

        elif t == "back":
            await self.cdp_fire("Runtime.evaluate", {"expression": "history.back()"})

        elif t == "forward":
            await self.cdp_fire("Runtime.evaluate", {"expression": "history.forward()"})

        elif t == "refresh":
            await self.cdp_fire("Page.reload")

    async def stop(self):
        self._running = False
        if self._reader_task and not self._reader_task.done():
            self._reader_task.cancel()
        if self.cdp_ws:
            await self.cdp_ws.close()
        if self.chrome_proc:
            self.chrome_proc.terminate()
            self.chrome_proc.wait()
        print("浏览器已关闭")


CLIENT_HTML = Path(__file__).parent / "client.html"
LOGIN_HTML = Path(__file__).parent / "login.html"


def _safe_equal(a, b):
    try:
        return hmac.compare_digest(a, b)
    except TypeError:
        return False


def _check_cookie(request):
    cookie_header = request.headers.get("Cookie", "")
    for part in cookie_header.split(";"):
        part = part.strip()
        if part.startswith(f"{COOKIE_NAME}="):
            return _safe_equal(part.split("=", 1)[1], COOKIE_TOKEN)
    return False


def process_request(connection, request):
    if request.headers.get("Upgrade", "").lower() == "websocket":
        if not _check_cookie(request):
            return Response(403, "Forbidden", Headers(), b"unauthorized")
        return

    auth_header = request.headers.get("X-Relay-Auth")
    if auth_header is not None:
        if _safe_equal(auth_header, AUTH_TOKEN):
            headers = Headers([(
                "Set-Cookie",
                f"{COOKIE_NAME}={COOKIE_TOKEN}; Path=/; Max-Age={COOKIE_MAX_AGE}; HttpOnly; SameSite=Strict",
            )])
            return Response(204, "No Content", headers, b"")
        return Response(403, "Forbidden", Headers(), b"wrong password")

    if not _check_cookie(request):
        try:
            html = LOGIN_HTML.read_bytes()
            return Response(200, "OK", Headers([("Content-Type", "text/html; charset=utf-8")]), html)
        except FileNotFoundError:
            return Response(500, "Error", Headers(), b"login.html not found")

    try:
        html = CLIENT_HTML.read_bytes()
        return Response(200, "OK", Headers([("Content-Type", "text/html; charset=utf-8")]), html)
    except FileNotFoundError:
        return Response(404, "Not Found", Headers(), b"client.html not found")


async def main():
    import argparse
    parser = argparse.ArgumentParser(description="Browser Relay")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--width", type=int, default=DEFAULT_WIDTH)
    parser.add_argument("--height", type=int, default=DEFAULT_HEIGHT)
    args = parser.parse_args()

    if not _PASS:
        print("警告: 未设置 BROWSER_RELAY_PASS，空密码即可登录 — 只可用于本机调试，绝不要这样暴露到公网")

    relay = BrowserRelay(width=args.width, height=args.height)
    await relay.start_browser()

    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    async with serve(
        relay.handle_client, args.host, args.port,
        process_request=process_request,
    ):
        print(f"服务已启动: http://{args.host}:{args.port}")
        await stop_event.wait()

    await relay.stop()


if __name__ == "__main__":
    asyncio.run(main())
