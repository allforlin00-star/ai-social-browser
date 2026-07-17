"""twitter-tool: 把刷推特包装成几个具体动作的 HTTP 服务。

连接 browser-relay 的常驻 Chrome (CDP 9333)，复用已登录的默认 context。
调用方：任何会发 HTTP 的 agent 或脚本。

POST /twitter  body: {"action": "...", ...}
  feed    [query?, n?, latest?]  不带 query=首页时间线，带 query=搜索
  profile [user, n?]             某人最近推文
  read    [url, n?]              单条推文 + 评论
  like    [url] / unlike [url]
  repost  [url]
  reply   [url, text]
  post    [text]

并发：读写共享总上限 2（标签页池），写操作另有全局串行锁 + 频率闸。
所有写操作记录到 actions.log。
"""

import asyncio
import json
import os
import re
import time
from collections import deque
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import quote

from fastapi import FastAPI, Request
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

CDP_URL = os.environ.get("TW_CDP_URL", "http://127.0.0.1:9333").rstrip("/")
BASE = "https://x.com"
LOG_PATH = Path(__file__).parent / "actions.log"
STATE_PATH = Path(os.environ.get(
    "TW_STATE_PATH", str(Path(__file__).parent / "runtime-state.json")))

# 频率闸：滑动窗口 1 小时
RATE_LIMITS = {
    "publish": 5,    # reply + post 合计
    "engage": 20,    # like + unlike + repost 合计
}
RATE_GROUP = {"reply": "publish", "post": "publish",
              "like": "engage", "unlike": "engage", "repost": "engage"}
_rate_windows = {g: deque() for g in RATE_LIMITS}  # epoch seconds，需跨重启保留

READ_SEM = asyncio.Semaphore(2)  # 读操作自身上限；读写还会共同占用 PAGE_CAPACITY
WRITE_LOCK = asyncio.Lock()
PAGE_CAPACITY = asyncio.Semaphore(2)  # 读写共用：工具自己的 x.com 工作页总数最多 2

_pw = None
_browser = None
_conn_lock = asyncio.Lock()
_owned_targets = {}  # target_id -> {created_at,last_active_at}，落盘后可清理重启遗孤
_owned_pages = {}    # id(page) -> {page,target_id,closing}
_cleanup_tasks = set()
_state_loaded = False

app = FastAPI()


def _now_cn():
    return datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")


def log_action(entry: dict):
    entry["ts"] = _now_cn()
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _persist_state():
    """频率窗口和工具自建 target 一起原子落盘；绝不记录/接管用户手动标签页。"""
    payload = {
        "rate_windows": {group: list(win) for group, win in _rate_windows.items()},
        "owned_targets": _owned_targets,
    }
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_name(f".{STATE_PATH.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, STATE_PATH)


def _load_state():
    global _state_loaded
    if _state_loaded:
        return
    _state_loaded = True
    try:
        payload = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return
    except Exception as e:
        log_action({"action": "_state_load_failed", "error": str(e)})
        return

    now = time.time()
    raw_windows = payload.get("rate_windows") or {}
    for group in RATE_LIMITS:
        values = raw_windows.get(group) or []
        _rate_windows[group] = deque(
            float(ts) for ts in values
            if isinstance(ts, (int, float)) and 0 <= now - float(ts) <= 3600
        )
    raw_targets = payload.get("owned_targets") or {}
    if isinstance(raw_targets, dict):
        for target_id, meta in raw_targets.items():
            if isinstance(target_id, str) and isinstance(meta, dict):
                _owned_targets[target_id] = {
                    "created_at": float(meta.get("created_at") or now),
                    "last_active_at": float(meta.get("last_active_at") or now),
                }


def check_rate(action: str):
    group = RATE_GROUP.get(action)
    if not group:
        return None
    win = _rate_windows[group]
    now = time.time()
    changed = False
    while win and now - win[0] > 3600:
        win.popleft()
        changed = True
    if changed:
        _persist_state()
    if len(win) >= RATE_LIMITS[group]:
        wait_min = int((3600 - (now - win[0])) / 60) + 1
        return f"频率闸：{group} 组最近 1 小时已用满 {RATE_LIMITS[group]} 次，约 {wait_min} 分钟后再试"
    return None


def record_rate(action: str):
    group = RATE_GROUP.get(action)
    if group:
        _rate_windows[group].append(time.time())
        _persist_state()


def _close_owned_targets_sync(target_ids):
    """通过 CDP HTTP 只关明确登记为 twitter-tool 所有的 target。"""
    import urllib.request
    report = {"closed": [], "missing": [], "errors": []}
    wanted = set(target_ids)
    if not wanted:
        return report
    try:
        targets = json.load(urllib.request.urlopen(
            f"{CDP_URL}/json", timeout=5))
        pages = {t.get("id"): t for t in targets if t.get("type") == "page"}
        for target_id in wanted:
            if target_id not in pages:
                report["missing"].append(target_id)
                continue
            try:
                urllib.request.urlopen(
                    f"{CDP_URL}/json/close/{target_id}", timeout=5)
                report["closed"].append(target_id)
            except Exception as e:
                report["errors"].append({"target_id": target_id, "error": str(e)})
    except Exception as e:
        report["errors"].append({"error": str(e)})
    return report


async def _sweep_owned_orphans():
    """清理工具自建且静默超过 30 分钟的遗孤；不按 URL 猜归属。"""
    cutoff = time.time() - max(30.0, float(os.environ.get("TW_ORPHAN_MINUTES", "30"))) * 60
    stale = [
        target_id for target_id, meta in list(_owned_targets.items())
        if float(meta.get("last_active_at") or 0) <= cutoff
    ]
    if not stale:
        return []
    result = await asyncio.get_running_loop().run_in_executor(
        None, _close_owned_targets_sync, stale)
    resolved = set(result.get("closed", [])) | set(result.get("missing", []))
    for target_id in resolved:
        _owned_targets.pop(target_id, None)
    _persist_state()
    return result


async def get_context():
    """连接（或重连）常驻 Chrome，返回带登录态的默认 context。

    连接卡死（通常是僵尸标签页拖住接管）时自动清僵尸重连一次。
    """
    global _pw, _browser
    async with _conn_lock:
        if _browser is not None and _browser.is_connected():
            return _browser.contexts[0]
        for attempt in (1, 2):
            try:
                if _pw is None:
                    _pw = await async_playwright().start()
                _browser = await asyncio.wait_for(
                    _pw.chromium.connect_over_cdp(CDP_URL), timeout=12)
                if not _browser.contexts:
                    raise RuntimeError("CDP 连上了但没有 browser context")
                return _browser.contexts[0]
            except Exception:
                _browser = None
                if _pw is not None:
                    try:
                        await asyncio.wait_for(_pw.stop(), timeout=5)
                    except Exception:
                        pass
                    _pw = None
                if attempt == 2:
                    raise RuntimeError("连中继浏览器失败（清了僵尸标签页重试仍不行）")
                swept = await _sweep_owned_orphans()
                log_action({"action": "_selfheal", "swept": swept})


_PAGE_LOCK = asyncio.Lock()  # target 创建与 Page 精确匹配必须串行，避免并发扫描互相干扰


async def _page_target_id(ctx, page):
    """读取 Page 自己的 CDP target id；不能靠“下一个新页事件”猜归属。"""
    session = await ctx.new_cdp_session(page)
    try:
        info = await session.send("Target.getTargetInfo")
        return (info.get("targetInfo") or {}).get("targetId")
    finally:
        await session.detach()


async def _find_page_by_target_id(ctx, target_id, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for candidate in reversed(ctx.pages):
            try:
                if await _page_target_id(ctx, candidate) == target_id:
                    return candidate
            except Exception:
                continue
        await asyncio.sleep(0.05)
    raise RuntimeError(f"创建 target {target_id} 后找不到对应 Page，拒绝接管其他标签页")


async def new_page(ctx, block_images=True):
    """开后台标签页干活，尽量不抢中继浏览器当前焦点。

    block_images=False 用于读单条推文详情：详情页图片请求被掐会导致
    img 元素不渲染、抓不到图片链接，所以看图的动作放行图片加载。"""
    await PAGE_CAPACITY.acquire()
    page = None
    target_id = None
    try:
        async with _PAGE_LOCK:
            cdp = await _browser.new_browser_cdp_session()
            try:
                created = await cdp.send(
                    "Target.createTarget",
                    {"url": "about:blank", "background": True})
                target_id = created.get("targetId")
            finally:
                await cdp.detach()

            if not target_id:
                raise RuntimeError("创建工作页成功但拿不到 target id，拒绝接管不明标签页")
            page = await _find_page_by_target_id(ctx, target_id)

        now = time.time()
        _owned_targets[target_id] = {"created_at": now, "last_active_at": now}
        _owned_pages[id(page)] = {
            "page": page, "target_id": target_id, "closing": False,
        }
        _persist_state()

        await page.set_viewport_size({"width": 1100, "height": 1300})
        page.set_default_timeout(15000)

        # 不真的下载图片/视频/字体——图片链接从 DOM 属性抓，省内存带宽
        blocked = ("image", "media", "font") if block_images else ("media", "font")

        async def _block_heavy(route):
            if route.request.resource_type in blocked:
                await route.abort()
            else:
                await route.continue_()

        await page.route("**/*", _block_heavy)
        return page
    except BaseException:
        if page is not None and id(page) in _owned_pages:
            try:
                await _close_owned_page(page)
            except asyncio.CancelledError:
                raise
        else:
            if target_id:
                fallback = await asyncio.get_running_loop().run_in_executor(
                    None, _close_owned_targets_sync, [target_id])
                resolved = target_id in fallback.get("closed", []) or target_id in fallback.get("missing", [])
                if resolved:
                    _owned_targets.pop(target_id, None)
                else:
                    now = time.time()
                    _owned_targets[target_id] = {
                        "created_at": now, "last_active_at": now,
                    }
                _persist_state()
            PAGE_CAPACITY.release()
        raise


def _touch_owned_page(page):
    rec = _owned_pages.get(id(page))
    if not rec:
        return
    target_id = rec.get("target_id")
    if target_id in _owned_targets:
        _owned_targets[target_id]["last_active_at"] = time.time()
        _persist_state()


async def _finish_owned_page_close(page, rec):
    target_id = rec.get("target_id")
    cleanup_error = None
    resolved = False
    try:
        await asyncio.wait_for(page.close(), timeout=10)
        resolved = True
    except Exception as e:
        cleanup_error = f"page.close: {type(e).__name__}: {e}"
        if target_id:
            fallback = await asyncio.get_running_loop().run_in_executor(
                None, _close_owned_targets_sync, [target_id])
            cleanup_error += f"; cdp_fallback={fallback}"
            resolved = (target_id in fallback.get("closed", [])
                        or target_id in fallback.get("missing", []))
    finally:
        _owned_pages.pop(id(page), None)
        if target_id and resolved:
            _owned_targets.pop(target_id, None)
        _persist_state()
        PAGE_CAPACITY.release()
        if cleanup_error:
            log_action({
                "action": "_page_cleanup_failed",
                "target_id": target_id,
                "error": cleanup_error,
            })


async def _close_owned_page(page):
    """关闭失败只单独记日志，不允许覆盖动作本身的成功/失败结论。"""
    rec = _owned_pages.get(id(page))
    if not rec or rec.get("closing"):
        return
    rec["closing"] = True
    task = asyncio.create_task(_finish_owned_page_close(page, rec))
    _cleanup_tasks.add(task)
    task.add_done_callback(_cleanup_tasks.discard)
    try:
        await asyncio.shield(task)
    except asyncio.CancelledError:
        # 外层 150s 超时可以返回；shield 内的 close 仍会跑完并释放容量。
        raise


# ---------- 页面解析 ----------

EXTRACT_JS = r"""
() => {
  const out = [];
  for (const art of document.querySelectorAll('article[data-testid="tweet"]')) {
    const t = {};
    const timeEl = art.querySelector('time');
    const link = timeEl ? timeEl.closest('a') : null;
    t.url = link ? link.href : null;
    t.time = timeEl ? timeEl.getAttribute('datetime') : null;
    if (!timeEl) t.promoted = true;  // 广告卡片没有时间戳/永久链接
    const nameEl = art.querySelector('div[data-testid="User-Name"]');
    if (nameEl) {
      const lines = nameEl.innerText.split('\n').map(s => s.trim()).filter(Boolean);
      t.author = lines[0] || null;
      t.handle = lines.find(s => s.startsWith('@')) || null;
    }
    const textEl = art.querySelector('div[data-testid="tweetText"]');
    t.text = textEl ? textEl.innerText : '';
    if (art.querySelector('[data-testid="tweet-text-show-more-link"]'))
      t.truncated = true;
    const social = art.querySelector('[data-testid="socialContext"]');
    if (social) t.repost_context = social.innerText;
    const quotedTexts = art.querySelectorAll('div[data-testid="tweetText"]');
    if (quotedTexts.length > 1) t.quoted_text = quotedTexts[1].innerText;
    const stats = {};
    for (const [key, tid] of [['replies','reply'],['reposts','retweet'],['likes','like'],['likes','unlike']]) {
      const btn = art.querySelector(`button[data-testid="${tid}"]`);
      if (btn) {
        const label = btn.getAttribute('aria-label') || '';
        const m = label.match(/[\d,.]+\s*[万KM]?/);
        stats[key] = m ? m[0].trim() : (btn.innerText.trim() || '0');
      }
    }
    t.stats = stats;
    t.liked_by_me = !!art.querySelector('button[data-testid="unlike"]');
    const imgs = [...art.querySelectorAll('img[src*="pbs.twimg.com/media"]')]
      .map(i => i.src.replace(/&name=\w+$/, ''));
    if (imgs.length) t.images = imgs;
    if (art.querySelector('[data-testid="videoPlayer"]')) t.has_video = true;
    if (art.querySelector('[data-testid="placementTracking"]')) t.promoted = true;
    out.push(t);
  }
  return out;
}
"""


async def check_logged_in(page):
    if "/login" in page.url or "/i/flow/login" in page.url:
        return False
    if await page.query_selector('[data-testid="loginButton"]'):
        return False
    return True


async def wait_tweets(page):
    """等推文出现；区分「没登录」「空结果」「真超时」。"""
    try:
        await page.wait_for_selector('article[data-testid="tweet"]', timeout=15000)
        return None
    except PWTimeout:
        if not await check_logged_in(page):
            return "登录态失效：中继浏览器里推特掉登录了，需要重新登录"
        if await page.query_selector('[data-testid="empty_state_header_text"]'):
            return "EMPTY"
        if await page.query_selector('[data-testid="error-detail"]'):
            return "EMPTY"
        return "等推文加载超时（页面开了但没刷出内容，可能是网络慢或页面结构变了）"


async def collect_tweets(page, n):
    seen, result = set(), []
    for _ in range(10):
        batch = await page.evaluate(EXTRACT_JS)
        for t in batch:
            key = t.get("url") or t.get("text", "")[:80]
            if key and key not in seen:
                seen.add(key)
                result.append(t)
        if len(result) >= n:
            break
        await page.mouse.wheel(0, 2600)
        await page.wait_for_timeout(1200)
    return result[:n]


async def goto(page, url):
    _touch_owned_page(page)
    await page.goto(url, wait_until="domcontentloaded", timeout=25000)
    _touch_owned_page(page)


# ---------- 读动作 ----------

async def act_feed(ctx, body):
    n = min(int(body.get("n", 10)), 30)
    query = (body.get("query") or "").strip()
    page = await new_page(ctx)
    try:
        if query:
            url = f"{BASE}/search?q={quote(query)}&src=typed_query"
            if body.get("latest"):
                url += "&f=live"
        else:
            url = f"{BASE}/home"
        await goto(page, url)
        err = await wait_tweets(page)
        if err == "EMPTY":
            return {"ok": True, "tweets": [], "note": "搜索无结果"}
        if err:
            return {"ok": False, "error": err}
        tweets = await collect_tweets(page, n)
        return {"ok": True, "source": "search" if query else "home_timeline",
                "tweets": tweets}
    finally:
        await _close_owned_page(page)


async def act_profile(ctx, body):
    user = (body.get("user") or "").strip().lstrip("@")
    if not user:
        # 不指定 user 时回退到配置的默认主页（TW_DEFAULT_USER）
        user = os.environ.get("TW_DEFAULT_USER", "").strip().lstrip("@")
    if not user:
        return {"ok": False, "error":
                "缺参数 user，且默认主页还没配置（TW_DEFAULT_USER）"}
    n = min(int(body.get("n", 10)), 30)
    page = await new_page(ctx)
    try:
        await goto(page, f"{BASE}/{quote(user)}")
        if await page.query_selector('[data-testid="emptyState"]'):
            return {"ok": False, "error": f"用户 @{user} 不存在或无内容"}
        err = await wait_tweets(page)
        if err == "EMPTY" or err is None:
            tweets = await collect_tweets(page, n) if err is None else []
            return {"ok": True, "user": user, "tweets": tweets}
        return {"ok": False, "error": err}
    finally:
        await _close_owned_page(page)


def _status_id(url):
    m = re.search(r"/status/(\d+)", url or "")
    return m.group(1) if m else None


async def act_read(ctx, body):
    url = (body.get("url") or "").strip()
    sid = _status_id(url)
    if not sid:
        return {"ok": False, "error": "url 不像推文链接，要形如 https://x.com/xxx/status/123..."}
    n = min(int(body.get("n", 15)), 40)
    page = await new_page(ctx, block_images=False)
    try:
        # 统一走 /i/status/<id>：twitter.com/vxtwitter/带?s=尾巴的分享链接都能开
        await goto(page, f"{BASE}/i/status/{sid}")
        err = await wait_tweets(page)
        if err:
            return {"ok": False, "error": "推文不存在或已删除" if err == "EMPTY" else err}
        tweets = await collect_tweets(page, n + 5)
        main, above, replies = None, [], []
        for t in tweets:
            if main is None and _status_id(t.get("url")) == sid:
                main = t
            elif main is None:
                above.append(t)
            else:
                replies.append(t)
        if main is None and tweets:
            main, replies = tweets[0], tweets[1:]
        return {"ok": True, "tweet": main, "thread_above": above,
                "replies": replies[:n]}
    finally:
        await _close_owned_page(page)


# ---------- 写动作 ----------


def _confirmed(verified: str, *, changed=True, **extra):
    return {"ok": True, "outcome": "confirmed", "changed": changed,
            "verified": verified, **extra}


def _failed(error: str, *, changed=False, **extra):
    return {"ok": False, "outcome": "failed", "changed": changed,
            "error": error, **extra}


def _uncertain(error: str, *, changed=True, **extra):
    return {"ok": None, "outcome": "uncertain", "changed": changed,
            "error": error, "retry_safe": False, **extra}


async def _click_with_x_response(page, click, markers):
    """先挂网络监听再点击；没抓到响应时不能把“可能已执行”误报成失败。"""
    try:
        async with page.expect_response(
                lambda response: any(marker in response.url for marker in markers),
                timeout=12000) as response_info:
            await click()
        response = await response_info.value
        try:
            payload = await response.json()
        except Exception:
            payload = None
        return {
            "captured": True,
            "status": response.status,
            "url": response.url,
            "payload": payload,
        }
    except PWTimeout:
        return {"captured": False}


def _network_public(network):
    return {k: v for k, v in (network or {}).items() if k != "payload"}


def _norm_text(text):
    return re.sub(r"\s+", " ", text or "").strip()


def _find_created_tweet_id(value, expected_text):
    """CreateTweet 响应里会混入用户等 rest_id，只接受旁边有相同 full_text 的 tweet。"""
    if isinstance(value, dict):
        legacy = value.get("legacy")
        rest_id = value.get("rest_id")
        if rest_id and isinstance(legacy, dict):
            full_text = legacy.get("full_text") or legacy.get("text")
            if full_text and _norm_text(full_text) == _norm_text(expected_text):
                return str(rest_id)
        for child in value.values():
            found = _find_created_tweet_id(child, expected_text)
            if found:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _find_created_tweet_id(child, expected_text)
            if found:
                return found
    return None


async def _verify_created_tweet(page, tweet_id, expected_text):
    """拿创建响应里的 tweet id 做一次纯读取复查。"""
    try:
        await goto(page, f"{BASE}/i/status/{tweet_id}")
        err = await wait_tweets(page)
        if err:
            return False
        tweets = await collect_tweets(page, 5)
        for tweet in tweets:
            if (_status_id(tweet.get("url")) == str(tweet_id)
                    and _norm_text(tweet.get("text")) == _norm_text(expected_text)):
                return True
    except Exception as e:
        log_action({"action": "_write_verify_failed", "tweet_id": tweet_id,
                    "error": f"{type(e).__name__}: {e}"})
    return False


async def _open_main_article(page, url):
    sid = _status_id(url)
    if not sid:
        return None, _failed("url 不像推文链接")
    await goto(page, f"{BASE}/i/status/{sid}")
    err = await wait_tweets(page)
    if err:
        return None, _failed("推文不存在或已删除" if err == "EMPTY" else err)
    art = page.locator('article[data-testid="tweet"]').filter(
        has=page.locator(f'a[href*="/status/{sid}"]')).first
    try:
        await art.wait_for(timeout=8000)
    except PWTimeout:
        art = page.locator('article[data-testid="tweet"]').first
    return art, None


async def act_like(ctx, body, undo=False):
    page = await new_page(ctx)
    try:
        art, err = await _open_main_article(page, body.get("url"))
        if err:
            return err
        want, other = ("unlike", "like") if undo else ("like", "unlike")
        btn = art.locator(f'button[data-testid="{want}"]')
        if await btn.count() == 0:
            if await art.locator(f'button[data-testid="{other}"]').count():
                return _confirmed("本来就是目标状态", changed=False)
            return _failed("页面上找不到点赞按钮，结构可能变了")
        marker = "UnfavoriteTweet" if undo else "FavoriteTweet"
        network = await _click_with_x_response(page, btn.first.click, [marker])
        try:
            await art.locator(f'button[data-testid="{other}"]').first.wait_for(timeout=6000)
            return _confirmed("按钮状态已翻转", network=_network_public(network))
        except PWTimeout:
            if network.get("captured") and int(network.get("status") or 0) >= 400:
                return _failed(f"X 接口返回 HTTP {network['status']}", changed=True,
                               network=_network_public(network))
            return _uncertain("已经点击，但按钮状态没有翻转，禁止自动重试",
                              network=_network_public(network))
    finally:
        await _close_owned_page(page)


async def act_repost(ctx, body):
    page = await new_page(ctx)
    try:
        art, err = await _open_main_article(page, body.get("url"))
        if err:
            return err
        btn = art.locator('button[data-testid="retweet"]')
        if await btn.count() == 0:
            if await art.locator('button[data-testid="unretweet"]').count():
                return _confirmed("本来就已转发", changed=False)
            return _failed("找不到转发按钮")
        await btn.first.click()
        confirm = page.locator('[data-testid="retweetConfirm"]')
        network = await _click_with_x_response(
            page, lambda: confirm.click(timeout=6000), ["CreateRetweet"])
        try:
            await art.locator('button[data-testid="unretweet"]').first.wait_for(timeout=6000)
            return _confirmed("按钮状态已翻转", network=_network_public(network))
        except PWTimeout:
            if network.get("captured") and int(network.get("status") or 0) >= 400:
                return _failed(f"X 接口返回 HTTP {network['status']}", changed=True,
                               network=_network_public(network))
            return _uncertain("已经点了转发确认，但状态没有翻转，禁止自动重试",
                              network=_network_public(network))
    finally:
        await _close_owned_page(page)


async def _type_and_send(page, text, button_tid):
    box = page.locator('[data-testid="tweetTextarea_0"]')
    await box.first.click(timeout=10000)
    await page.keyboard.type(text, delay=15)
    btn = page.locator(f'button[data-testid="{button_tid}"]')
    await btn.first.wait_for(timeout=6000)
    if await btn.first.is_disabled():
        return {"error": "发送按钮是灰的：内容可能超长或不合规"}
    network = await _click_with_x_response(page, btn.first.click, ["CreateTweet"])
    return {"network": network}


async def act_reply(ctx, body):
    text = (body.get("text") or "").strip()
    if not text:
        return _failed("缺参数 text")
    page = await new_page(ctx)
    try:
        art, err = await _open_main_article(page, body.get("url"))
        if err:
            return err
        sent = await _type_and_send(page, text, "tweetButtonInline")
        if sent.get("error"):
            return _failed(sent["error"])
        network = sent.get("network") or {}
        if network.get("captured") and int(network.get("status") or 0) >= 400:
            return _failed(f"X 接口返回 HTTP {network['status']}", changed=True,
                           network=_network_public(network))
        tweet_id = _find_created_tweet_id(network.get("payload"), text)
        if tweet_id and await _verify_created_tweet(page, tweet_id, text):
            return _confirmed("回复已按 tweet id 二次读取确认", tweet_id=tweet_id,
                              network=_network_public(network))
        return _uncertain("发送动作已发生，但没有拿到可二次确认的回复，禁止自动重试",
                          tweet_id=tweet_id, network=_network_public(network))
    finally:
        await _close_owned_page(page)


async def act_post(ctx, body):
    text = (body.get("text") or "").strip()
    if not text:
        return _failed("缺参数 text")
    page = await new_page(ctx)
    try:
        await goto(page, f"{BASE}/compose/post")
        if not await check_logged_in(page):
            return _failed("登录态失效")
        sent = await _type_and_send(page, text, "tweetButton")
        if sent.get("error"):
            return _failed(sent["error"])
        network = sent.get("network") or {}
        if network.get("captured") and int(network.get("status") or 0) >= 400:
            return _failed(f"X 接口返回 HTTP {network['status']}", changed=True,
                           network=_network_public(network))
        tweet_id = _find_created_tweet_id(network.get("payload"), text)
        if tweet_id and await _verify_created_tweet(page, tweet_id, text):
            return _confirmed("新推文已按 tweet id 二次读取确认", tweet_id=tweet_id,
                              network=_network_public(network))
        return _uncertain("发送动作已发生，但没有拿到可二次确认的新推文，禁止自动重试",
                          tweet_id=tweet_id, network=_network_public(network))
    finally:
        await _close_owned_page(page)


# ---------- 路由 ----------

READ_ACTIONS = {"feed": act_feed, "profile": act_profile, "read": act_read}
WRITE_ACTIONS = {"like": act_like,
                 "unlike": lambda c, b: act_like(c, b, undo=True),
                 "repost": act_repost, "reply": act_reply, "post": act_post}


async def _tab_sweeper():
    while True:
        await asyncio.sleep(60)
        closed = await _sweep_owned_orphans()
        if closed:
            log_action({"action": "_sweep_owned_orphans", "closed": closed})


@app.on_event("startup")
async def _start_sweeper():
    _load_state()
    closed = await _sweep_owned_orphans()
    if closed:
        log_action({"action": "_startup_sweep_owned_orphans", "closed": closed})
    asyncio.create_task(_tab_sweeper())


@app.get("/health")
async def health():
    try:
        ctx = await asyncio.wait_for(get_context(), timeout=45)
        return {"ok": True, "tabs": len(ctx.pages),
                "owned_pages": len(_owned_pages),
                "tracked_orphans": len(_owned_targets) - len(_owned_pages)}
    except asyncio.TimeoutError:
        return {"ok": False, "error": "连中继浏览器超时（含自愈重试仍失败）"}
    except Exception as e:
        return {"ok": False, "error": f"连不上中继浏览器: {e}"}


@app.post("/twitter")
async def twitter(req: Request):
    try:
        body = await req.json()
    except Exception:
        return {"ok": False, "error": "body 不是合法 JSON"}
    action = body.get("action")
    if action in READ_ACTIONS:
        async with READ_SEM:
            try:
                ctx = await get_context()
                return await asyncio.wait_for(
                    READ_ACTIONS[action](ctx, body), timeout=150)
            except asyncio.TimeoutError:
                return {"ok": False, "error": "动作整体超时（150秒），浏览器可能卡住了"}
            except Exception as e:
                return {"ok": False, "error": f"{type(e).__name__}: {e}"}
    if action in WRITE_ACTIONS:
        async with WRITE_LOCK:
            # 必须在写锁内判断/记账，两个并发请求不能同时穿过最后一个名额。
            gate = check_rate(action)
            if gate:
                result = _failed(gate)
                log_action({"action": action, "body": body, "result": result})
                return result
            try:
                ctx = await get_context()
                result = await asyncio.wait_for(
                    WRITE_ACTIONS[action](ctx, body), timeout=150)
            except asyncio.TimeoutError:
                result = _uncertain("动作整体超时；可能已经生效，禁止自动重试")
            except Exception as e:
                result = _uncertain(
                    f"动作执行中异常：{type(e).__name__}: {e}；是否生效不确定，禁止自动重试")
            if (result.get("changed")
                    and result.get("outcome") in ("confirmed", "uncertain")):
                record_rate(action)
            log_action({"action": action,
                        "url": body.get("url"), "text": body.get("text"),
                        "result": result})
            return result
    return _failed(
        f"不认识的 action: {action}。可用: feed/profile/read/like/unlike/repost/reply/post")
