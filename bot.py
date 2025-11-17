#!/usr/bin/env python3
"""
Nefer Bot — Async AI workers (aiohttp) + instagrapi sync calls in threadpool.
All original features preserved. AI calls converted to async (aiohttp)
to avoid blocking the event loop. Polling is async and instagrapi calls are
executed in a ThreadPoolExecutor.
"""

import os
import time
import json
import sqlite3
import logging
import traceback
import threading
import random
import re
import asyncio
import base64
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from io import BytesIO
from urllib.parse import urlparse
from dotenv import load_dotenv
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import sys
import binascii
import aiohttp
import requests  # still used in a few places, preserved
from instagrapi import Client

# -------------------------
# Load environment/config
# -------------------------
load_dotenv()

INSTA_USERNAME = os.getenv("INSTA_USERNAME")
INSTA_PASSWORD = os.getenv("INSTA_PASSWORD")
SESSION_FILE = os.getenv("SESSION_FILE", "session.json")
DB_FILE = os.getenv("DB_FILE", "nefer_bot.db")
IMAGE_CACHE_DIR = os.getenv("IMAGE_CACHE_DIR", "./image_cache")
LOG_FILE = os.getenv("LOG_FILE", "nefer_bot.log")
ADMIN_USERS = [u.strip().lower() for u in os.getenv("ADMIN_USERS", "").split(",") if u.strip()]
AI_PROVIDER = os.getenv("AI_PROVIDER", "pollinations").lower() 
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")  # optional
JSON_DECRYPT_KEY = os.getenv("JSON_DECRYPT_KEY") # to decrypt the session.json
# runtime config (tweakable)
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "12"))           # base poll interval (seconds)
BACKOFF_BASE = float(os.getenv("BACKOFF_BASE", "5"))              # base backoff on failure
BACKOFF_MAX = int(os.getenv("BACKOFF_MAX", "600"))                # cap backoff (s)
WORKER_THREADS = int(os.getenv("WORKER_THREADS", "6"))
MAX_IMAGE_CACHE = int(os.getenv("MAX_IMAGE_CACHE", "300"))
IMAGE_MAX_AGE = int(os.getenv("IMAGE_MAX_AGE", str(60*60*24*7)))  # 7 days
RATE_PER_MINUTE = int(os.getenv("RATE_PER_MINUTE", "40"))         # token bucket
ASYNC_WORKER_POOL_SIZE = int(os.getenv("ASYNC_WORKER_POOL_SIZE", "6"))

# human mimic settings (conservative and safe)
HUMAN_MIN_DELAY = float(os.getenv("HUMAN_MIN_DELAY", "0.6"))
HUMAN_MAX_DELAY = float(os.getenv("HUMAN_MAX_DELAY", "2.0"))
POLL_JITTER_MAX = float(os.getenv("POLL_JITTER_MAX", "3.5"))

os.makedirs(IMAGE_CACHE_DIR, exist_ok=True)

# -------------------------
# Logging
# -------------------------
logger = logging.getLogger("nefer_bot")
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(LOG_FILE)
fh.setLevel(logging.DEBUG)
fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
fh.setFormatter(fmt)
logger.addHandler(fh)
sh = logging.StreamHandler()
sh.setFormatter(fmt)
sh.setLevel(logging.INFO)
logger.addHandler(sh)

def decrypt_and_overwrite(password: str, path="session.json"):
    # load encrypted metadata
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    salt = bytes.fromhex(data["salt"])
    nonce = bytes.fromhex(data["nonce"])
    ciphertext = bytes.fromhex(data["ciphertext"])

    key = generate_key_from_password(password, salt)
    aes = AESGCM(key)

    decrypted_bytes = aes.decrypt(nonce, ciphertext, None)

    # Convert AES output bytes → UTF-8
    text = decrypted_bytes.decode("utf-8")

    # Overwrite with original JSON
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

    print("Decrypted ✔ (AES-256 restored)")


# -------------------------
# SQLite persistence
# -------------------------
_db_lock = threading.Lock()
def init_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS processed (
        msg_id TEXT PRIMARY KEY,
        thread_id TEXT,
        username TEXT,
        ts INTEGER
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS optouts (
        username TEXT PRIMARY KEY,
        reason TEXT,
        ts INTEGER
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER,
        level TEXT,
        message TEXT
    )""")
    conn.commit()
    return conn

db = init_db()

def mark_processed(msg_id, thread_id, username):
    with _db_lock:
        cur = db.cursor()
        cur.execute("INSERT OR IGNORE INTO processed (msg_id, thread_id, username, ts) VALUES (?, ?, ?, ?)",
                    (msg_id, thread_id, username, int(time.time())))
        db.commit()

def is_processed(msg_id):
    with _db_lock:
        cur = db.cursor()
        cur.execute("SELECT 1 FROM processed WHERE msg_id = ?", (msg_id,))
        return cur.fetchone() is not None

def optout_user(username, reason="user_requested"):
    with _db_lock:
        cur = db.cursor()
        cur.execute("INSERT OR REPLACE INTO optouts (username, reason, ts) VALUES (?, ?, ?)",
                    (username.lower(), reason, int(time.time())))
        db.commit()

def optin_user(username):
    with _db_lock:
        cur = db.cursor()
        cur.execute("DELETE FROM optouts WHERE username = ?", (username.lower(),))
        db.commit()

def is_opted_out(username):
    with _db_lock:
        cur = db.cursor()
        cur.execute("SELECT 1 FROM optouts WHERE username = ?", (username.lower(),))
        return cur.fetchone() is not None

def db_log(level, message):
    with _db_lock:
        cur = db.cursor()
        cur.execute("INSERT INTO logs (ts, level, message) VALUES (?, ?, ?)", (int(time.time()), level, message))
        db.commit()

# -------------------------
# Rate limiter (token bucket)
# -------------------------
class TokenBucket:
    def __init__(self, rate_per_minute=60):
        self.capacity = max(1, rate_per_minute)
        self.tokens = float(self.capacity)
        self.refill = self.capacity / 60.0
        self.last = time.time()
        self.lock = threading.Lock()
    def consume(self, n=1):
        with self.lock:
            now = time.time()
            self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.refill)
            self.last = now
            if self.tokens >= n:
                self.tokens -= n
                return True
            return False

rate_limiter = TokenBucket(rate_per_minute=RATE_PER_MINUTE)

# -------------------------
# Async AI provider functions (aiohttp)
# -------------------------
# These are async and non-blocking. They replace previous request-based functions.

async def ai_text_generate_async(prompt, timeout=20):
    prompt = (prompt or "").strip()
    if not prompt:
        return "Empty prompt."
    # OpenAI async
    if AI_PROVIDER == "openai" and OPENAI_API_KEY:
        try:
            url = "https://api.openai.com/v1/chat/completions"
            headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
            payload = {"model":"gpt-3.5-turbo","messages":[{"role":"user","content":prompt}], "max_tokens":400}
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, headers=headers, timeout=timeout) as r:
                    text = await r.text()
                    if r.status >= 400:
                        logger.warning("OpenAI text API returned %s: %s", r.status, text)
                    j = await r.json()
                    return j["choices"][0]["message"]["content"].strip()
        except Exception as e:
            logger.warning("OpenAI async failed, falling back: %s", e)
    # Pollinations text fallback (async GET)
    try:
        url = f"https://text.pollinations.ai/{aiohttp.helpers.quote(prompt, safe='')}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=timeout) as r:
                if r.status == 200:
                    return (await r.text()).strip()
                else:
                    logger.warning("Pollinations text returned %s", r.status)
    except Exception as e:
        logger.exception("AI text generation async failed")
    return "Sorry, couldn't generate text right now."

async def ai_image_generate_async(prompt, timeout=25):
    prompt = (prompt or "").strip()
    if not prompt:
        return None
    nsfw_blacklist = {"nsfw","porn","sex","nude","xxx","illegal","bomb"}
    if any(k in prompt.lower() for k in nsfw_blacklist):
        logger.info("Blocked unsafe prompt")
        return None
    # Try Pollinations image (async)
    try:
        url = f"https://image.pollinations.ai/prompt/{aiohttp.helpers.quote(prompt, safe='')}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=timeout) as r:
                if r.status == 200:
                    data = await r.read()
                    return BytesIO(data)
                else:
                    logger.warning("Pollinations image returned %s", r.status)
    except Exception:
        logger.exception("Pollinations image async failed")
    # Fallback to OpenAI images (if key present) - use async POST
    if OPENAI_API_KEY:
        try:
            url = "https://api.openai.com/v1/images/generations"
            headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
            payload = {"prompt":prompt,"n":1,"size":"1024x1024"}
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, headers=headers, timeout=timeout) as r:
                    j = await r.json()
                    b64 = j["data"][0]["b64_json"]
                    return BytesIO(base64.b64decode(b64))
        except Exception:
            logger.exception("OpenAI image fallback async failed")
    return None

# For backward compatibility, provide sync wrappers that run the async functions
def ai_text_generate(prompt, timeout=20):
    try:
        return asyncio.get_event_loop().run_until_complete(ai_text_generate_async(prompt, timeout=timeout))
    except RuntimeError:
        # if no loop (called from separate thread), run a new loop
        return asyncio.new_event_loop().run_until_complete(ai_text_generate_async(prompt, timeout=timeout))

def ai_image_generate(prompt, timeout=25):
    try:
        return asyncio.get_event_loop().run_until_complete(ai_image_generate_async(prompt, timeout=timeout))
    except RuntimeError:
        return asyncio.new_event_loop().run_until_complete(ai_image_generate_async(prompt, timeout=timeout))

# -------------------------
# Image cache helpers
# -------------------------
def cache_image_io(b: BytesIO, prefix="img"):
    ts = int(time.time()*1000)
    filename = f"{prefix}_{ts}.jpg"
    path = os.path.join(IMAGE_CACHE_DIR, filename)
    with open(path, "wb") as f:
        f.write(b.getbuffer())
    _enforce_image_cache()
    return path

def _enforce_image_cache():
    try:
        files = sorted([os.path.join(IMAGE_CACHE_DIR, f) for f in os.listdir(IMAGE_CACHE_DIR)], key=os.path.getmtime)
    except FileNotFoundError:
        return
    while len(files) > MAX_IMAGE_CACHE:
        try:
            os.remove(files.pop(0))
        except: pass
    cutoff = time.time() - IMAGE_MAX_AGE
    for p in files:
        try:
            if os.path.getmtime(p) < cutoff:
                os.remove(p)
        except: pass

# -------------------------
# Instagram client & login (sync)
# -------------------------
client_lock = threading.Lock()
client = None

SAFE_USER_AGENTS = [
    "Instagram 280.0.0.0 Android",
    "Instagram 279.0.0.0 Android",
    "Instagram 278.0.0.0 Android",
    "Instagram 277.0.0.0 Android"
]

def create_client_instance():
    c = Client()
    try:
        ua = random.choice(SAFE_USER_AGENTS)
        c.set_user_agent(ua)
    except Exception:
        pass
    return c

def interactive_challenge_resolve(c: Client):
    try:
        print("Challenge flow detected. Follow instructions from console and check your Instagram (app/email).")
        c.challenge_resolve(c.last_json)
    except Exception as e:
        logger.exception("Interactive challenge resolution failed: %s", e)
        raise

def login_client():
    global client
    with client_lock:
        client = create_client_instance()
        try:
            if os.path.exists(SESSION_FILE):
                try:
                    client.load_settings(SESSION_FILE)
                except Exception as e:
                    logger.warning("Failed loading session file: %s", e)
                try:
                    client.login(INSTA_USERNAME, INSTA_PASSWORD)
                    client.dump_settings(SESSION_FILE)
                    logger.info("Logged in using saved session.")
                    db_log("INFO", "Logged in using saved session")
                    return
                except Exception as e:
                    logger.warning("Saved session login failed: %s — removing and retrying fresh login", e)
                    try: os.remove(SESSION_FILE)
                    except: pass
            client.login(INSTA_USERNAME, INSTA_PASSWORD)
            client.dump_settings(SESSION_FILE)
            logger.info("Fresh login success.")
            db_log("INFO", "Fresh login success")
        except Exception as e:
            logger.exception("Login attempt raised exception: %s", e)
            try:
                if hasattr(client, "last_json") and client.last_json and "challenge" in json.dumps(client.last_json):
                    logger.info("Attempting interactive challenge resolution...")
                    interactive_challenge_resolve(client)
                    try:
                        client.dump_settings(SESSION_FILE)
                    except:
                        pass
                    return
                else:
                    raise
            except Exception as ex:
                logger.exception("Challenge resolution or login failed: %s", ex)
                raise

# initial login
try:
    login_client()
except Exception as e:
    logger.exception("Initial login failed; exit. Fix login manually and rerun.")
    raise SystemExit(1)

# small stabilization wait
time.sleep(2.0)

# -------------------------
# Utility helpers for media/profile handling
# -------------------------
def extract_username_from_url(url_or_username):
    s = (url_or_username or "").strip()
    if not s:
        return None
    if s.startswith("http://") or s.startswith("https://"):
        try:
            p = urlparse(s)
            parts = [x for x in p.path.split("/") if x]
            if parts:
                return parts[0]
            return None
        except:
            return None
    return s.lstrip("@")

def get_user_info_by_any(identifier):
    uname = extract_username_from_url(identifier)
    if not uname:
        return None
    try:
        u = client.user_info_by_username(uname)
        return {
            "pk": u.pk,
            "username": u.username,
            "full_name": u.full_name,
            "is_private": u.is_private,
            "followers": getattr(u, "follower_count", None),
            "following": getattr(u, "following_count", None),
            "biography": getattr(u, "biography", ""),
            "external_url": getattr(u, "external_url", ""),
        }
    except Exception as e:
        logger.exception("Failed to get user info for %s: %s", uname, e)
        return None

def media_id_from_url(url):
    try:
        if not url:
            return None
        if isinstance(url, str) and url.startswith("http"):
            return client.media_pk_from_url(url)
        return url
    except Exception as e:
        logger.exception("Failed to parse media id from url: %s", e)
        return None

# -------------------------
# Command parsing and handlers (sync helpers remain)
# -------------------------
def _human_typing_delay(min_delay=HUMAN_MIN_DELAY, max_delay=HUMAN_MAX_DELAY, scale=1.0):
    d = random.uniform(min_delay, max_delay) * scale
    time.sleep(d)

def send_text_thread(thread_id, text):
    if not rate_limiter.consume():
        logger.debug("Rate limiter blocked text send")
        time.sleep(random.uniform(0.8, 2.0))
    try:
        _human_typing_delay(min_delay=0.6, max_delay=1.6, scale=min(1.5, max(1.0, len(text)/120.0)))
        client.direct_send(text[:2000], thread_ids=[thread_id])
    except Exception as e:
        logger.exception("Failed to send text: %s", e)

def send_photo_thread(thread_id, path, caption=""):
    if not rate_limiter.consume():
        logger.debug("Rate limiter blocked photo send")
        time.sleep(random.uniform(1.0, 3.0))
    try:
        with open(path, "rb") as f:
            _human_typing_delay(min_delay=0.8, max_delay=2.2)
            client.direct_send_photo(f, thread_ids=[thread_id], caption=caption)
        return True
    except Exception as e:
        logger.exception("Failed to send photo: %s", e)
        return False

def send_video_thread(thread_id, path, caption=""):
    if not rate_limiter.consume():
        logger.debug("Rate limiter blocked video send")
        time.sleep(random.uniform(1.2, 4.0))
    try:
        with open(path, "rb") as f:
            _human_typing_delay(min_delay=1.0, max_delay=3.0)
            client.direct_send_video(f, thread_ids=[thread_id], caption=caption)
        return True
    except Exception as e:
        logger.exception("Failed to send video: %s", e)
        return False

def handle_command_text(msg_text, thread_id, from_username):
    if not msg_text:
        return None, None
    txt = msg_text.strip()
    lower = txt.lower()
    if lower.startswith("/help") or lower.startswith("help"):
        help_msg = (
            "Nefer Bot — commands:\n"
            "/help - show commands\n"
            "/text <prompt> - AI text reply\n"
            "/image <prompt> OR include 'imagine' - AI image\n"
            "/like <post_url> - like a post\n"
            "/comment <post_url> | <comment> - comment on post\n"
            "/reel <local_video_path> | <caption> - upload a reel (local file)\n"
            "/userinfo <profile_url or username> - get profile info\n"
            "/optout /optin - control replies\n"
            "Admin: /status /ban <user> /unban <user> /shutdown\n"
        )
        return "text", help_msg
    if lower.startswith("/optout"):
        optout_user(from_username)
        return "text", "You have opted out. Use /optin to opt back in."
    if lower.startswith("/optin"):
        optin_user(from_username)
        return "text", "You have opted in. I will reply again."
    if from_username.lower() in ADMIN_USERS:
        if lower.startswith("/status"):
            st = f"Bot running. Async workers: {ASYNC_WORKER_POOL_SIZE}. Rate: {rate_limiter.capacity}/min"
            return "text", st
        if lower.startswith("/ban "):
            toks = txt.split()
            if len(toks) >= 2:
                target = toks[1].lstrip("@").lower()
                optout_user(target, reason="admin_ban")
                return "text", f"@{target} banned (opted out)."
        if lower.startswith("/unban "):
            toks = txt.split()
            if len(toks) >= 2:
                target = toks[1].lstrip("@").lower()
                optin_user(target)
                return "text", f"@{target} unbanned."
        if lower.startswith("/shutdown"):
            return "shutdown", "Shutting down as requested by admin."
    if lower.startswith("/text "):
        prompt = txt.partition(" ")[2].strip()
        return "aitext", prompt
    if lower.startswith("/image "):
        prompt = txt.partition(" ")[2].strip()
        return "aiimage", prompt
    if "imagine" in lower:
        idx = lower.find("imagine")
        prompt = txt[idx + len("imagine"):].strip() or txt
        return "aiimage", prompt
    if lower.startswith("/like "):
        target = txt.partition(" ")[2].strip()
        return "like", target
    if lower.startswith("/comment "):
        rest = txt.partition(" ")[2].strip()
        if "|" in rest:
            url_part, _, comment_part = rest.partition("|")
            return "comment", (url_part.strip(), comment_part.strip())
        else:
            return "comment", (rest, "")
    if lower.startswith("/reel "):
        rest = txt.partition(" ")[2].strip()
        if "|" in rest:
            path, _, cap = rest.partition("|")
            return "reel", (path.strip(), cap.strip())
        else:
            return "reel", (rest, "")
    if lower.startswith("/userinfo "):
        param = txt.partition(" ")[2].strip()
        return "userinfo", param
    return "aitext", txt

# -------------------------
# Legacy (sync) worker left for compatibility (unused by async scheduler)
# -------------------------
def worker_process(msg, thread_id, from_username):
    """
    Legacy synchronous worker (kept for compatibility).
    The async scheduler uses async_worker instead.
    """
    try:
        mid = getattr(msg, "id", None) or str(time.time())
        if is_processed(mid):
            return
        # Very small wrapper: reuse existing synchronous helpers where convenient
        uname = getattr(msg, "user", getattr(msg, "user_id", "unknown"))
        text = getattr(msg, "text", "") or ""
        kind, payload = handle_command_text(text, thread_id, uname)
        # For sync behavior call blocking ai_* wrappers which run the async functions
        if kind == "aitext":
            reply = ai_text_generate(payload)
            send_text_thread(thread_id, reply)
        elif kind == "aiimage":
            img_io = ai_image_generate(payload)
            if img_io:
                path = cache_image_io(img_io, prefix=f"{uname}")
                send_photo_thread(thread_id, path, caption=f"Image for: {payload[:120]}")
        # Other commands reuse sync helpers
        # (the full sync branch is intentionally minimal; async_worker is preferred)
        mark_processed(mid, thread_id, uname)
    except Exception:
        logger.exception("legacy worker_process error")

# -------------------------
# ThreadPool for blocking calls
# -------------------------
blocking_executor = ThreadPoolExecutor(max_workers=max(WORKER_THREADS, ASYNC_WORKER_POOL_SIZE, 12))

def run_in_executor_sync(fn, *args, **kwargs):
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(blocking_executor, lambda: fn(*args, **kwargs))

# -------------------------
# Async worker (new) — uses async AI calls and runs instagrapi actions in executor
# -------------------------
async def async_worker(msg, thread_id, from_username):
    """
    Async worker that:
      - Uses ai_text_generate_async / ai_image_generate_async (aiohttp)
      - Runs instagrapi (sync) calls in the blocking_executor
    """
    try:
        mid = getattr(msg, "id", None) or str(time.time())
        if is_processed(mid):
            logger.debug("Already processed %s", mid)
            return
        # extract username string
        try:
            uobj = getattr(msg, "user", None)
            if isinstance(uobj, str):
                uname = uobj
            elif hasattr(uobj, "username"):
                uname = uobj.username
            else:
                uname = getattr(msg, "user_id", "unknown")
        except Exception:
            uname = getattr(msg, "user_id", "unknown")

        if is_opted_out(uname):
            logger.info("User %s opted out; skipping", uname)
            mark_processed(mid, thread_id, uname)
            return

        text = getattr(msg, "text", "") or ""
        logger.info("Async processing msg from %s: %s", uname, (text or "")[:140])

        # pre-confirm (send_text_thread is sync; call in executor)
        try:
            await run_in_executor_sync(send_text_thread, thread_id, "⏳ Processing your request...")
        except Exception:
            pass

        kind, payload = handle_command_text(text, thread_id, uname)

        # handle commands — use async AI calls where heavy network used
        if kind == "text":
            await run_in_executor_sync(send_text_thread, thread_id, payload)

        elif kind == "aitext":
            prompt = payload
            # inform user then generate asynchronously
            await run_in_executor_sync(send_text_thread, thread_id, "⏳ Generating AI response...")
            try:
                reply = await ai_text_generate_async(prompt, timeout=30)
            except Exception as e:
                logger.exception("AI text async failed: %s", e)
                reply = "Sorry, AI failed to generate a response."
            await run_in_executor_sync(send_text_thread, thread_id, reply)

        elif kind == "aiimage":
            prompt = payload
            await run_in_executor_sync(send_text_thread, thread_id, "⏳ Generating image...")
            try:
                img_io = await ai_image_generate_async(prompt, timeout=40)
            except Exception as e:
                logger.exception("AI image async failed: %s", e)
                img_io = None
            if img_io:
                try:
                    path = cache_image_io(img_io, prefix=f"{uname}")
                    ok = await run_in_executor_sync(send_photo_thread, thread_id, path, f"Image for: {prompt[:120]}")
                    if not ok:
                        await run_in_executor_sync(send_text_thread, thread_id, "Sorry, couldn't send the image.")
                except Exception:
                    logger.exception("Image caching or sending failed")
                    await run_in_executor_sync(send_text_thread, thread_id, "Sorry, couldn't process the image.")
            else:
                await run_in_executor_sync(send_text_thread, thread_id, "Sorry, couldn't generate the image (maybe blocked or failed).")

        elif kind == "like":
            target = payload
            try:
                media_pk = await run_in_executor_sync(media_id_from_url, target)
                if media_pk:
                    if not rate_limiter.consume():
                        await asyncio.sleep(random.uniform(1.0, 2.5))
                    # call media_like in executor
                    await run_in_executor_sync(_human_typing_delay, 0.4, 1.2)
                    await run_in_executor_sync(client.media_like, media_pk)
                    await run_in_executor_sync(send_text_thread, thread_id, "✅ Liked that post.")
                else:
                    await run_in_executor_sync(send_text_thread, thread_id, "Couldn't detect media from that URL.")
            except Exception as e:
                logger.exception("Like failed")
                await run_in_executor_sync(send_text_thread, thread_id, f"Failed to like: {e}")

        elif kind == "comment":
            url_part, comment_text = payload
            try:
                media_pk = await run_in_executor_sync(media_id_from_url, url_part)
                if not media_pk:
                    await run_in_executor_sync(send_text_thread, thread_id, "Couldn't detect media from that URL.")
                else:
                    if not comment_text:
                        await run_in_executor_sync(send_text_thread, thread_id, "Please provide comment text after '|' in the command.")
                    else:
                        if not rate_limiter.consume():
                            await asyncio.sleep(random.uniform(1.0, 3.0))
                        await run_in_executor_sync(_human_typing_delay, 0.6, 1.8)
                        await run_in_executor_sync(client.media_comment, media_pk, comment_text[:1000])
                        await run_in_executor_sync(send_text_thread, thread_id, "✅ Comment posted.")
            except Exception as e:
                logger.exception("Comment failed")
                await run_in_executor_sync(send_text_thread, thread_id, f"Failed to comment: {e}")

        elif kind == "reel":
            local_path, caption = payload
            if not await run_in_executor_sync(os.path.exists, local_path):
                await run_in_executor_sync(send_text_thread, thread_id, "Local file not found. Provide a valid path.")
            else:
                try:
                    await run_in_executor_sync(_human_typing_delay, 2.0, 4.5)
                    uploaded = False
                    # try several upload methods in executor
                    try:
                        await run_in_executor_sync(client.video_upload, local_path, caption)
                        uploaded = True
                        await run_in_executor_sync(send_text_thread, thread_id, "✅ Reel uploaded (via video_upload).")
                    except Exception:
                        try:
                            await run_in_executor_sync(client.reel_upload, local_path, caption)
                            uploaded = True
                            await run_in_executor_sync(send_text_thread, thread_id, "✅ Reel uploaded (via reel_upload).")
                        except Exception:
                            try:
                                await run_in_executor_sync(client.clip_upload, local_path, caption)
                                uploaded = True
                                await run_in_executor_sync(send_text_thread, thread_id, "✅ Reel uploaded (via clip_upload).")
                            except Exception:
                                raise
                    if not uploaded:
                        await run_in_executor_sync(send_text_thread, thread_id, "Upload attempted but not confirmed.")
                except Exception as e:
                    logger.exception("Reel upload failed")
                    await run_in_executor_sync(send_text_thread, thread_id, f"Failed to upload reel: {e}")

        elif kind == "userinfo":
            param = payload
            info = await run_in_executor_sync(get_user_info_by_any, param)
            if info:
                s = json.dumps(info, indent=2, ensure_ascii=False)
                await run_in_executor_sync(send_text_thread, thread_id, f"User info:\n{s}")
            else:
                await run_in_executor_sync(send_text_thread, thread_id, "Could not fetch user info (maybe private or not found).")

        elif kind == "shutdown":
            await run_in_executor_sync(send_text_thread, thread_id, "Shutting down as requested by admin.")
            logger.info("Shutdown requested by admin %s", from_username)
            mark_processed(mid, thread_id, from_username)
            os._exit(0)
        else:
            logger.debug("Unknown action kind=%s payload=%s", kind, payload)

        mark_processed(mid, thread_id, from_username)
    except Exception:
        logger.exception("Error in async_worker")

# -------------------------
# Safe async wrappers for direct threads/messages (blocking client inside executor)
# -------------------------
async def async_safe_direct_threads(amount=10, retries=3):
    for attempt in range(retries):
        try:
            threads = await run_in_executor_sync(client.direct_threads, amount)
            return threads or []
        except Exception as e:
            logger.warning("async_safe_direct_threads attempt %d failed: %s", attempt+1, e)
            wait = min(BACKOFF_BASE * (2 ** attempt), BACKOFF_MAX) + random.uniform(0.5, 1.5)
            await asyncio.sleep(wait)
    logger.error("async_safe_direct_threads giving up after %d attempts", retries)
    return []

async def async_safe_direct_messages(thread_id, amount=10, retries=2):
    for attempt in range(retries):
        try:
            messages = await run_in_executor_sync(client.direct_messages, thread_id, amount)
            return messages or []
        except Exception as e:
            logger.warning("async_safe_direct_messages attempt %d failed for %s: %s", attempt+1, thread_id, e)
            await asyncio.sleep(min(2**attempt + random.random(), 10))
    return []

# -------------------------
# Async polling loop (schedules async_worker)
# -------------------------
RUNNING = True

async def polling_loop():
    backoff = BACKOFF_BASE
    logger.info("Starting async polling loop (base poll interval %ss)", POLL_INTERVAL)
    while RUNNING:
        try:
            if not client or not getattr(client, "user_id", None):
                logger.warning("Client not logged in, attempting re-login...")
                await run_in_executor_sync(login_client)
                await asyncio.sleep(2 + random.random())

            threads = await async_safe_direct_threads(amount=10)
            logger.debug("Fetched %d threads", len(threads))

            scheduled = []
            for thread in threads:
                messages = await async_safe_direct_messages(thread.id, amount=8)
                if not messages:
                    continue
                for msg in messages:
                    try:
                        if getattr(msg, "user_id", None) == client.user_id:
                            continue
                    except Exception:
                        pass
                    ts = getattr(msg, "timestamp", None)
                    if ts:
                        try:
                            tsnaive = ts.replace(tzinfo=None)
                            if tsnaive < datetime.now() - timedelta(minutes=10):
                                continue
                        except Exception:
                            pass
                    mentioned = False
                    try:
                        if getattr(msg, "mentions", None):
                            for m in msg.mentions:
                                try:
                                    if m.user.username.lower() == client.username.lower():
                                        mentioned = True
                                        break
                                except Exception:
                                    pass
                        if not mentioned and getattr(msg, "text", "") and f"@{client.username.lower()}" in msg.text.lower():
                            mentioned = True
                    except Exception:
                        pass
                    users = getattr(thread, "users", []) or []
                    is_private = len(users) <= 2
                    should_process = mentioned or is_private
                    if should_process:
                        mid = getattr(msg, "id", None) or str(time.time())
                        if not is_processed(mid):
                            # extract username string
                            try:
                                uobj = getattr(msg, "user", None)
                                if isinstance(uobj, str):
                                    uname = uobj
                                elif hasattr(uobj, "username"):
                                    uname = uobj.username
                                else:
                                    uname = getattr(msg, "user_id", "unknown")
                            except Exception:
                                uname = getattr(msg, "user_id", "unknown")
                            # schedule async_worker
                            scheduled.append(asyncio.create_task(async_worker(msg, thread.id, uname)))

            # Let tasks run concurrently but keep loop responsive
            if scheduled:
                # short wait — tasks will continue in background
                await asyncio.sleep(0.05)
            backoff = BACKOFF_BASE
            sleep_for = POLL_INTERVAL + random.uniform(0.0, POLL_JITTER_MAX)
            await asyncio.sleep(sleep_for)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception("Async main polling error: %s", e)
            db_log("ERROR", f"Async main loop error: {e}")
            logger.info("Backing off for %s seconds (with jitter)", backoff)
            await asyncio.sleep(backoff + random.uniform(0.0, 2.0))
            backoff = min(backoff * 2, BACKOFF_MAX)

# -------------------------
# Entrypoint
# -------------------------
def main():
    decrypt_and_overwrite(JSON_DECRYPT_KEY)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(polling_loop())
    except KeyboardInterrupt:
        logger.info("Shutdown requested by keyboard interrupt.")
    except Exception:
        logger.exception("Unhandled exception in main")
    finally:
        global RUNNING
        RUNNING = False
        pending = asyncio.all_tasks(loop=loop)
        for t in pending:
            t.cancel()
        try:
            loop.run_until_complete(asyncio.sleep(0.2))
        except Exception:
            pass
        loop.stop()
        loop.close()
        try:
            blocking_executor.shutdown(wait=True)
        except Exception:
            pass
        logger.info("Nefer Bot stopped.")
        db_log("INFO", "Nefer Bot stopped.")

if __name__ == "__main__":
    logger.info("Nefer Bot starting up (async ai + threadpool for instagrapi)...")
    db_log("INFO", "Nefer Bot starting up (async)")
    main()





