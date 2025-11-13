#!/usr/bin/env python3
"""
Nefer Bot v3 - Industrial feature set for Instagram DMs using instagrapi.

Features:
- Concurrent message processing with ThreadPoolExecutor
- SQLite persistence for processed messages, opt-outs, and logs
- Rate limiting + backoff + retries
- Admin commands and user commands
- Pre-generation "Processing your request..." message
- Image caching with cleanup
- Optional OpenAI + Pollinations support
"""

import os
import time
import json
import sqlite3
import logging
import traceback
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from io import BytesIO
from typing import Optional
from collections import deque
from dataclasses import dataclass, field
from dotenv import load_dotenv
import requests
from instagrapi import Client
from instagrapi.types import DirectMessage
from math import inf
import shutil
import yaml
from tqdm import tqdm

# ---------------------------
# Load environment & config
# ---------------------------
load_dotenv()

# Core env
INSTA_USERNAME = os.getenv("INSTA_USERNAME")
INSTA_PASSWORD = os.getenv("INSTA_PASSWORD")
SESSION_FILE = os.getenv("SESSION_FILE", "session.json")
DB_FILE = os.getenv("DB_FILE", "nefer_bot.db")
IMAGE_CACHE_DIR = os.getenv("IMAGE_CACHE_DIR", "./image_cache")
LOG_FILE = os.getenv("LOG_FILE", "nefer_bot.log")
ADMIN_USERS = [u.strip().lower() for u in os.getenv("ADMIN_USERS", "").split(",") if u.strip()]
AI_PROVIDER = os.getenv("AI_PROVIDER", "pollinations").lower()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
# Optional YAML config override
CONFIG_YAML = os.getenv("CONFIG_YAML", "config.yaml")
custom_cfg = {}
if os.path.exists(CONFIG_YAML):
    with open(CONFIG_YAML, "r") as f:
        custom_cfg = yaml.safe_load(f) or {}

# Merge defaults with YAML override
CFG = {
    "worker_threads": custom_cfg.get("worker_threads", 6),
    "poll_interval": custom_cfg.get("poll_interval", 8),  # seconds
    "max_messages_fetch": custom_cfg.get("max_messages_fetch", 25),
    "message_stale_seconds": custom_cfg.get("message_stale_seconds", 300),
    "image_cache_max_files": custom_cfg.get("image_cache_max_files", 200),
    "image_cache_max_age_seconds": custom_cfg.get("image_cache_max_age_seconds", 60 * 60 * 24),
    "max_text_length": custom_cfg.get("max_text_length", 2000),
    "max_image_bytes": custom_cfg.get("max_image_bytes", 6 * 1024 * 1024),
    "rate_limit_per_minute": custom_cfg.get("rate_limit_per_minute", 40),
    "ai_timeout": custom_cfg.get("ai_timeout", 25),
}
# Create cache dir
os.makedirs(IMAGE_CACHE_DIR, exist_ok=True)

# ---------------------------
# Logging setup (rotating simple)
# ---------------------------
logger = logging.getLogger("nefer_bot_v3")
logger.setLevel(logging.DEBUG)
fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

fh = logging.FileHandler(LOG_FILE)
fh.setLevel(logging.DEBUG)
fh.setFormatter(fmt)
logger.addHandler(fh)

sh = logging.StreamHandler()
sh.setLevel(logging.INFO)
sh.setFormatter(fmt)
logger.addHandler(sh)

# ---------------------------
# SQLite persistence
# ---------------------------
def init_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS processed (
        msg_id TEXT PRIMARY KEY,
        thread_id TEXT,
        user TEXT,
        timestamp INTEGER
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS optouts (
        username TEXT PRIMARY KEY,
        reason TEXT,
        ts INTEGER
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER,
        level TEXT,
        message TEXT
    )
    """)
    conn.commit()
    return conn

db_conn = init_db()
db_lock = threading.Lock()

def mark_processed(msg_id, thread_id, user):
    with db_lock:
        cur = db_conn.cursor()
        cur.execute("INSERT OR IGNORE INTO processed (msg_id, thread_id, user, timestamp) VALUES (?, ?, ?, ?)",
                    (msg_id, thread_id, user, int(time.time())))
        db_conn.commit()

def is_processed(msg_id):
    with db_lock:
        cur = db_conn.cursor()
        cur.execute("SELECT 1 FROM processed WHERE msg_id = ?", (msg_id,))
        return cur.fetchone() is not None

def optout_user(username, reason=""):
    with db_lock:
        cur = db_conn.cursor()
        cur.execute("INSERT OR REPLACE INTO optouts (username, reason, ts) VALUES (?, ?, ?)",
                    (username.lower(), reason, int(time.time())))
        db_conn.commit()

def optin_user(username):
    with db_lock:
        cur = db_conn.cursor()
        cur.execute("DELETE FROM optouts WHERE username = ?", (username.lower(),))
        db_conn.commit()

def is_opted_out(username):
    with db_lock:
        cur = db_conn.cursor()
        cur.execute("SELECT 1 FROM optouts WHERE username = ?", (username.lower(),))
        return cur.fetchone() is not None

def db_log(level, message):
    with db_lock:
        cur = db_conn.cursor()
        cur.execute("INSERT INTO logs (ts, level, message) VALUES (?, ?, ?)", (int(time.time()), level, message))
        db_conn.commit()

# ---------------------------
# Rate limiter (token bucket)
# ---------------------------
class TokenBucket:
    def __init__(self, rate_per_minute):
        self.capacity = rate_per_minute
        self.tokens = rate_per_minute
        self.refill_rate = rate_per_minute / 60.0
        self.last = time.time()
        self.lock = threading.Lock()

    def consume(self, tokens=1):
        with self.lock:
            now = time.time()
            elapsed = now - self.last
            self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
            self.last = now
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False

rate_limiter = TokenBucket(CFG["rate_limit_per_minute"])

# ---------------------------
# AI provider abstraction
# ---------------------------
def ai_text_generate(prompt: str, timeout=CFG["ai_timeout"]) -> str:
    """Try OpenAI (if configured) then fallback to Pollinations text endpoint."""
    prompt = prompt.strip()
    if len(prompt) == 0:
        return "You sent an empty prompt."
    if AI_PROVIDER == "openai" and OPENAI_API_KEY:
        try:
            # Minimal OpenAI call using requests (no openai package required)
            headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
            data = {
                "model": "gpt-3.5-turbo",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 600,
            }
            r = requests.post("https://api.openai.com/v1/chat/completions", json=data, headers=headers, timeout=timeout)
            r.raise_for_status()
            j = r.json()
            return j["choices"][0]["message"]["content"].strip()
        except Exception as e:
            logger.warning("OpenAI text generation failed, falling back. %s", e)
    # Pollinations simple text fallback
    try:
        url = f"https://text.pollinations.ai/{requests.utils.requote_uri(prompt)}"
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        return r.text.strip()
    except Exception as e:
        logger.exception("AI text generation failed: %s", e)
        return "Sorry — I couldn't generate a reply right now."

def ai_image_generate(prompt: str, timeout=CFG["ai_timeout"]) -> Optional[BytesIO]:
    """Try Pollinations image generator; optionally add OpenAI DALL·E path if API key provided."""
    prompt = prompt.strip()
    # Basic pollinations
    try:
        url = f"https://image.pollinations.ai/prompt/{requests.utils.requote_uri(prompt)}"
        r = requests.get(url, timeout=timeout)
        if r.status_code == 200 and len(r.content) <= CFG["max_image_bytes"]:
            return BytesIO(r.content)
        logger.warning("Pollinations returned status %s or big image %s", r.status_code, len(r.content) if r.content else 0)
    except Exception:
        logger.exception("Pollinations image generation failed.")
    # Optional: OpenAI image generation (DALL·E)
    if OPENAI_API_KEY:
        try:
            headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
            payload = {
                "prompt": prompt,
                "size": "1024x1024",
                "n": 1
            }
            r = requests.post("https://api.openai.com/v1/images/generations", json=payload, headers=headers, timeout=timeout)
            r.raise_for_status()
            j = r.json()
            b64 = j["data"][0]["b64_json"]
            import base64
            img_bytes = base64.b64decode(b64)
            if len(img_bytes) <= CFG["max_image_bytes"]:
                return BytesIO(img_bytes)
        except Exception:
            logger.exception("OpenAI image generation failed.")
    return None

# ---------------------------
# Safety filters (very simple)
# ---------------------------
NSFW_KEYWORDS = {"nsfw", "porn", "sex", "nude", "xxx", "illegal", "bomb"}
def content_is_safe(prompt: str) -> bool:
    lower = prompt.lower()
    for k in NSFW_KEYWORDS:
        if k in lower:
            return False
    return True

# ---------------------------
# Image cache helpers
# ---------------------------
def cache_image(img_io: BytesIO, prefix="img") -> str:
    ts = int(time.time() * 1000)
    filename = f"{prefix}_{ts}.jpg"
    path = os.path.join(IMAGE_CACHE_DIR, filename)
    with open(path, "wb") as f:
        f.write(img_io.getbuffer())
    enforce_cache_limits()
    return path

def enforce_cache_limits():
    files = sorted(
        (os.path.join(IMAGE_CACHE_DIR, f) for f in os.listdir(IMAGE_CACHE_DIR)),
        key=lambda p: os.path.getmtime(p)
    )
    # Remove old files exceeding count
    while len(files) > CFG["image_cache_max_files"]:
        rm = files.pop(0)
        try:
            os.remove(rm)
        except:
            pass
    # Remove old based on age
    cutoff = time.time() - CFG["image_cache_max_age_seconds"]
    for f in files:
        try:
            if os.path.getmtime(f) < cutoff:
                os.remove(f)
        except:
            pass

# ---------------------------
# Instagram client management
# ---------------------------
client_lock = threading.Lock()
def create_client():
    c = Client()
    # optional: adjust c settings here (e.g., c.api_timeout)
    return c

client = create_client()
def login_client():
    global client
    with client_lock:
        try:
            if os.path.exists(SESSION_FILE):
                client.load_settings(SESSION_FILE)
            client.login(INSTA_USERNAME, INSTA_PASSWORD)
            client.dump_settings(SESSION_FILE)
            logger.info("Logged in as @%s", INSTA_USERNAME)
            db_log("INFO", f"Logged in as {INSTA_USERNAME}")
        except Exception as e:
            logger.exception("Login failed: %s", e)
            # attempt fresh client
            try:
                if os.path.exists(SESSION_FILE):
                    os.remove(SESSION_FILE)
                client = create_client()
                client.login(INSTA_USERNAME, INSTA_PASSWORD)
                client.dump_settings(SESSION_FILE)
                logger.info("Fresh login success.")
            except Exception as e2:
                logger.exception("Fresh login also failed: %s", e2)
                raise

# login once at start
login_client()

# ---------------------------
# Command parsing
# ---------------------------
@dataclass
class CommandResult:
    acted: bool = False
    response_text: Optional[str] = None
    send_image_path: Optional[str] = None

def parse_command_and_handle(msg: DirectMessage, thread_id: str, from_username: str) -> CommandResult:
    """
    Recognizes commands:
    - /help
    - /image <prompt>  OR messages with 'imagine' keyword
    - /text <prompt>
    - /optout [reason]
    - /optin
    - /status (admin)
    - /ban <username> (admin)
    - /unban <username> (admin)
    """
    text = (msg.text or "").strip()
    if not text:
        return CommandResult(False, None, None)
    lowered = text.lower()
    # Admin commands available to ADMIN_USERS
    tokens = text.split()
    cmd = tokens[0].lstrip("/").lower() if tokens else ""
    # Help
    if cmd == "help" or lowered.startswith("help"):
        help_msg = (
            "Nefer Bot Commands:\n"
            "/help - show this message\n"
            "/image <prompt> or use 'imagine' - generate image\n"
            "/text <prompt> - generate text reply\n"
            "/optout - stop receiving replies from the bot\n"
            "/optin - allow replies again\n"
            "Admins: /status, /ban <user>, /unban <user>\n"
            "Mention me with @yourusername to trigger.\n"
        )
        return CommandResult(True, help_msg, None)

    # optout / optin
    if cmd == "optout":
        optout_user(from_username, reason="user_requested")
        return CommandResult(True, "You have opted out. I will not reply to your messages until you /optin.", None)
    if cmd == "optin":
        optin_user(from_username)
        return CommandResult(True, "You are opted in — I will reply to your messages again.", None)

    # admin commands
    if from_username.lower() in ADMIN_USERS:
        if cmd == "status":
            return CommandResult(True, f"Bot running. Threads: {CFG['worker_threads']}. Rate limit: {CFG['rate_limit_per_minute']}/min", None)
        if cmd == "ban" and len(tokens) >= 2:
            to_ban = tokens[1].lstrip("@").lower()
            optout_user(to_ban, reason="admin_ban")
            return CommandResult(True, f"User @{to_ban} has been banned (opted out).", None)
        if cmd == "unban" and len(tokens) >= 2:
            to_unban = tokens[1].lstrip("@").lower()
            optin_user(to_unban)
            return CommandResult(True, f"User @{to_unban} has been unbanned.", None)

    # Image generation: explicit /image or 'imagine' keyword
    if cmd == "image" and len(tokens) >= 2:
        prompt = text.partition(" ")[2].strip()
        if not content_is_safe(prompt):
            return CommandResult(True, "Your prompt seems to contain unsafe terms; cannot generate.", None)
        img_io = ai_image_generate(prompt)
        if img_io:
            path = cache_image(img_io, prefix=f"img_{from_username}")
            return CommandResult(True, "Here is your generated image:", path)
        else:
            return CommandResult(True, "Sorry, I couldn't generate the image right now.", None)

    if "imagine" in lowered:
        # take text after 'imagine' or entire text
        idx = lowered.find("imagine")
        prompt = text[idx + len("imagine"):].strip() or text
        if not content_is_safe(prompt):
            return CommandResult(True, "Your prompt seems to contain unsafe terms; cannot generate.", None)
        img_io = ai_image_generate(prompt)
        if img_io:
            path = cache_image(img_io, prefix=f"img_{from_username}")
            return CommandResult(True, "Here is your generated image:", path)
        else:
            return CommandResult(True, "Sorry, I couldn't generate the image right now.", None)

    # Text generation: /text or fallback
    if cmd == "text" and len(tokens) >= 2:
        prompt = text.partition(" ")[2].strip()
        if not content_is_safe(prompt):
            return CommandResult(True, "Your prompt seems to contain unsafe terms; cannot generate.", None)
        reply = ai_text_generate(prompt)
        return CommandResult(True, reply, None)

    # fallback: treat as plain text prompt to AI
    if len(text) > 0:
        if not content_is_safe(text):
            return CommandResult(True, "Your message seems to contain disallowed content; cannot process.", None)
        reply = ai_text_generate(text)
        return CommandResult(True, reply, None)

    return CommandResult(False, None, None)

# ---------------------------
# Worker logic
# ---------------------------
executor = ThreadPoolExecutor(max_workers=CFG["worker_threads"])
work_queue = deque()  # queue of (msg, thread_id, username)

def send_text_safe(thread_id: str, text: str):
    """Rate-limited send with retries"""
    backoff = 1
    for attempt in range(6):
        if rate_limiter.consume():
            try:
                client.direct_send(text[:CFG["max_text_length"]], thread_ids=[thread_id])
                return True
            except Exception as e:
                logger.exception("Failed to send text (attempt %s): %s", attempt+1, e)
        time.sleep(backoff)
        backoff = min(30, backoff * 2)
    return False

def send_image_safe(thread_id: str, image_path: str, caption: Optional[str] = None):
    backoff = 1
    for attempt in range(6):
        if rate_limiter.consume():
            try:
                with open(image_path, "rb") as f:
                    client.direct_send_photo(f, thread_ids=[thread_id], caption=caption or "")
                return True
            except Exception as e:
                logger.exception("Failed to send image (attempt %s): %s", attempt+1, e)
        time.sleep(backoff)
        backoff = min(30, backoff * 2)
    return False

def worker_process(msg, thread_id, from_username):
    try:
        if is_processed(msg.id):
            logger.debug("Already processed %s", msg.id)
            return
        # Respect opt-out
        if is_opted_out(from_username):
            logger.info("User %s opted out; skipping", from_username)
            mark_processed(msg.id, thread_id, from_username)
            return

        # Send pre-processing confirmation
        try:
            # attempt to send confirmation (not guaranteed)
            send_text_safe(thread_id, "⏳ Processing your request...")
        except Exception:
            pass

        # Parse and handle commands (including AI generation)
        res = parse_command_and_handle(msg, thread_id, from_username)
        if res.acted:
            if res.send_image_path:
                ok = send_image_safe(thread_id, res.send_image_path, caption=res.response_text)
                if ok:
                    logger.info("Sent image to %s", from_username)
                    db_log("INFO", f"Image sent to {from_username}")
                else:
                    send_text_safe(thread_id, "Sorry, could not send the image after multiple tries.")
            elif res.response_text is not None:
                send_text_safe(thread_id, res.response_text)
                logger.info("Sent text to %s", from_username)
                db_log("INFO", f"Text sent to {from_username}")
            else:
                logger.debug("No send needed for %s", msg.id)
        else:
            # nothing recognized; mark processed
            logger.debug("No action for msg %s", msg.id)

        mark_processed(msg.id, thread_id, from_username)
    except Exception:
        logger.exception("Error in worker_process for msg %s", getattr(msg, "id", "<no id>"))

# ---------------------------
# Main polling loop
# ---------------------------
def run_polling_loop():
    logger.info("Starting main loop. Poll interval: %ss", CFG["poll_interval"])
    while True:
        try:
            # refresh client if needed
            try:
                if not client.user_id:
                    login_client()
            except Exception:
                logger.exception("Client validation/login failed.")

            threads = client.direct_threads(amount=10)
            logger.debug("Found %s threads", len(threads))
            two_minutes_ago = datetime.now() - timedelta(seconds=CFG["message_stale_seconds"])

            tasks = []
            for thread in threads:
                # fetch recent messages for that thread
                try:
                    messages = client.direct_messages(thread.id, amount=CFG["max_messages_fetch"])
                except Exception:
              
