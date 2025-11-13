#!/usr/bin/env python3
"""
Nefer Bot — All-in-one Instagram assistant (single-file).
Features: robust login + session, challenge resolve, polling with backoff,
threaded workers, AI hooks, likes/comments/reels upload, userinfo from url,
sqlite persistence, token bucket rate limiting, image cache, admin commands.
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
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from io import BytesIO
from urllib.parse import urlparse
from dotenv import load_dotenv
import requests
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

# runtime config (tweakable)
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "8"))               # normal poll
BACKOFF_BASE = float(os.getenv("BACKOFF_BASE", "5"))              # base backoff on failure
BACKOFF_MAX = int(os.getenv("BACKOFF_MAX", "600"))                # cap backoff (s)
WORKER_THREADS = int(os.getenv("WORKER_THREADS", "6"))
MAX_IMAGE_CACHE = int(os.getenv("MAX_IMAGE_CACHE", "300"))
IMAGE_MAX_AGE = int(os.getenv("IMAGE_MAX_AGE", str(60*60*24*7)))  # 7 days

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

rate_limiter = TokenBucket(rate_per_minute=int(os.getenv("RATE_PER_MINUTE", "40")))

# -------------------------
# AI provider functions
# -------------------------
def ai_text_generate(prompt, timeout=20):
    prompt = (prompt or "").strip()
    if not prompt:
        return "Empty prompt."
    if AI_PROVIDER == "openai" and OPENAI_API_KEY:
        try:
            headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
            payload = {"model":"gpt-3.5-turbo","messages":[{"role":"user","content":prompt}], "max_tokens":400}
            r = requests.post("https://api.openai.com/v1/chat/completions", json=payload, headers=headers, timeout=timeout)
            r.raise_for_status()
            j = r.json()
            return j["choices"][0]["message"]["content"].strip()
        except Exception as e:
            logger.warning("OpenAI text failed, falling back to pollinations: %s", e)
    # pollinations text fallback
    try:
        url = f"https://text.pollinations.ai/{requests.utils.requote_uri(prompt)}"
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        return r.text.strip()
    except Exception as e:
        logger.exception("AI text generation failed")
        return "Sorry, couldn't generate text right now."

def ai_image_generate(prompt, timeout=25):
    prompt = (prompt or "").strip()
    if not prompt:
        return None
    # simple safety keyword check
    nsfw_blacklist = {"nsfw","porn","sex","nude","xxx","illegal","bomb"}
    if any(k in prompt.lower() for k in nsfw_blacklist):
        logger.info("Blocked unsafe prompt")
        return None
    try:
        url = f"https://image.pollinations.ai/prompt/{requests.utils.requote_uri(prompt)}"
        r = requests.get(url, timeout=timeout)
        if r.status_code == 200 and r.content:
            return BytesIO(r.content)
    except Exception:
        logger.exception("Pollinations image failed")
    # optionally fallback to OpenAI images if configured
    if OPENAI_API_KEY:
        try:
            headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
            payload = {"prompt":prompt,"n":1,"size":"1024x1024"}
            r = requests.post("https://api.openai.com/v1/images/generations", json=payload, headers=headers, timeout=timeout)
            r.raise_for_status()
            j = r.json()
            b64 = j["data"][0]["b64_json"]
            import base64
            return BytesIO(base64.b64decode(b64))
        except Exception:
            logger.exception("OpenAI image fallback failed")
    return None

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
    files = sorted([os.path.join(IMAGE_CACHE_DIR, f) for f in os.listdir(IMAGE_CACHE_DIR)], key=os.path.getmtime)
    # remove oldest if over limit
    while len(files) > MAX_IMAGE_CACHE:
        try:
            os.remove(files.pop(0))
        except: pass
    # remove by age
    cutoff = time.time() - IMAGE_MAX_AGE
    for p in files:
        try:
            if os.path.getmtime(p) < cutoff:
                os.remove(p)
        except: pass

# -------------------------
# Instagram client & login
# -------------------------
client_lock = threading.Lock()
client = None

def create_client_instance():
    c = Client()
    # tweak device info? careful — keep reasonable
    return c

def interactive_challenge_resolve(c: Client):
    # instagrapi provides challenge_resolve helpers; here we drive simple flow:
    try:
        print("Challenge flow detected. Follow instructions from console and check your Instagram (app/email).")
        c.challenge_resolve(c.last_json)
        # if challenge asks for code, library usually prompts automatically;
        # our earlier attempts had interactive prompts; we keep it simple:
    except Exception as e:
        logger.exception("Interactive challenge resolution failed: %s", e)
        raise

def login_client():
    global client
    with client_lock:
        client = create_client_instance()
        # try to load session
        try:
            if os.path.exists(SESSION_FILE):
                client.load_settings(SESSION_FILE)
                try:
                    client.login(INSTA_USERNAME, INSTA_PASSWORD)
                    client.dump_settings(SESSION_FILE)
                    logger.info("Logged in using saved session.")
                    db_log("INFO", "Logged in using saved session")
                    return
                except Exception as e:
                    logger.warning("Saved session failed: %s — removing and retrying fresh login", e)
                    try: os.remove(SESSION_FILE)
                    except: pass
            # fresh login
            client.login(INSTA_USERNAME, INSTA_PASSWORD)
            client.dump_settings(SESSION_FILE)
            logger.info("Fresh login success.")
            db_log("INFO", "Fresh login success")
        except Exception as e:
            # If challenge_required occurs, try to interactively resolve
            logger.exception("Login attempt raised exception: %s", e)
            # attempt to resolve challenge if available
            try:
                if hasattr(client, "last_json") and client.last_json and "challenge" in json.dumps(client.last_json):
                    logger.info("Attempting interactive challenge resolution...")
                    interactive_challenge_resolve(client)
                    # post-challenge, try to dump session
                    try:
                        client.dump_settings(SESSION_FILE)
                    except:
                        pass
                else:
                    raise
            except Exception as ex:
                logger.exception("Challenge resolution or login failed: %s", ex)
                raise

# login at start
try:
    login_client()
except Exception as e:
    logger.exception("Initial login failed; exit. Fix login manually and rerun.")
    raise SystemExit(1)

# Small stabilization wait after challenge to avoid immediate 500s
time.sleep(6)

# -------------------------
# Utility helpers for media/profile handling
# -------------------------
def extract_username_from_url(url_or_username):
    # Accept username or profile URL (instagram.com/username)
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
    # may include @
    return s.lstrip("@")

def get_user_info_by_any(identifier):
    # identifier: username or profile url
    uname = extract_username_from_url(identifier)
    if not uname:
        return None
    try:
        u = client.user_info_by_username(uname)
        # return safe subset
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
    # Instagram post URL patterns contain /p/ or /reel/ or /tv/
    try:
        if url.startswith("http"):
            # instagrapi has media_pk_from_url
            return client.media_pk_from_url(url)
        # maybe the user provided a pk already
        return url
    except Exception as e:
        logger.exception("Failed to parse media id from url: %s", e)
        return None

# -------------------------
# Command parsing and handlers
# -------------------------
def send_text_thread(thread_id, text):
    if not rate_limiter.consume(): 
        logger.debug("Rate limiter blocked text send")
        time.sleep(2)
    try:
        client.direct_send(text[:2000], thread_ids=[thread_id])
    except Exception as e:
        logger.exception("Failed to send text: %s", e)

def send_photo_thread(thread_id, path, caption=""):
    if not rate_limiter.consume(): 
        logger.debug("Rate limiter blocked photo send")
        time.sleep(2)
    try:
        with open(path, "rb") as f:
            client.direct_send_photo(f, thread_ids=[thread_id], caption=caption)
        return True
    except Exception as e:
        logger.exception("Failed to send photo: %s", e)
        return False

def send_video_thread(thread_id, path, caption=""):
    if not rate_limiter.consume(): 
        logger.debug("Rate limiter blocked video send")
        time.sleep(2)
    try:
        with open(path, "rb") as f:
            client.direct_send_video(f, thread_ids=[thread_id], caption=caption)
        return True
    except Exception as e:
        logger.exception("Failed to send video: %s", e)
        return False

def handle_command_text(msg_text, thread_id, from_username):
    """
    Parse incoming message text and run commands.
    Commands:
      /help
      /text <prompt>
      /image <prompt>   or contains 'imagine'
      /like <post_url>
      /comment <post_url> | comment here
      /reel <local_video_path> | caption
      /userinfo <profile_url or username>
      /optout /optin
      admin: /status /ban /unban /shutdown
    """
    if not msg_text:
        return None, None  # no action

    txt = msg_text.strip()
    lower = txt.lower()

    # simple prefix parse
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

    # optout/optin
    if lower.startswith("/optout"):
        optout_user(from_username)
        return "text", "You have opted out. Use /optin to opt back in."
    if lower.startswith("/optin"):
        optin_user(from_username)
        return "text", "You have opted in. I will reply again."

    # admin
    if from_username.lower() in ADMIN_USERS:
        if lower.startswith("/status"):
            st = f"Bot running. Threads: {WORKER_THREADS}. Rate: {rate_limiter.capacity}/min"
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

    # /text
    if lower.startswith("/text "):
        prompt = txt.partition(" ")[2].strip()
        # respond with pre-message then generate
        return "aitext", prompt

    # /image
    if lower.startswith("/image "):
        prompt = txt.partition(" ")[2].strip()
        return "aiimage", prompt

    # contains imagine (inline)
    if "imagine" in lower:
        idx = lower.find("imagine")
        prompt = txt[idx + len("imagine"):].strip() or txt
        return "aiimage", prompt

    # /like
    if lower.startswith("/like "):
        target = txt.partition(" ")[2].strip()
        return "like", target

    # /comment
    if lower.startswith("/comment "):
        rest = txt.partition(" ")[2].strip()
        # support "url | comment"
        if "|" in rest:
            url_part, _, comment_part = rest.partition("|")
            return "comment", (url_part.strip(), comment_part.strip())
        else:
            return "comment", (rest, "")

    # /reel
    if lower.startswith("/reel "):
        rest = txt.partition(" ")[2].strip()
        if "|" in rest:
            path, _, cap = rest.partition("|")
            return "reel", (path.strip(), cap.strip())
        else:
            return "reel", (rest, "")

    # /userinfo
    if lower.startswith("/userinfo "):
        param = txt.partition(" ")[2].strip()
        return "userinfo", param

    # fallback: treat as AI text prompt
    # but don't do so if user opted out
    return "aitext", txt

# -------------------------
# Worker: process a single DM message
# -------------------------
executor = ThreadPoolExecutor(max_workers=WORKER_THREADS)

def worker_process(msg, thread_id, from_username):
    try:
        mid = getattr(msg, "id", None) or str(time.time())
        if is_processed(mid):
            logger.debug("Already processed %s", mid)
            return
        if is_opted_out(from_username):
            logger.info("User %s opted out; skipping", from_username)
            mark_processed(mid, thread_id, from_username)
            return

        text = getattr(msg, "text", "") or ""
        logger.info("Processing msg from %s: %s", from_username, text[:120])

        # send pre-confirmation
        try:
            send_text_thread(thread_id, "⏳ Processing your request...")
        except Exception:
            pass

        kind, payload = handle_command_text(text, thread_id, from_username)

        if kind == "text":
            send_text_thread(thread_id, payload)

        elif kind == "aitext":
            prompt = payload
            reply = ai_text_generate(prompt)
            send_text_thread(thread_id, reply)

        elif kind == "aiimage":
            prompt = payload
            img_io = ai_image_generate(prompt)
            if img_io:
                path = cache_image_io(img_io, prefix=f"{from_username}")
                ok = send_photo_thread(thread_id, path, caption=f"Image for: {prompt[:120]}")
                if not ok:
                    send_text_thread(thread_id, "Sorry, couldn't send the image.")
            else:
                send_text_thread(thread_id, "Sorry, couldn't generate the image (maybe blocked or failed).")

        elif kind == "like":
            target = payload
            media_pk = None
            try:
                media_pk = media_id_from_url(target)
                if media_pk:
                    client.media_like(media_pk)
                    send_text_thread(thread_id, "✅ Liked that post.")
                else:
                    send_text_thread(thread_id, "Couldn't detect media from that URL.")
            except Exception as e:
                logger.exception("Like failed")
                send_text_thread(thread_id, f"Failed to like: {e}")

        elif kind == "comment":
            url_part, comment_text = payload
            try:
                media_pk = media_id_from_url(url_part)
                if not media_pk:
                    send_text_thread(thread_id, "Couldn't detect media from that URL.")
                else:
                    if not comment_text:
                        send_text_thread(thread_id, "Please provide comment text after '|' in the command.")
                    else:
                        client.media_comment(media_pk, comment_text)
                        send_text_thread(thread_id, "✅ Comment posted.")
            except Exception as e:
                logger.exception("Comment failed")
                send_text_thread(thread_id, f"Failed to comment: {e}")

        elif kind == "reel":
            local_path, caption = payload
            if not os.path.exists(local_path):
                send_text_thread(thread_id, "Local file not found. Provide a valid path.")
            else:
                try:
                    # attempt video upload as reel; instagrapi exposes video_upload and reel_upload functions in some versions
                    try:
                        client.video_upload(local_path, caption=caption)
                        send_text_thread(thread_id, "✅ Reel uploaded (via video_upload).")
                    except Exception:
                        # fallback: try clip/reel upload if available
                        try:
                            client.reel_upload(local_path, caption=caption)
                            send_text_thread(thread_id, "✅ Reel uploaded (via reel_upload).")
                        except Exception:
                            raise
                except Exception as e:
                    logger.exception("Reel upload failed")
                    send_text_thread(thread_id, f"Failed to upload reel: {e}")

        elif kind == "userinfo":
            param = payload
            info = get_user_info_by_any(param)
            if info:
                s = json.dumps(info, indent=2, ensure_ascii=False)
                send_text_thread(thread_id, f"User info:\n{s}")
            else:
                send_text_thread(thread_id, "Could not fetch user info (maybe private or not found).")

        elif kind == "shutdown":
            send_text_thread(thread_id, "Shutting down as requested by admin.")
            logger.info("Shutdown requested by admin %s", from_username)
            os._exit(0)

        else:
            # unknown action — ignore or reply
            logger.debug("Unknown action kind=%s payload=%s", kind, payload)
            # mark processed anyway
        mark_processed(mid, thread_id, from_username)
    except Exception:
        logger.exception("Error in worker_process")

# -------------------------
# Polling loop with backoff & jitter — robust
# -------------------------
def run_polling_loop():
    backoff = BACKOFF_BASE
    jitter = lambda: random.uniform(0.0, 2.0)
    logger.info("Starting polling loop (poll interval %ss)", POLL_INTERVAL)
    while True:
        try:
            # ensure client valid
            if not client or not getattr(client, "user_id", None):
                logger.warning("Client not logged in, attempting re-login...")
                login_client()
                time.sleep(5)

            # fetch threads (safe)
            threads = client.direct_threads(amount=10)
            logger.debug("Fetched %d threads", len(threads))

            for thread in threads:
                # fetch recent messages
                try:
                    messages = client.direct_messages(thread.id, amount=10)
                except Exception as e:
                    logger.exception("Failed to fetch messages for thread %s: %s", getattr(thread, "id", "<no id>"), e)
                    continue
                for msg in messages:
                    # skip messages sent by bot itself
                    if getattr(msg, "user_id", None) == client.user_id:
                        continue
                    # timestamp filter: consider last 5 minutes
                    ts = getattr(msg, "timestamp", None)
                    if ts:
                        try:
                            tsnaive = ts.replace(tzinfo=None)
                            if tsnaive < datetime.now() - timedelta(minutes=10):
                                continue
                        except:
                            pass
                    # detect mentions or private chat
                    mentioned = False
                    try:
                        if getattr(msg, "mentions", None):
                            for m in msg.mentions:
                                try:
                                    if m.user.username.lower() == client.username.lower():
                                        mentioned = True
                                        break
                                except:
                                    pass
                        if not mentioned and getattr(msg, "text", "") and f"@{client.username.lower()}" in msg.text.lower():
                            mentioned = True
                    except Exception:
                        pass
                    # is private?
                    users = getattr(thread, "users", []) or []
                    is_private = len(users) <= 2
                    should_process = mentioned or is_private
                    if should_process:
                        mid = getattr(msg, "id", None) or str(time.time())
                        if not is_processed(mid):
                            # submit worker
                            executor.submit(worker_process, msg, thread.id, getattr(msg, "user", getattr(msg, "user_id", "unknown")))
            # success -> reset backoff
            backoff = BACKOFF_BASE
            # sleep a randomized poll interval to avoid pattern detection
            time.sleep(POLL_INTERVAL + random.uniform(0.0, 2.0))
        except Exception as e:
            logger.exception("Main polling loop error: %s", e)
            # incremental backoff (exponential with cap)
            logger.info("Backing off for %s seconds (with jitter)", backoff)
            time.sleep(backoff + jitter())
            backoff = min(backoff * 2, BACKOFF_MAX)

# run
if __name__ == "__main__":
    logger.info("Nefer Bot starting up...")
    try:
        run_polling_loop()
    except KeyboardInterrupt:
        logger.info("Shutdown requested by keyboard interrupt.")
    except SystemExit:
        logger.info("System exit invoked.")
    except Exception:
        logger.exception("Unhandled exception in main")
