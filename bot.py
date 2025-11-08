from instagrapi import Client
import time
import os
from dotenv import load_dotenv
import requests

load_dotenv()

USERNAME = os.getenv("INSTA_USERNAME")
PASSWORD = os.getenv("INSTA_PASSWORD")

try:
    client = Client()
    client.load_settings("session.json")
    client.login(USERNAME, PASSWORD)
except:
    client = Client()
    client.login(USERNAME, PASSWORD)
    client.dump_settings("session.json")

processed_messages = set()
BOT_PREFIX = "!Nefer chat "

def generate_ai_response_text(prompt: str) -> str:
    """Fetch AI text from Pollinations API"""
    try:
        url = f"https://text.pollinations.ai/{prompt}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.text.strip()
    except Exception as e:
        print(f"⚠️ AI request failed: {e}")
        return "Sorry, I couldn't get a reply right now."


def handle_message(msg, thread_id: str):
    """Process a message and respond if it starts with !Rimuru chat"""
    text = msg.text.strip()

    if msg.id in processed_messages:
        return
    processed_messages.add(msg.id)

    if text.lower().startswith(BOT_PREFIX.lower()):
        prompt = text[len(BOT_PREFIX):].strip()
        print(f"💬 Prompt: {prompt}")

        ai_reply = generate_ai_response_text(prompt)
        print(f"🤖 Reply: {ai_reply[:80]}...")

        try:
            client.direct_send(ai_reply, thread_ids=[thread_id])
        except Exception as e:
            print(f"⚠️ Error sending message: {e}")


print("🤖 Nefer Bot is running...")

while True:
    try:
        threads = client.direct_threads(amount=5)
        for thread in threads:
            if len(thread.users) > 2:
                messages = client.direct_messages(thread.id, amount=1)
                if messages:
                    last_msg = messages[0]
                    if last_msg.user_id != client.user_id:
                        handle_message(last_msg, thread.id)
        time.sleep(5)
    except Exception as e:
        print(f"⚠️ Main loop error: {e}")
        time.sleep(10)
