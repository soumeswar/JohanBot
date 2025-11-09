from instagrapi import Client
import time
import os
from dotenv import load_dotenv
import requests
from datetime import datetime, timedelta
from threading import Timer
from io import BytesIO

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
temp_files = {}

def generate_ai_response_text(prompt: str) -> str:
    try:
        url = f"https://text.pollinations.ai/{prompt}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.text.strip()
    except Exception as e:
        print(f"⚠️ AI text request failed: {e}")
        return "Sorry, I couldn't get a reply right now."

def generate_ai_image(prompt: str):
    try:
        url = f"https://image.pollinations.ai/prompt/{prompt}"
        response = requests.get(url, timeout=20)
        if response.status_code == 200:
            return BytesIO(response.content)
        else:
            print(f"⚠️ Image request failed: {response.status_code}")
            return None
    except Exception as e:
        print(f"⚠️ AI image request failed: {e}")
        return None

def schedule_delete(key):
    def delete_file():
        if key in temp_files:
            del temp_files[key]
            print(f"🧹 Image buffer {key} auto-deleted after 1 minute.")
    Timer(60, delete_file).start()

def handle_message(msg, thread_id: str):
    text = msg.text.strip()
    if msg.id in processed_messages:
        return
    processed_messages.add(msg.id)
    print(f"💬 Mentioned Prompt: {text}")

    lower_text = text.lower()
    if "imagine" in lower_text:
        prompt = text.split("imagine", 1)[1].strip() or text
        print(f"🖼️ Generating image for: {prompt}")
        img_data = generate_ai_image(prompt)
        if img_data:
            key = f"img_{msg.id}"
            temp_files[key] = img_data
            try:
                client.direct_send("", thread_ids=[thread_id], file=img_data)
                print("✅ Image sent successfully.")
                schedule_delete(key)
            except Exception as e:
                print(f"⚠️ Error sending image: {e}")
                del temp_files[key]
        else:
            client.direct_send("Sorry, I couldn't generate that image.", thread_ids=[thread_id])
    else:
        ai_reply = generate_ai_response_text(text)
        print(f"🤖 Reply: {ai_reply[:80]}...")
        try:
            client.direct_send(ai_reply, thread_ids=[thread_id])
        except Exception as e:
            print(f"⚠️ Error sending message: {e}")

print("🤖 Nefer Bot is running (mention + imagine + auto-clean)...")

while True:
    try:
        threads = client.direct_threads(amount=10)
        two_minutes_ago = datetime.now() - timedelta(minutes=2)

        for thread in threads:
            if len(thread.users) > 2:  # Only process group chats
                messages = client.direct_messages(thread.id, amount=20)
                for msg in messages:
                    if msg.user_id == client.user_id:
                        continue
                    if msg.timestamp and msg.timestamp.replace(tzinfo=None) >= two_minutes_ago:
                        mentioned = False

                        # Check both structured mentions and plain-text mentions
                        if msg.mentions:
                            for mention in msg.mentions:
                                if mention.user.username.lower() == client.username.lower():
                                    mentioned = True
                                    break
                        if not mentioned and f"@{client.username.lower()}" in msg.text.lower():
                            mentioned = True

                        if mentioned:
                            handle_message(msg, thread.id)
                            break
        time.sleep(10)
    except Exception as e:
        print(f"⚠️ Main loop error: {e}")
        time.sleep(10)
