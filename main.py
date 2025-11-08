from flask import Flask
import threading
import os
import subprocess

app = Flask(__name__)

@app.route('/')
def home():
    return "✅ NeferBot is alive and running!"

def run_bot():
    # Runs your original bot.py exactly as-is
    subprocess.call(["python", "bot.py"])

if __name__ == "__main__":
    # Start your bot in a background thread
    threading.Thread(target=run_bot).start()

    # Run Flask server so Render detects an open port
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
