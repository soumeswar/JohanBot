from flask import Flask, render_template_string, request, redirect, url_for, session, flash
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from functools import wraps
import threading, os, subprocess, time
from datetime import timedelta
from dotenv import load_dotenv

# --- Load environment variables ---
load_dotenv()
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "changeme")
SECRET_KEY = os.getenv("SECRET_KEY", "supersecretkey")

app = Flask(__name__)
app.secret_key = SECRET_KEY
app.permanent_session_lifetime = timedelta(minutes=30)

# --- Rate limiting ---
limiter = Limiter(get_remote_address, app=app, default_limits=["10 per minute"])

# --- Simple bot runner ---
def run_bot():
    subprocess.call(["python", "bot.py"])

# --- Decorator for protecting routes ---
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated_function

# --- Login Page ---
@app.route("/login", methods=["GET", "POST"])
@limiter.limit("5 per minute")
def login():
    if request.method == "POST":
        password = request.form.get("password")
        if password == ADMIN_PASSWORD:
            session["logged_in"] = True
            session.permanent = True
            flash("Login successful!", "success")
            return redirect(url_for("dashboard"))
        else:
            flash("Invalid password.", "error")
    return render_template_string(LOGIN_HTML)

# --- Logout ---
@app.route("/logout")
def logout():
    session.clear()
    flash("Logged out successfully.", "info")
    return redirect(url_for("login"))

# --- Dashboard (protected) ---
@app.route("/")
@login_required
def dashboard():
    return render_template_string(DASHBOARD_HTML)

# --- Start bot thread (optional button on dashboard) ---
@app.route("/start-bot")
@login_required
def start_bot():
    global bot_thread
    if not bot_thread or not bot_thread.is_alive():
        bot_thread = threading.Thread(target=run_bot, daemon=True)
        bot_thread.start()
        flash("✅ Bot started successfully!", "success")
    else:
        flash("⚙️ Bot is already running.", "info")
    return redirect(url_for("dashboard"))

# --- HTML Templates ---
LOGIN_HTML = """
<!DOCTYPE html>
<html>
<head>
<title>NeferBot Admin Login</title>
<style>
body {
  background: linear-gradient(135deg, #0d0d0d, #1a1a1a);
  color: white;
  font-family: "Segoe UI", sans-serif;
  display: flex;
  justify-content: center;
  align-items: center;
  height: 100vh;
}
form {
  background: #111;
  padding: 40px;
  border-radius: 20px;
  box-shadow: 0 0 30px #00ffae20;
  text-align: center;
  width: 300px;
}
input[type=password] {
  width: 100%;
  padding: 10px;
  margin: 15px 0;
  border-radius: 10px;
  border: none;
  outline: none;
}
button {
  background: #00ffaa;
  border: none;
  color: black;
  font-weight: bold;
  padding: 10px 20px;
  border-radius: 10px;
  cursor: pointer;
  transition: 0.3s;
}
button:hover {
  background: #00cc88;
}
.flash {
  margin: 10px;
  color: #ff5555;
}
</style>
</head>
<body>
<form method="post">
  <h2>🔒 NeferBot Login</h2>
  {% with messages = get_flashed_messages(with_categories=true) %}
    {% for category, message in messages %}
      <div class="flash">{{ message }}</div>
    {% endfor %}
  {% endwith %}
  <input type="password" name="password" placeholder="Enter admin password" required><br>
  <button type="submit">Login</button>
</form>
</body>
</html>
"""

DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
<title>NeferBot Admin Dashboard</title>
<style>
body {
  background: radial-gradient(circle at top left, #101010, #000);
  color: #00ffaa;
  font-family: "Segoe UI", sans-serif;
  padding: 40px;
}
h1 {
  text-align: center;
}
.container {
  max-width: 600px;
  margin: auto;
  background: #0b0b0b;
  padding: 30px;
  border-radius: 15px;
  box-shadow: 0 0 20px #00ffaa30;
  text-align: center;
}
button {
  background: #00ffaa;
  color: black;
  padding: 10px 20px;
  border-radius: 10px;
  border: none;
  cursor: pointer;
  transition: 0.3s;
  margin: 10px;
}
button:hover {
  background: #00cc88;
}
.flash {
  margin: 10px;
  color: #ff5555;
}
a {
  color: #ff6666;
  text-decoration: none;
}
</style>
</head>
<body>
<div class="container">
  <h1>⚙️ NeferBot Dashboard</h1>
  {% with messages = get_flashed_messages(with_categories=true) %}
    {% for category, message in messages %}
      <div class="flash">{{ message }}</div>
    {% endfor %}
  {% endwith %}
  <p>Status: 🟢 Running</p>
  <a href="/start-bot"><button>🚀 Start Bot</button></a>
  <a href="/logout"><button>🔒 Logout</button></a>
</div>
</body>
</html>
"""

# --- Run Flask + Bot thread together ---
if __name__ == "__main__":
    # Start bot automatically in background
    threading.Thread(target=run_bot, daemon=True).start()

    # Run Flask (Render compatible)
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)
