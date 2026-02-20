import os
from dotenv import load_dotenv

load_dotenv()

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")
BOT_TOKEN = os.getenv("BOT_TOKEN")
BOT_API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}" if BOT_TOKEN else ""
SESSION_NAME = os.getenv("SESSION_NAME", "/tmp/my_account")
MY_CHANNEL_ID = os.getenv("MY_CHANNEL_ID")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_ID_RAW = os.getenv("ADMIN_ID")
ADMIN_ID = int(ADMIN_ID_RAW) if ADMIN_ID_RAW and ADMIN_ID_RAW.isdigit() else None
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME")
CRON_SECRET = os.getenv("CRON_SECRET")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
AI_MODEL_NAME = os.getenv("AI_MODEL_NAME", "Qwen/Qwen3-Coder-Next:novita")
AI_BASE_URL = os.getenv("AI_BASE_URL", "https://router.huggingface.co/v1")
AI_API_KEY = os.getenv("AI_API_KEY")


def validate_required_settings() -> list[str]:
    required = {
        "API_ID": API_ID,
        "API_HASH": API_HASH,
        "SESSION_STRING": SESSION_STRING,
        "BOT_TOKEN": BOT_TOKEN,
        "DATABASE_URL": DATABASE_URL,
        "MY_CHANNEL_ID": MY_CHANNEL_ID,
        "ADMIN_ID": ADMIN_ID,
    }
    return [key for key, value in required.items() if not value]
