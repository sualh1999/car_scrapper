import asyncio
import re
import json
import os
import random
import traceback
import uuid
import sys
import psycopg2
from datetime import datetime

from pyrogram import Client, filters
from pyrogram.types import InputMediaPhoto
from openai import AsyncOpenAI
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse
import uvicorn
import threading
import httpx

load_dotenv()

# ---------- CONFIG ----------
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")

BOT_TOKEN = "6970368451:AAGVYEUitV6hFD9KS29Bk0S-nDPJNvWpMGs" #os.getenv("BOT_TOKEN")
BOT_API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"

SESSION_NAME = "/tmp/my_account"
MY_CHANNEL_ID = os.getenv("MY_CHANNEL_ID")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_ID = int(os.getenv("ADMIN_ID"))
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME") # New: Admin username for Pyrogram filters
is_scraping = False
AI_MODEL_NAME = "tngtech/deepseek-r1t2-chimera:free"

# Global async client for bot API calls
http_client = httpx.AsyncClient()

# This is for the Pyrogram Client, not the bot. The bot uses ADMIN_ID for webhook.
is_admin_pyrogram = filters.create(lambda _, __, m: m.from_user and m.from_user.username == ADMIN_USERNAME)
# ----------------------------

# ---------- USERBOT ----------
# u_app = Client(SESSION_NAME, api_id=API_ID, api_hash=API_HASH, session_string=os.getenv("SESSION_STRING"))

# ---------- OPENROUTER CONFIG ----------
def _load_ai_clients():
    clients = []
    i = 0
    while True:
        key = os.getenv(f"OPENROUTER_API_KEY__{i}")
        if key:
            clients.append(AsyncOpenAI(base_url="https://openrouter.ai/api/v1", api_key=key))
            i += 1
        else:
            # Also check for the single key for backward compatibility
            if i == 0:
                single_key = os.getenv("OPENROUTER_API_KEY")
                if single_key:
                    clients.append(AsyncOpenAI(base_url="https://openrouter.ai/api/v1", api_key=single_key))
            break
    print(f"Loaded {len(clients)} OpenRouter AI clients.")
    return clients

ai_clients = _load_ai_clients()
if not ai_clients:
    print("Warning: No OPENROUTER_API_KEY or OPENROUTER_API_KEY__n environment variables set. AI caption parsing will be disabled.")
    
if not BOT_TOKEN:
	print("ERROR: No Bot Token Found!")
	raise
# ---------------------------------------


# ---------- CONTACT INFO ----------
CONTACT_INFO = """

Rijal Cars
- 2% commission is required.
መኪና ለመግዛትም ሆነ ለመሸጥ፣ ከስር ባለው ቁጥር ይደውሉልን።
📞 0982148598
📞 0991923566
ተጨማሪ መኪኖችን ለማግኘት የቴሌግራም ቻናላችንን ይቀላቀሉ።
https://t.me/Rijalcars
"""
# ----------------------------------
# ---------- UTILS ----------
def log(msg: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def generate_car_id():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            # This query might be slow on very large tables without an index on LENGTH(id).
            cursor.execute("""
                SELECT id FROM my_schema.cars 
                WHERE LENGTH(id) = 4 
                ORDER BY created_at DESC, id DESC 
                LIMIT 1
            """)
            
            chars = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
            last_id = None

            # Iterate through potential candidates from newest to oldest
            for row in cursor.fetchall():
                candidate = row[0]
                if all(c in chars for c in candidate):
                    last_id = candidate
                    break # Found the newest valid one

    if last_id is None:
        return "C13s" # No valid IDs found, start from the beginning

    # --- Increment logic (base-62) ---
    char_map = {c: i for i, c in enumerate(chars)}
    n = len(chars)
    id_list = list(last_id)
    
    # Start from the rightmost character and move left
    for i in range(len(id_list) - 1, -1, -1):
        char = id_list[i]
        index = char_map[char]
        
        # If the character is not the last in the set, increment it and we're done
        if index < n - 1:
            id_list[i] = chars[index + 1]
            return "".join(id_list)
        # If it is the last, reset to the first character and carry over to the left
        else:
            id_list[i] = chars[0]
            
    # If the loop completes, it means we've overflowed (e.g., from "ZZZZ")
    log("WARNING: Car ID overflow. Resetting to start value.")
    return "C13s"

async def parse_caption(caption: str | None):
    """Parses a caption using an AI model to extract structured car data."""
    if not caption or not ai_clients:
        log("AI client not available or empty caption. Skipping AI parse.")
        log(caption)
        return None

    schema = {
        "type": "object",
        "properties": {
            "is_for_sale_post": {"type": "boolean", "description": "Set to True if this is an advertisement for selling a vehicle, otherwise False."},
            "make": {"type": ["string", "null"], "description": "The brand of the car, e.g., 'Toyota'"},
            "model": {"type": ["string", "null"], "description": "The model of the car, e.g., 'Corolla'"},
            "year": {"type": ["string", "null"], "description": "The manufacturing year as a 4-digit string"},
            "mileage": {"type": ["string", "null"], "description": "The mileage or distance driven, as a numeric string without commas or units"},
            "transmission": {"type": ["string", "null"], "description": "The transmission type, e.g., 'Automatic' or 'Manual'"},
            "price": {
                "type": ["object", "null"],
                "description": "The price of the car. If one price is mentioned, use 'total'.",
                "properties": {
                    "total": {"type": ["string", "null"], "description": "The total price of the car."},
                    "cash": {"type": ["string", "null"], "description": "The required cash/down-payment amount."},
                    "bank": {"type": ["string", "null"], "description": "The amount to be financed by a bank."},
                    "monthly": {"type": ["string", "null"], "description": "The monthly payment amount, if specified."}
                }
            },
            "plate_number": {"type": ["string", "null"], "description": "The license plate number, which might be partially masked (e.g., '2C5XXX**')."},
            "condition": {"type": ["string", "null"], "description": "The condition of the car, e.g., 'Used', 'New', 'Excellent'."},
            "fuel_type": {"type": ["string", "null"], "description": "The type of fuel the car uses, e.g., 'Benzine', 'Diesel', 'Electric'."},
            "cc": {"type": ["string", "null"], "description": "The engine displacement in cubic centimeters (CC), as a numeric string."}, 
            "seats": {"type": ["string", "null"], "description": "The number of seats in the car."},
            "engine": {"type": ["string", "null"], "description": "Information about the engine, e.g., 'V8', '4-cylinder'."},
            "battery_capacity": {"type": ["string", "null"], "description": "For electric cars, the battery capacity in kWh, as a numeric string."},
            "driver_type": {"type": ["string", "null"], "description": "The drive type, e.g., 'Left-Hand Drive', 'FWD', 'AWD'."},
            "top_speed": {"type": ["string", "null"], "description": "The top speed of the car, e.g., '200 km/h'."},
            "color": {"type": ["string", "null"], "description": "The color of the car."},
            "body_type": {"type": ["string", "null"], "description": "The body type of the car, e.g., 'SUV', 'Sedan'."},
            "additional_details": {
                "type": ["array", "null"], 
                "items": {"type": "string"},
                "description": "A list of any other relevant details about the car not covered by other fields (e.g., 'Full option', 'Slightly negotiable')."
            }
        },
        "required": [
            "is_for_sale_post", "make", "model", "year", "mileage", "transmission", "price",
            "plate_number", "condition", "fuel_type", "cc", "seats", "engine",
            "battery_capacity", "driver_type", "top_speed", "color", "body_type",
            "additional_details"
        ],
    }
    
    prompt = f"""You are an expert car advertisement parser.
    Your primary goal is to determine if the ad is for selling a car and then extract structured details into a single valid JSON object.

    ### Car Ad Text
    "{caption}"

    ### Expected JSON Schema
    {json.dumps(schema, indent=2)}

    ### Extraction Rules
    1.  First, determine if the text is an ad to **sell a vehicle**. Set `is_for_sale_post` to `True` or `False`.
    2.  **If `is_for_sale_post` is `False`**, you **must** return `null` for all other fields. Do not extract any data.
    3.  If `is_for_sale_post` is `True`, extract all other details.
    4.  Your response must be **only one JSON object** with no extra text or explanations.
    5.  Always include **all fields** in the JSON output, even if their value is `null`.
    6.  **price**: 
        - Extract all price components (total, cash, bank, monthly) into the `price` object.
        - **Crucial:** If `total` price is not explicitly mentioned but `cash` and `bank` amounts are, you **must** calculate their sum and use that for the `total` field.
        - Do not mistake the `cash` amount for the `total` price when a `bank` amount is also present.
        - If only a single price is mentioned for the car, use that for the `total` field.
        - Extract only numbers. Remove all commas, currency symbols, and text like 'birr'.
    7.  **mileage**: Extract only numbers. Remove commas and units like 'km'.
    8.  **additional_details**: Use this for any other relevant information about the car itself, don't mention any commission info.
    9.  **transmission**: Standardize to 'Automatic' or 'Manual'.
    """
    
    try:
        client = random.choice(ai_clients)
        log("Sending caption to OpenRouter for parsing...")
        completion = await client.chat.completions.create(
            model=AI_MODEL_NAME,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "You are a strict car ad parser that only returns a single, valid JSON object based on the user's request. Your first task is to decide if the ad is for selling a car. If not, you must return null for all fields except 'is_for_sale_post'."},
                {"role": "user", "content": prompt}
            ]
        )
        response_content = completion.choices[0].message.content
        log(f"Received from OpenRouter: {response_content}")
        return json.loads(response_content)
    except Exception as e:
        log(f"Error calling OpenRouter or parsing response: {e}")
        return None

async def fetch_next_msg(client, msg):
    # Gets the single message that came right before this one in the chat history.
    async for message in client.get_chat_history(msg.chat.id, offset_id=msg.id, limit=1):
        return message
    return None

def should_attach_caption(photo_msg, text_msg):
    if not text_msg.forward_date:
        return True
    if text_msg.forward_date and not photo_msg.forward_date:
        return False
    # Both forwarded
    if (
        photo_msg.forward_from_chat 
        and text_msg.forward_from_chat 
        and photo_msg.forward_from_chat.id == text_msg.forward_from_chat.id
    ):
        return True
    if (
        photo_msg.forward_from 
        and text_msg.forward_from 
        and photo_msg.forward_from.id == text_msg.forward_from.id
    ):
        return True
    return False

def format_caption_from_json(parsed_data: dict) -> str:
    """Formats the parsed JSON data into a human-readable Telegram caption."""
    caption_parts = []

    # This check is a safeguard; run_scraper should prevent invalid data from reaching here.
    if not parsed_data or not parsed_data.get("is_for_sale_post"):
        log("format_caption_from_json called with invalid data, returning empty.")
        return ""

    field_map = {
        "make": "🚗 **Make**",
        "model": "🚘 **Model**",
        "year": "📅 **Year**",
        "body_type": "🚙 **Body Type**",
        "color": "🎨 **Color**",
        "mileage": "🛣️ **Mileage**",
        "transmission": "🕹️ **Transmission**",
        "fuel_type": "⛽ **Fuel**",
        "engine": "⚙️ **Engine**",
        "cc": "🔌 **CC**",
        "battery_capacity": "🔋 **Battery**",
        "top_speed": "⚡ **Top Speed**",
        "seats": "👥 **Seats**",
        "driver_type": "👤 **Driver Side**",
        "condition": "✨ **Condition**",
        "plate_number": "🆔 **Plate**",
    }

    # --- Main Details Formatting ---
    for key, display_name in field_map.items():
        value = parsed_data.get(key)
        if value and str(value).strip():
            if key == 'mileage':
                try:
                    value = f"{int(str(value).replace(',', '')):,} km"
                except (ValueError, TypeError): pass
            caption_parts.append(f"{display_name}: {value}")

    # --- Custom Price Formatting ---
    price_data = parsed_data.get("price")
    if isinstance(price_data, dict):
        total_price_label = "💰 **Price**"
        if price_data.get("bank") and str(price_data.get("bank")).strip():
            total_price_label = "💰 **Total Price**"
            
        price_map = {
            "total": total_price_label,
            "cash": "💵 **Cash**",
            "bank": "🏦 **Bank**",
            "monthly": "🗓️ **Monthly**",
        }
        for key, display_name in price_map.items():
            value = price_data.get(key)
            if value and str(value).strip():
                try:
                    # Format numbers with commas, but handle potential non-numeric strings
                    value = f"{int(float(str(value).replace(',', ''))):,}"
                except (ValueError, TypeError):
                    pass # Keep original value if it's not a number
                caption_parts.append(f"{display_name}: {value}")

    # --- Custom Additional Details Formatting ---
    additional_details = parsed_data.get("additional_details")
    if additional_details:
        caption_parts.append("\n📝 **Additional Info**")
        for detail in additional_details:
            caption_parts.append(f" - {escape_markdown(detail)}")
    
    return "\n".join(caption_parts)
import psycopg2
from psycopg2.extras import RealDictCursor

def get_db_connection():
    conn = psycopg2.connect(DATABASE_URL)
    return conn

def update_channel_state(channel_username: str, last_processed_id: int) -> None:
    """Upserts the last processed message ID for a channel."""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO channel_state (channel_id, last_processed_id) VALUES (%s, %s) ON CONFLICT (channel_id) DO UPDATE SET last_processed_id = %s",
                (channel_username, last_processed_id, last_processed_id)
            )
        conn.commit()

def init_db():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS my_schema;")
            cursor.execute("SET search_path TO my_schema")
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS cars (
                id TEXT PRIMARY KEY,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                source_chat_id TEXT NOT NULL,
                source_message_id BIGINT NOT NULL,
                caption_raw TEXT,
                parsed_data JSONB,
                images TEXT[],
                my_channel_id TEXT,
                my_message_id BIGINT
            );
            """)

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS channel_state (
                channel_id TEXT PRIMARY KEY,
                last_processed_id BIGINT NOT NULL
            );
            """)

            # New table for source channels
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS source_channels (
                username TEXT PRIMARY KEY NOT NULL
            );
            """)

            # Migrate initial channels if the table is empty
            cursor.execute("SELECT COUNT(*) FROM source_channels")
            if cursor.fetchone()[0] == 0:
                initial_channels = ["Mikycarboss", "Golden_car_market"]
                for channel in initial_channels:
                    cursor.execute("INSERT INTO source_channels (username) VALUES (%s) ON CONFLICT (username) DO NOTHING", (channel,))
                log(f"Initialized source_channels table with: {', '.join(initial_channels)}")
        
        conn.commit()

# Initialize the database
init_db()


# ---------- CHANNEL MANAGEMENT ----------
def get_source_channels():
    """Fetches the list of source channel usernames from the database."""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT username FROM source_channels")
            return [row[0] for row in cursor.fetchall()]

# @u_app.on_message(filters.command("cha") & filters.private & is_admin)
async def list_channels_command(client, message):
    """Handles the /channels command to list all source channels."""
    log("Received /channels command.")
    channels = get_source_channels()
    if not channels:
        await message.reply_text("There are no source channels configured.")
        return
    
    response = "**Current Source Channels:**\n" + "\n".join(f"- `{channel}`" for channel in channels)
    await message.reply_text(response)

# @u_app.on_message(filters.command("addc") & filters.private & is_admin)
async def add_channel_command(client, message):
    """Handles the /addchannel command to add a new source channel."""
    log(f"Received /addchannel command: {message.text}")
    parts = message.text.split()
    if len(parts) < 2:
        await message.reply_text("**Usage:** `/addc <username>`\n\n*Example:* `/addc some_channel_name`")
        return
    
    channel_username = parts[1].lstrip('@')
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("INSERT INTO source_channels (username) VALUES (%s)", (channel_username,))
            conn.commit()
        log(f"Added new source channel: {channel_username}")
        await message.reply_text(f"✅ Successfully added channel: `{channel_username}`")
    except psycopg2.errors.UniqueViolation:
        log(f"Attempted to add duplicate channel: {channel_username}")
        await message.reply_text(f"⚠️ Channel `{channel_username}` is already in the list.")
    except Exception as e:
        log(f"Error adding channel {channel_username}: {e}")
        await message.reply_text(f"❌ An error occurred while adding the channel: `{e}`")

# @u_app.on_message(filters.command("delc") & filters.private & is_admin)
async def del_channel_command(client, message):
    """Handles the /delchannel command to remove a source channel."""
    log(f"Received /delchannel command: {message.text}")
    parts = message.text.split()
    if len(parts) < 2:
        await message.reply_text("**Usage:** `/delc <username>`\n\n*Example:* `/delc some_channel_name`")
        return
        
    channel_username = parts[1].lstrip('@')
    
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            # Check if the channel exists before deleting
            cursor.execute("SELECT username FROM source_channels WHERE username = %s", (channel_username,))
            if cursor.fetchone() is None:
                await message.reply_text(f"⚠️ Channel `{channel_username}` not found in the list.")
                return

            try:
                cursor.execute("DELETE FROM source_channels WHERE username = %s", (channel_username,))
                conn.commit()
                log(f"Removed source channel: {channel_username}")
                await message.reply_text(f"✅ Successfully removed channel: `{channel_username}`")
            except Exception as e:
                log(f"Error removing channel {channel_username}: {e}")
                await message.reply_text(f"❌ An error occurred while removing the channel: `{e}`")

# ---------- BOT ----------
async def send_bot_message(chat_id: int, text: str, **kwargs):
    """Sends a message using the Telegram Bot API."""
    payload = {'chat_id': chat_id, 'text': text, 'parse_mode': 'Markdown', **kwargs}
    try:
        r = await http_client.post(f"{BOT_API_URL}/sendMessage", json=payload, timeout=20)
        r.raise_for_status()
        log(f"Sent bot message to {chat_id}.")
        return r.json()
    except httpx.HTTPStatusError as e:
        log(f"Error sending bot message to {chat_id}: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        log(f"Error in send_bot_message: {e}")
    return None

async def handle_bot_command(message: dict):
    """Parses a message and executes the corresponding bot command."""
    text = message.get("text", "")
    chat_id = message["chat"].get("id")
    
    if not text.startswith('/'):
        return # Not a command

    command, *args = text.split()
    command = command[1:] # remove "/"
    
    log(f"Received command '{command}' from chat {chat_id}")

    if command == "start":
        await send_bot_message(chat_id, "Hello! I am the Car Scraper Bot. Use /s to start scraping.")
    
    elif command == "s":
        notify = lambda msg: asyncio.create_task(send_bot_message(chat_id, msg))
        await send_bot_message(chat_id, "Scraping process initiated...")
        async with Client(SESSION_NAME, api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING) as u_app:
            await run_scraper(u_app, notify=notify)

    elif command == "cha":
        channels = get_source_channels()
        if not channels:
            response = "There are no source channels configured."
        else:
            response = "**Current Source Channels:**\n" + "\n".join(f"- `{channel}`" for channel in channels)
        await send_bot_message(chat_id, response)

    elif command == "addc":
        if not args:
            await send_bot_message(chat_id, "**Usage:** `/addc <username>`\n\n*Example:* `/addc some_channel_name`")
            return
        
        channel_username = args[0].lstrip('@')
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("INSERT INTO source_channels (username) VALUES (%s)", (channel_username,))
                conn.commit()
            log(f"Added new source channel: {channel_username}")
            await send_bot_message(chat_id, f"✅ Successfully added channel: `{channel_username}`")
        except psycopg2.errors.UniqueViolation:
            log(f"Attempted to add duplicate channel: {channel_username}")
            await send_bot_message(chat_id, f"⚠️ Channel `{channel_username}` is already in the list.")
        except Exception as e:
            log(f"Error adding channel {channel_username}: {e}")
            await send_bot_message(chat_id, f"❌ An error occurred while adding the channel: `{e}`")

    elif command == "delc":
        if not args:
            await send_bot_message(chat_id, "**Usage:** `/delc <username>`\n\n*Example:* `/delc some_channel_name`")
            return
            
        channel_username = args[0].lstrip('@')
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT username FROM source_channels WHERE username = %s", (channel_username,))
                if cursor.fetchone() is None:
                    await send_bot_message(chat_id, f"⚠️ Channel `{channel_username}` not found in the list.")
                    return

                try:
                    cursor.execute("DELETE FROM source_channels WHERE username = %s", (channel_username,))
                    conn.commit()
                    log(f"Removed source channel: {channel_username}")
                    await send_bot_message(chat_id, f"✅ Successfully removed channel: `{channel_username}`")
                except Exception as e:
                    log(f"Error removing channel {channel_username}: {e}")
                    await send_bot_message(chat_id, f"❌ An error occurred while removing the channel: `{e}`")

def escape_markdown(text: str) -> str:
    """Escapes special characters for Telegram Markdown parsing."""
    if not text:
        return ""
    # Chars to escape are: *, _, `, [
    escape_chars = r'([_*`\[])'
    return re.sub(escape_chars, r'\\\1', text)

async def handle_bot_forward(message: dict):
    """Handles forwarded posts to get car info by ID."""
    chat_id = message["chat"]["id"]
    caption = message.get("caption", message.get("text"))
    if not caption:
        return

    log(f"Received a forwarded message from {chat_id}. Checking for Car ID...")

    match = re.search(r"ID:\s*([a-zA-Z0-9]+)", caption, re.MULTILINE)
    if not match:
        log("No Car ID found in the forwarded message.")
        return

    car_id = match.group(1)
    log(f"Found Car ID: {car_id}. Querying database...")

    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT parsed_data, caption_raw, source_chat_id, source_message_id, created_at FROM cars WHERE id = %s", (car_id,))
            car_row = cursor.fetchone()

    if not car_row:
        log(f"Car ID {car_id} not found in the database.")
        await send_bot_message(chat_id, f"Sorry, I couldn't find any information for Car ID: `{car_id}`")
        return

    parsed_data = car_row['parsed_data']
    raw_caption = car_row['caption_raw']
    source_chat_id = car_row['source_chat_id']
    source_message_id = car_row['source_message_id']
    created_at = car_row['created_at']

    reply_parts = [f"Found info for **Car ID:** `{car_id}`\n"]

    if not parsed_data:
        reply_parts.append(f"No structured details available. Original info:\n\n{escape_markdown(raw_caption or 'N/A')}")
    else:
        field_map = {
            "make": "🚗 **Make**", "model": "🚘 **Model**", "year": "📅 **Year**",
            "body_type": "🚙 **Body Type**", "color": "🎨 **Color**", "mileage": "🛣️ **Mileage**",
            "transmission": "🕹️ **Transmission**", "fuel_type": "⛽ **Fuel**", "engine": "⚙️ **Engine**",
            "cc": "🔌 **CC**", "battery_capacity": "🔋 **Battery**", "top_speed": "⚡ **Top Speed**",
            "seats": "👥 **Seats**", "driver_type": "👤 **Driver Side**", "condition": "✨ **Condition**",
            "plate_number": "🆔 **Plate**",
        }
        for key, display_name in field_map.items():
            value = parsed_data.get(key)
            if value and str(value).strip():
                value = escape_markdown(str(value))
                if key == 'mileage':
                    try: value = f"{int(str(value).replace(',', '')):,} km"
                    except (ValueError, TypeError): pass
                reply_parts.append(f"{display_name}: {value}")

        # Price formatting
        price_data = parsed_data.get('price')
        if isinstance(price_data, dict):
            price_parts = []
            if price_data.get('total'): 
                try: price_parts.append(f"💰 **Price**: {int(float(str(price_data['total']).replace(',', ''))):,}")
                except (ValueError, TypeError): price_parts.append(f"💰 **Price**: {escape_markdown(str(price_data['total']))}")
            if price_data.get('cash'): 
                try: price_parts.append(f"💵 **Cash**: {int(float(str(price_data['cash']).replace(',', ''))):,}")
                except (ValueError, TypeError): price_parts.append(f"💵 **Cash**: {escape_markdown(str(price_data['cash']))}")
            if price_data.get('bank'): 
                try: price_parts.append(f"🏦 **Bank**: {int(float(str(price_data['bank']).replace(',', ''))):,}")
                except (ValueError, TypeError): price_parts.append(f"🏦 **Bank**: {escape_markdown(str(price_data['bank']))}")
            if price_data.get('monthly'): 
                try: price_parts.append(f"🗓️ **Monthly**: {int(float(str(price_data['monthly']).replace(',', ''))):,}")
                except (ValueError, TypeError): price_parts.append(f"🗓️ **Monthly**: {escape_markdown(str(price_data['monthly']))}")
            if price_parts: reply_parts.extend(price_parts)
        elif price_data and str(price_data).strip(): # Old string price format
            try:
                formatted_price = f"{int(str(price_data).replace(',', '')):,}"
            except (ValueError, TypeError):
                formatted_price = escape_markdown(str(price_data))
            reply_parts.append(f"💰 **Price**: {formatted_price}")

        # Additional details formatting
        additional_details = parsed_data.get("additional_details")
        if additional_details:
            reply_parts.append("\n📝 **Additional Info**:")
            for detail in additional_details:
                reply_parts.append(f" - {escape_markdown(detail)}")

    source_link = ""
    chat_identifier = str(source_chat_id)
    if isinstance(source_chat_id, int) and str(source_chat_id).startswith('-100'):
        chat_identifier = str(source_chat_id)[4:]
        source_link = f"https://t.me/c/{chat_identifier}/{source_message_id}"
    else:
        chat_identifier = chat_identifier.lstrip('@')
        source_link = f"https://t.me/{chat_identifier}/{source_message_id}"

    reply_parts.append("\n" + "─" * 15)
    reply_parts.append(f"ℹ️ **Source:** [Original Post]({source_link})")
    reply_parts.append(f"🕒 **Scraped At:** `{created_at}`")

    await send_bot_message(chat_id, "\n".join(reply_parts), disable_web_page_preview=True)

async def run_scraper(client, notify=None):
    global is_scraping
    if is_scraping:
        log("Scraping is already in progress.")
        if notify:
            await notify("Scraping is already in progress.")
        return

    is_scraping = True
    
    admin_notify = lambda text: notify(text) if notify else asyncio.sleep(0)

    try:
        log("Scraping started for all channels...")
        await admin_notify("Scraping started for all channels...")

        source_channels = get_source_channels()
        if not source_channels:
            log("No source channels found in the database. Aborting scrape.")
            await admin_notify("⚠️ No source channels configured. Add some with `/addc`.")
            return

        for channel_username in source_channels:
            log(f"\n--- Processing channel: {channel_username} ---")

            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # Get last processed ID for this channel
                    cursor.execute("SELECT last_processed_id FROM channel_state WHERE channel_id = %s", (channel_username,))
                    result = cursor.fetchone()
                    last_processed_id = result[0] if result else 0
            log(f"Will process messages newer than ID: {last_processed_id} for channel {channel_username}")

            # Fetch new messages since the last run for this specific channel
            is_first_run_channel = last_processed_id == 0
            history_limit_channel = 100 if is_first_run_channel else 0 

            new_messages = []
            try:
                async for msg in client.get_chat_history(channel_username, limit=history_limit_channel):
                    if not is_first_run_channel and msg.id <= last_processed_id:
                        log(f"Reached message {msg.id}, which was already processed for {channel_username}. Halting message collection.")
                        break
                    new_messages.append(msg)
            except Exception as e:
                log(f"Error fetching history for {channel_username}: {e}. Skipping this channel.")
                await admin_notify(f"❌ Could not access channel `{channel_username}`. Is the username correct and is the bot a member? Error: {e}")
                continue

            if not new_messages:
                log(f"No new messages found for {channel_username}.")
                continue # Move to next channel

            log(f"Found {len(new_messages)} new messages to process for {channel_username}.")
            
            messages = list(reversed(new_messages)) # Oldest to newest
            message_index_map = {msg.id: i for i, msg in enumerate(messages)}
            processed_message_ids = set()

            i = 0
            while i < len(messages):
                msg = messages[i]
                
                if msg.id in processed_message_ids:
                    i += 1
                    continue

                processed_ids_this = set()
                log(f"Processing message {i+1}/{len(messages)} in {channel_username}, ID: {msg.id}...")

                images = []
                caption_text = None
                message_to_process = msg
                is_car_post = (msg.photo and not msg.media_group_id) or msg.media_group_id

                if not is_car_post:
                    log(f"Skipping message {msg.id} as it is not a car post in {channel_username}.")
                    processed_ids_this.add(msg.id)
                    update_channel_state(channel_username, msg.id)
                    i += 1
                    continue
                
                if msg.media_group_id:
                    try:
                        group_msgs = await client.get_media_group(msg.chat.id, msg.id)
                        message_to_process = sorted(group_msgs, key=lambda m: m.id)[-1]
                        for m in group_msgs:
                            if m.photo:
                                images.append(m.photo.file_id)
                            if m.caption:
                                caption_text = m.caption
                            processed_message_ids.add(m.id)
                            processed_ids_this.add(m.id)
                    except Exception as e:
                        log(f"Error getting media group for msg {msg.id} in {channel_username}: {e}")
                        i += 1
                        continue
                else: # Single photo post
                    images.append(msg.photo.file_id)
                    caption_text = msg.caption
                    processed_message_ids.add(msg.id)
                    processed_ids_this.add(msg.id)

                if not caption_text:
                    last_msg_index = message_index_map.get(message_to_process.id)
                    if last_msg_index is not None:
                        next_msg_index = last_msg_index + 1
                        if next_msg_index < len(messages):
                            next_msg = messages[next_msg_index]
                            time_diff = (next_msg.date - message_to_process.date).total_seconds()
                            is_text = next_msg.text and not next_msg.photo
                            if is_text and 0 <= time_diff < 300:
                                log(f"Found caption for post {message_to_process.id} in subsequent message {next_msg.id} for {channel_username}.")
                                caption_text = next_msg.text
                                processed_message_ids.add(next_msg.id)
                                processed_ids_this.add(next_msg.id)

                parsed = await parse_caption(caption_text)

                if not parsed or not parsed.get("is_for_sale_post"):
                    log(f"Skipping post {message_to_process.id} from {channel_username} as it was not a 'for sale' post. Forwarding to admin.")
                    try:
                        await client.forward_messages(
                            chat_id=ADMIN_ID,
                            from_chat_id=message_to_process.chat.id,
                            message_ids=message_to_process.id
                        )
                    except Exception as e:
                        log(f"Failed to forward non-sale post {message_to_process.id} to admin: {e}")
                    if processed_ids_this:
                        update_channel_state(channel_username, max(processed_ids_this))
                    i += 1
                    continue

                car_id = generate_car_id()
                details_caption = format_caption_from_json(parsed)
                
                final_caption = f"{details_caption}{CONTACT_INFO}\n\n**ID:** `{car_id}`"

                log(f"Reposting Car ID {car_id} from {channel_username} with {len(images)} images...")

                media_group = []
                for j, file_id in enumerate(images):
                    if j == 0:
                        media_group.append(InputMediaPhoto(media=file_id, caption=final_caption))
                    else:
                        media_group.append(InputMediaPhoto(media=file_id))

                if not media_group:
                    log(f"Skipping Car ID {car_id}: no media to send from {channel_username}.")
                    i += 1
                    continue
                    
                try:
                    sent_messages = await client.send_media_group(chat_id=MY_CHANNEL_ID, media=media_group)
                    my_msg_id = sent_messages[0].id
                except Exception as e:
                    log(f"Failed to send media group for Car ID {car_id} from {channel_username}: {e}")
                    i += 1
                    continue

                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            """
                            INSERT INTO cars 
                            (id, created_at, source_chat_id, source_message_id, caption_raw, parsed_data, images, my_channel_id, my_message_id)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                car_id,
                                datetime.now(), # Store current timestamp for when it was scraped
                                channel_username,
                                message_to_process.id,
                                caption_text,
                                json.dumps(parsed),
                                images,
                                MY_CHANNEL_ID,
                                my_msg_id
                            )
                        )
                    conn.commit()
                if processed_ids_this:
                    update_channel_state(channel_username, max(processed_ids_this))
                log(f"Inserted Car ID {car_id} into DB for source message {message_to_process.id} from {channel_username}. Updated state.")
                i += 1
            
        log("--- Scraping finished for all channels. ---")
        await admin_notify("Scraping finished for all channels.")
    finally:
        is_scraping = False

# ---------- HTTP (BOT) TRIGGER ----------
app = FastAPI()

@app.post("/telegram")
async def telegram_webhook(request: Request):
    """Endpoint to receive updates from the Telegram Bot API."""
    data = await request.json()
    log(f"Received update from Telegram: {data}")
    
    # Ensure there's a message and it's from the admin
    message = data.get("message")
    print("-------------------------------")
    print(message.get("from", {}).get("id"), ADMIN_ID, ADMIN_ID == message.get("from", {}).get("id"))

    # Route to appropriate handler
    if "forward_date" in message or "forward_from" in message:
        await handle_bot_forward(message)
    elif "text" in message:
        if not message or message.get("from", {}).get("id") != ADMIN_ID:
            log(f"Update rejected: Not a message or not from admin. From ID: {message.get('from', {}).get('id')}")
            return {"status": "unauthorized"}
        await handle_bot_command(message)

    return {"status": "ok"}


@app.post("/run-scraper")
async def run_scraper_trigger(request: Request):
    secret = request.headers.get("X-CRON-SECRET")
    if secret != os.getenv("CRON_SECRET"):
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        # Run the scraper in the background
        # asyncio.create_task(run_scraper_with_client())
        await run_scraper_with_client()
        return {"status": "scraper task started"}
    except Exception as e:
        log(f"An error occurred during scraper run: {e}")
        log(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

async def run_scraper_with_client():
    """Initializes a Pyrogram client and runs the scraper."""
    # The scraper needs a client to run, but we don't want to notify anyone
    # in this context as it's triggered by a cron job.
    async with Client(SESSION_NAME, api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING) as u_app:
        await run_scraper(u_app, notify=None)
    

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Car Info</title>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap');
        
        :root {
            --primary-color: #007bff;
            --primary-hover: #0056b3;
            --background-color: #f8f9fa;
            --card-background: #ffffff;
            --text-color: #333;
            --muted-text-color: #6c757d;
            --border-color: #dee2e6;
            --shadow: 0 4px 6px rgba(0,0,0,0.1);
        }

        body {
            font-family: 'Inter', sans-serif;
            margin: 0;
            background-color: var(--background-color);
            color: var(--text-color);
            padding: 20px;
        }

        .container {
            max-width: 700px;
            margin: auto;
        }

        h1 {
            color: var(--primary-color);
            text-align: center;
            font-weight: 600;
            margin-bottom: 2rem;
        }

        .search-form {
            display: flex;
            margin-bottom: 2rem;
            box-shadow: var(--shadow);
            border-radius: 8px;
            overflow: hidden;
        }

        .search-form input[type="text"] {
            flex-grow: 1;
            padding: 12px 15px;
            border: 1px solid var(--border-color);
            border-right: none;
            font-size: 16px;
        }
        .search-form input[type="text"]:focus {
            outline: 2px solid var(--primary-color);
        }

        .search-form button {
            padding: 12px 20px;
            background-color: var(--primary-color);
            color: white;
            border: none;
            cursor: pointer;
            font-size: 16px;
            transition: background-color 0.2s;
        }

        .search-form button:hover {
            background-color: var(--primary-hover);
        }
        
        .car-card {
            background: var(--card-background);
            border-radius: 12px;
            box-shadow: var(--shadow);
            overflow: hidden;
        }

        .car-header {
            background-color: var(--primary-color);
            color: white;
            padding: 1.5rem;
            text-align: center;
        }
        .car-header h2 {
            margin: 0;
            font-size: 1.5rem;
        }
        .car-header small {
            font-size: 1rem;
            opacity: 0.8;
        }

        .car-body {
            padding: 1.5rem;
        }
        
        .detail-item {
            padding: 10px;
            border-radius: 6px;
            background-color: #f1f3f5;
        }
        .detail-item strong {
            display: block;
            color: var(--muted-text-color);
            font-size: 0.8rem;
            margin-bottom: 4px;
        }
        
        .price-section p {
            font-size: 1.2rem;
            font-weight: 600;
            margin: 0 0 1rem 0;
        }
        .price-section strong {
           color: var(--primary-color);
        }
        
        .additional-info ul {
            padding-left: 20px;
            margin: 0;
        }
        .additional-info li {
            margin-bottom: 0.5rem;
        }

        .source-link-container {
            text-align: center;
            padding-top: 1.5rem;
            border-top: 1px solid var(--border-color);
            margin-top: 1.5rem;
        }

        .source-link-button {
            display: inline-block;
            background-color: var(--primary-color);
            color: white;
            padding: 12px 25px;
            border-radius: 8px;
            text-decoration: none;
            font-weight: 500;
            transition: background-color 0.2s;
        }
        .source-link-button:hover {
            background-color: var(--primary-hover);
        }

        .not-found, .error {
            background-color: #fff3cd;
            padding: 1rem;
            border-radius: 8px;
            text-align: center;
            color: #664d03;
            border: 1px solid #ffecb5;
        }

    </style>
</head>
<body>
    <div class="container">
        <h1>Car Intel</h1>
        <form action="/car" method="get" class="search-form">
            <input type="text" name="car_id" placeholder="Enter Car ID (e.g., C13s)" value="{car_id_value}">
            <button type="submit">Search</button>
        </form>
        {results}
    </div>
</body>
</html>
"""

@app.get("/car", response_class=HTMLResponse)
async def get_car_by_id_page(request: Request, car_id: str | None = None):
    results_html = ""
    car_id_value = car_id if car_id else ""

    if car_id:
        try:
            with get_db_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("SELECT * FROM my_schema.cars WHERE id = %s", (car_id,))
                    car_row = cursor.fetchone()

            if car_row:
                parsed_data = car_row.get('parsed_data', {})
                
                # --- Extract and Sanitize Data ---
                make = parsed_data.get('make')
                model = parsed_data.get('model')
                year = parsed_data.get('year')
                headline = escape_markdown(', '.join(filter(None, [make, model, year])))

                car_id_display = escape_markdown(car_row.get('id', ''))
                
                # --- Price Formatting ---
                price_lines = []
                price_data = parsed_data.get('price')
                if isinstance(price_data, dict):
                    if total := price_data.get('total'):
                        try:
                            price_lines.append(f"<strong>Price:</strong> ETB {int(float(str(total).replace(',', ''))):,}")
                        except (ValueError, TypeError):
                            price_lines.append(f"<strong>Price:</strong> ETB {escape_markdown(str(total))}")
                    if cash := price_data.get('cash'): 
                        price_lines.append(f"<strong>Cash:</strong> ETB {escape_markdown(str(cash))}")
                    if bank := price_data.get('bank'): 
                        price_lines.append(f"<strong>Bank:</strong> ETB {escape_markdown(str(bank))}")
                    if monthly := price_data.get('monthly'): 
                        price_lines.append(f"<strong>Monthly:</strong> ETB {escape_markdown(str(monthly))}")
                elif price_data: # Handle old string format
                    try:
                        price_lines.append(f"<strong>Price:</strong> ETB {int(float(str(price_data).replace(',', ''))):,}")
                    except (ValueError, TypeError):
                        price_lines.append(f"<strong>Price:</strong> ETB {escape_markdown(str(price_data))}")
                
                if not price_lines:
                    price_lines.append("<strong>Price:</strong> Not Available")

                # --- Other Details ---
                details_lines = []
                field_map = {
                    "make": "Make", "model": "Model", "year": "Year",
                    "body_type": "Body Type", "color": "Color", "mileage": "Mileage",
                    "transmission": "Transmission", "fuel_type": "Fuel", "engine": "Engine",
                    "cc": "CC", "battery_capacity": "Battery", "top_speed": "Top Speed",
                    "seats": "Seats", "driver_type": "Driver Side", "condition": "Condition",
                    "plate_number": "Plate",
                }
                for key, display_name in field_map.items():
                    value = parsed_data.get(key)
                    if value and str(value).strip():
                        formatted_value = escape_markdown(str(value))
                        if key == 'mileage':
                            try:
                                formatted_value = f"{int(str(value).replace(',', '')):,} km"
                            except (ValueError, TypeError):
                                pass
                        details_lines.append(f"<strong>{display_name}:</strong> {formatted_value}")
                
                # --- Additional Details ---
                additional_details_html = ""
                additional_details = parsed_data.get("additional_details")
                if additional_details:
                    additional_details_html = "<p><strong>Additional Info:</strong></p><ul>"
                    for detail in additional_details:
                        additional_details_html += f"<li>{escape_markdown(detail)}</li>"
                    additional_details_html += "</ul>"

                # --- Source Link ---
                source_link_url = "#"
                source_chat_id = car_row.get('source_chat_id')
                source_message_id = car_row.get('source_message_id')
                if source_chat_id and source_message_id:
                    chat_identifier = str(source_chat_id).lstrip('@')
                    if isinstance(source_chat_id, int) and str(source_chat_id).startswith('-100'):
                        chat_identifier = str(source_chat_id)[4:]
                        source_link_url = f"https://t.me/c/{chat_identifier}/{source_message_id}"
                    else:
                        source_link_url = f"https://t.me/{chat_identifier}/{source_message_id}"


                # --- Build HTML ---
                results_html = f'''
                <div class="car-card">
                    <div class="car-header">
                        <h2>{headline or "Vehicle Information"}</h2>
                        <small>ID: {car_id_display}</small>
                    </div>
                    <div class="car-body">
                        <div class="price-section">
                            {''.join(f"<p>{line}</p>" for line in price_lines)}
                        </div>
                        
                        {additional_details_html}
                        
                        <div class="source-link-container">
                            <a href="{source_link_url}" class="source-link-button">View Original Listing</a>
                        </div>
                    </div>
                </div>
                '''
            else:
                results_html = f"<div class='not-found'><p>No vehicle found with ID: <strong>{escape_markdown(car_id)}</strong></p></div>"

        except Exception as e:
            log(f"Error during car search for ID {car_id}: {e}")
            results_html = f"<div class='error'><p>An error occurred while searching. Please check server logs for details.</p></div>"

    # Use a sanitizer for the values inserted into the template
    safe_car_id_value = escape_markdown(car_id_value)
    # Correctly escape braces for the final format call
    final_html = HTML_TEMPLATE.replace('{', '{{').replace('}', '}}').replace('{{results}}', '{results}').replace('{{car_id_value}}', '{car_id_value}')
    return final_html.format(results=results_html, car_id_value=safe_car_id_value)


@app.get("/")
async def home(request: Request):
    return {"status": "Working :D"}

# ---------- RUN ----------
async def setup_bot():
    """Sets the Telegram webhook."""
    # TODO: Need to get the public URL for the webhook
    webhook_url = os.getenv("WEBHOOK_URL") 
    if not webhook_url:
        log("WEBHOOK_URL environment variable not set. Skipping webhook setup.")
        return

    set_webhook_url = f"{BOT_API_URL}/setWebhook?url={webhook_url}/telegram"
    try:
        r = await http_client.get(set_webhook_url)
        r.raise_for_status()
        log(f"Webhook set successfully to {webhook_url}/telegram. Response: {r.json()}")
    except Exception as e:
        log(f"Error setting webhook: {e}")

async def main():
    # Set up the bot webhook
    await setup_bot()

    # Run FastAPI in a separate thread
    def run_fastapi():
        uvicorn.run(app, host="0.0.0.0", port=8080)

    threading.Thread(target=run_fastapi, daemon=True).start()
    
    log("FastAPI server is running.")
    
    # Keep the main coroutine running
    while True:
        await asyncio.sleep(3600) # Sleep for an hour, or any long duration

if __name__ == "__main__":
    if not all([API_ID, API_HASH, SESSION_STRING, BOT_TOKEN, ADMIN_ID, DATABASE_URL, MY_CHANNEL_ID]):
        log("FATAL: One or more required environment variables are missing.")
    else:
        if len(sys.argv) > 1 and sys.argv[1] == '--scrape':
            asyncio.run(run_scraper_with_client())
        else:
            asyncio.run(main())
        
        
