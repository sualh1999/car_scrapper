import asyncio
import re
import sys
import traceback
from pathlib import Path

import httpx
import psycopg2
import uvicorn
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from pyrogram import Client

from config import (
    ADMIN_ID,
    API_HASH,
    API_ID,
    BOT_API_URL,
    CRON_SECRET,
    SESSION_NAME,
    SESSION_STRING,
    WEBHOOK_URL,
    validate_required_settings,
)
from db import (
    add_source_channel,
    get_car_by_id,
    get_car_row_by_id,
    get_source_channels,
    init_db,
    remove_source_channel,
)
from scraper import run_scraper, run_scraper_with_client
from services import escape_markdown, log

http_client = httpx.AsyncClient()

init_db(log)

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
        add_source_channel(channel_username)
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
    try:
        if not remove_source_channel(channel_username):
            await message.reply_text(f"⚠️ Channel `{channel_username}` not found in the list.")
            return
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
            add_source_channel(channel_username)
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
        try:
            if not remove_source_channel(channel_username):
                await send_bot_message(chat_id, f"⚠️ Channel `{channel_username}` not found in the list.")
                return
            log(f"Removed source channel: {channel_username}")
            await send_bot_message(chat_id, f"✅ Successfully removed channel: `{channel_username}`")
        except Exception as e:
            log(f"Error removing channel {channel_username}: {e}")
            await send_bot_message(chat_id, f"❌ An error occurred while removing the channel: `{e}`")

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

    car_row = get_car_by_id(car_id)

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

@asynccontextmanager
async def lifespan(app: FastAPI):
    await setup_bot()
    try:
        yield
    finally:
        await http_client.aclose()

# ---------- HTTP (BOT) TRIGGER ----------
app = FastAPI(lifespan=lifespan)

@app.post("/telegram")
async def telegram_webhook(request: Request):
    """Endpoint to receive updates from the Telegram Bot API."""
    data = await request.json()
    log(f"Received update from Telegram: {data}")
    
    message = data.get("message")
    if not message:
        return {"status": "ignored"}

    sender_id = message.get("from", {}).get("id")
    if sender_id != ADMIN_ID:
        log(f"Update rejected: not from admin. From ID: {sender_id}")
        return {"status": "unauthorized"}

    # Route to appropriate handler
    if "forward_date" in message or "forward_from" in message:
        await handle_bot_forward(message)
    elif "text" in message:
        await handle_bot_command(message)

    return {"status": "ok"}


@app.post("/run-scraper")
async def run_scraper_trigger(request: Request):
    secret = request.headers.get("X-CRON-SECRET")
    if secret != CRON_SECRET:
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

HTML_TEMPLATE_PATH = Path(__file__).parent / "templates" / "car.html"
HTML_TEMPLATE = HTML_TEMPLATE_PATH.read_text(encoding="utf-8")

@app.get("/car", response_class=HTMLResponse)
async def get_car_by_id_page(request: Request, car_id: str | None = None):
    results_html = ""
    car_id_value = car_id if car_id else ""

    if car_id:
        try:
            car_row = get_car_row_by_id(car_id)

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

    safe_car_id_value = escape_markdown(car_id_value)
    return (
        HTML_TEMPLATE
        .replace("__RESULTS__", results_html)
        .replace("__CAR_ID_VALUE__", safe_car_id_value)
    )


@app.get("/")
async def home(request: Request):
    return {"status": "Working :D"}

# ---------- RUN ----------
async def setup_bot():
    """Sets the Telegram webhook."""
    # TODO: Need to get the public URL for the webhook
    if not WEBHOOK_URL:
        log("WEBHOOK_URL environment variable not set. Skipping webhook setup.")
        return

    set_webhook_url = f"{BOT_API_URL}/setWebhook?url={WEBHOOK_URL}/telegram"
    try:
        r = await http_client.get(set_webhook_url)
        r.raise_for_status()
        log(f"Webhook set successfully to {WEBHOOK_URL}/telegram. Response: {r.json()}")
    except Exception as e:
        log(f"Error setting webhook: {e}")

if __name__ == "__main__":
    missing = validate_required_settings()
    if missing:
        log(f"FATAL: Missing required environment variables: {', '.join(missing)}")
    else:
        if len(sys.argv) > 1 and sys.argv[1] == '--scrape':
            asyncio.run(run_scraper_with_client())
        else:
            uvicorn.run("main:app", host="0.0.0.0", port=8080)
        
        
