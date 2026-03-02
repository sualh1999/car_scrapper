import asyncio
import re

import httpx
import psycopg2
from pyrogram import Client

from config import (
    ADMIN_ID,
    API_HASH,
    API_ID,
    BOT_API_URL,
    SESSION_NAME,
    SESSION_STRING,
    WEBHOOK_URL,
)
from db import (
    add_source_channel,
    get_car_by_id,
    get_source_channels,
    remove_source_channel,
)
from scraper import run_scraper
from services import escape_markdown, log


async def send_bot_message(http_client: httpx.AsyncClient, chat_id: int, text: str, **kwargs):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown", **kwargs}
    try:
        resp = await http_client.post(f"{BOT_API_URL}/sendMessage", json=payload, timeout=20)
        resp.raise_for_status()
        log(f"Sent bot message to {chat_id}.")
        return resp.json()
    except httpx.HTTPStatusError as e:
        log(f"Error sending bot message to {chat_id}: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        log(f"Error in send_bot_message: {e}")
    return None


async def handle_bot_command(http_client: httpx.AsyncClient, message: dict):
    text = message.get("text", "")
    chat_id = message["chat"].get("id")
    sender_id = message.get("from", {}).get("id")

    if not text.startswith("/"):
        return

    command, *args = text.split()
    command = command[1:].lower()
    log(f"Received command '{command}' from chat {chat_id}")

    if command == "start":
        await send_bot_message(http_client, chat_id, "Hello! I am the Car Scraper Bot.\n\n🚗 Forward a car post to me to get more details or a direct link to the listing.")
        return

    if sender_id != ADMIN_ID:
        log(f"Unauthorized command '{command}' from {sender_id}")
        return

    if command == "s":
        notify = lambda msg: asyncio.create_task(send_bot_message(http_client, chat_id, msg))
        await send_bot_message(http_client, chat_id, "Scraping process initiated...")
        async with Client(SESSION_NAME, api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING) as u_app:
            await run_scraper(u_app, notify=notify)
        return

    if command == "cha":
        channels = get_source_channels()
        if not channels:
            response = "There are no source channels configured."
        else:
            response = "**Current Source Channels:**\n" + "\n".join(f"- `{channel}`" for channel in channels)
        await send_bot_message(http_client, chat_id, response)
        return

    if command == "addc":
        if not args:
            await send_bot_message(http_client, chat_id, "**Usage:** `/addc <username>`\n\n*Example:* `/addc some_channel_name`")
            return
        channel_username = args[0].lstrip("@")
        try:
            add_source_channel(channel_username)
            log(f"Added new source channel: {channel_username}")
            await send_bot_message(http_client, chat_id, f"✅ Successfully added channel: `{channel_username}`")
        except psycopg2.errors.UniqueViolation:
            log(f"Attempted to add duplicate channel: {channel_username}")
            await send_bot_message(http_client, chat_id, f"⚠️ Channel `{channel_username}` is already in the list.")
        except Exception as e:
            log(f"Error adding channel {channel_username}: {e}")
            await send_bot_message(http_client, chat_id, f"❌ An error occurred while adding the channel: `{e}`")
        return

    if command == "delc":
        if not args:
            await send_bot_message(http_client, chat_id, "**Usage:** `/delc <username>`\n\n*Example:* `/delc some_channel_name`")
            return
        channel_username = args[0].lstrip("@")
        try:
            if not remove_source_channel(channel_username):
                await send_bot_message(http_client, chat_id, f"⚠️ Channel `{channel_username}` not found in the list.")
                return
            log(f"Removed source channel: {channel_username}")
            await send_bot_message(http_client, chat_id, f"✅ Successfully removed channel: `{channel_username}`")
        except Exception as e:
            log(f"Error removing channel {channel_username}: {e}")
            await send_bot_message(http_client, chat_id, f"❌ An error occurred while removing the channel: `{e}`")


async def handle_bot_forward(http_client: httpx.AsyncClient, message: dict):
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
        await send_bot_message(http_client, chat_id, f"Sorry, I couldn't find any information for Car ID: `{car_id}`")
        return

    parsed_data = car_row["parsed_data"]
    raw_caption = car_row["caption_raw"]
    source_chat_id = car_row["source_chat_id"]
    source_message_id = car_row["source_message_id"]
    created_at = car_row["created_at"]
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
                if key == "mileage":
                    try:
                        value = f"{int(str(value).replace(',', '')):,} km"
                    except (ValueError, TypeError):
                        pass
                reply_parts.append(f"{display_name}: {value}")

        price_data = parsed_data.get("price")
        if isinstance(price_data, dict):
            price_parts = []
            if price_data.get("total"):
                try:
                    price_parts.append(f"💰 **Price**: {int(float(str(price_data['total']).replace(',', ''))):,}")
                except (ValueError, TypeError):
                    price_parts.append(f"💰 **Price**: {escape_markdown(str(price_data['total']))}")
            if price_data.get("cash"):
                try:
                    price_parts.append(f"💵 **Cash**: {int(float(str(price_data['cash']).replace(',', ''))):,}")
                except (ValueError, TypeError):
                    price_parts.append(f"💵 **Cash**: {escape_markdown(str(price_data['cash']))}")
            if price_data.get("bank"):
                try:
                    price_parts.append(f"🏦 **Bank**: {int(float(str(price_data['bank']).replace(',', ''))):,}")
                except (ValueError, TypeError):
                    price_parts.append(f"🏦 **Bank**: {escape_markdown(str(price_data['bank']))}")
            if price_data.get("monthly"):
                try:
                    price_parts.append(f"🗓️ **Monthly**: {int(float(str(price_data['monthly']).replace(',', ''))):,}")
                except (ValueError, TypeError):
                    price_parts.append(f"🗓️ **Monthly**: {escape_markdown(str(price_data['monthly']))}")
            if price_parts:
                reply_parts.extend(price_parts)
        elif price_data and str(price_data).strip():
            try:
                formatted_price = f"{int(str(price_data).replace(',', '')):,}"
            except (ValueError, TypeError):
                formatted_price = escape_markdown(str(price_data))
            reply_parts.append(f"💰 **Price**: {formatted_price}")

        additional_details = parsed_data.get("additional_details")
        if additional_details:
            reply_parts.append("\n📝 **Additional Info**:")
            for detail in additional_details:
                reply_parts.append(f" - {escape_markdown(detail)}")

    chat_identifier = str(source_chat_id)
    if isinstance(source_chat_id, int) and str(source_chat_id).startswith("-100"):
        chat_identifier = str(source_chat_id)[4:]
        source_link = f"https://t.me/c/{chat_identifier}/{source_message_id}"
    else:
        chat_identifier = chat_identifier.lstrip("@")
        source_link = f"https://t.me/{chat_identifier}/{source_message_id}"

    reply_parts.append("\n" + "-" * 15)
    reply_parts.append(f"ℹ️ **Source:** [Original Post]({source_link})")
    reply_parts.append(f"🕒 **Scraped At:** `{created_at}`")
    await send_bot_message(http_client, chat_id, "\n".join(reply_parts), disable_web_page_preview=True)


async def setup_bot(http_client: httpx.AsyncClient):
    if not WEBHOOK_URL:
        log("WEBHOOK_URL environment variable not set. Skipping webhook setup.")
        return

    set_webhook_url = f"{BOT_API_URL}/setWebhook?url={WEBHOOK_URL}/telegram"
    try:
        resp = await http_client.get(set_webhook_url)
        resp.raise_for_status()
        log(f"Webhook set successfully to {WEBHOOK_URL}/telegram. Response: {resp.json()}")
    except Exception as e:
        log(f"Error setting webhook: {e}")
