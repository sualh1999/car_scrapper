import asyncio
import json
from datetime import datetime

import psycopg2
from pyrogram import Client
from pyrogram.types import InputMediaPhoto

from config import ADMIN_ID, API_HASH, API_ID, MY_CHANNEL_ID, SESSION_NAME, SESSION_STRING
from db import (
    CARS_TABLE,
    get_db_connection,
    get_last_processed_id,
    get_source_channels,
    update_channel_state,
)
from services import format_caption_from_json, generate_car_id, log, parse_caption

is_scraping = False

CONTACT_INFO = """

Rijal Cars
- 2% commission is required.
መኪና ለመግዛትም ሆነ ለመሸጥ፣ ከስር ባለው ቁጥር ይደውሉልን።
📞 0982148598
📞 0991923566
ተጨማሪ መኪኖችን ለማግኘት የቴሌግራም ቻናላችንን ይቀላቀሉ።
https://t.me/Rijalcars
"""


def _render_post_caption(details_caption: str, car_id: str) -> str:
    return f"{details_caption}{CONTACT_INFO}\n\n**ID:** `{car_id}`"


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
            last_processed_id = get_last_processed_id(channel_username)
            log(f"Will process messages newer than ID: {last_processed_id} for channel {channel_username}")

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
                continue

            log(f"Found {len(new_messages)} new messages to process for {channel_username}.")

            messages = list(reversed(new_messages))
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
                        for media_msg in group_msgs:
                            if media_msg.photo:
                                images.append(media_msg.photo.file_id)
                            if media_msg.caption:
                                caption_text = media_msg.caption
                            processed_message_ids.add(media_msg.id)
                            processed_ids_this.add(media_msg.id)
                    except Exception as e:
                        log(f"Error getting media group for msg {msg.id} in {channel_username}: {e}")
                        i += 1
                        continue
                else:
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
                            message_ids=message_to_process.id,
                        )
                    except Exception as e:
                        log(f"Failed to forward non-sale post {message_to_process.id} to admin: {e}")
                    if processed_ids_this:
                        update_channel_state(channel_username, max(processed_ids_this))
                    i += 1
                    continue

                car_id = generate_car_id()
                details_caption = format_caption_from_json(parsed)
                final_caption = _render_post_caption(details_caption, car_id)

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

                insert_ok = False
                for attempt in range(1, 4):
                    try:
                        with get_db_connection() as conn:
                            with conn.cursor() as cursor:
                                cursor.execute(
                                    f"""
                                    INSERT INTO {CARS_TABLE}
                                    (id, created_at, source_chat_id, source_message_id, caption_raw, parsed_data, images, my_channel_id, my_message_id)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    """,
                                    (
                                        car_id,
                                        datetime.now(),
                                        channel_username,
                                        message_to_process.id,
                                        caption_text,
                                        json.dumps(parsed),
                                        images,
                                        MY_CHANNEL_ID,
                                        my_msg_id,
                                    ),
                                )
                            conn.commit()
                        insert_ok = True
                        break
                    except psycopg2.errors.UniqueViolation:
                        log(f"Duplicate Car ID detected on insert: {car_id} (attempt {attempt}/3). Regenerating...")
                        if attempt == 3:
                            raise
                        car_id = generate_car_id()
                        final_caption = _render_post_caption(details_caption, car_id)
                        await client.edit_message_caption(
                            chat_id=MY_CHANNEL_ID,
                            message_id=my_msg_id,
                            caption=final_caption,
                        )
                        log(f"Updated posted caption with new Car ID: {car_id}")

                if not insert_ok:
                    raise RuntimeError(f"Failed to insert car row for source message {message_to_process.id}")
                if processed_ids_this:
                    update_channel_state(channel_username, max(processed_ids_this))
                log(f"Inserted Car ID {car_id} into DB for source message {message_to_process.id} from {channel_username}. Updated state.")
                i += 1

        log("--- Scraping finished for all channels. ---")
        await admin_notify("Scraping finished for all channels.")
    finally:
        is_scraping = False


async def run_scraper_with_client():
    async with Client(SESSION_NAME, api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING) as u_app:
        await run_scraper(u_app, notify=None)
