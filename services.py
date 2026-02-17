import json
import os
import random
import re
from datetime import datetime

from openai import AsyncOpenAI

from config import AI_MODEL_NAME
from db import CARS_TABLE, get_db_connection


def log(msg: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def _load_ai_clients():
    clients = []
    i = 0
    while True:
        key = os.getenv(f"HF_TOKEN__{i}")
        if key:
            clients.append(
                AsyncOpenAI(base_url="https://router.huggingface.co/v1", api_key=key)
            )
            i += 1
        else:
            if i == 0:
                single_key = os.getenv("HF_TOKEN")
                if single_key:
                    clients.append(
                        AsyncOpenAI(
                            base_url="https://router.huggingface.co/v1", api_key=single_key
                        )
                    )
            break
    log(f"Loaded {len(clients)} Hugging Face Router AI clients.")
    return clients


ai_clients = _load_ai_clients()


def escape_markdown(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"([_*`\[])", r"\\\1", text)


def _format_number(value):
    try:
        return f"{int(float(str(value).replace(',', ''))):,}"
    except (ValueError, TypeError):
        return value


def format_caption_from_json(parsed_data: dict) -> str:
    if not parsed_data or not parsed_data.get("is_for_sale_post"):
        log("format_caption_from_json called with invalid data, returning empty.")
        return ""

    caption_parts = []
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
    for key, display_name in field_map.items():
        value = parsed_data.get(key)
        if value and str(value).strip():
            if key == "mileage":
                value = f"{_format_number(value)} km"
            caption_parts.append(f"{display_name}: {value}")

    price_data = parsed_data.get("price")
    if isinstance(price_data, dict):
        total_price_label = (
            "💰 **Total Price**"
            if price_data.get("bank") and str(price_data.get("bank")).strip()
            else "💰 **Price**"
        )
        price_map = {
            "total": total_price_label,
            "cash": "💵 **Cash**",
            "bank": "🏦 **Bank**",
            "monthly": "🗓️ **Monthly**",
        }
        for key, display_name in price_map.items():
            value = price_data.get(key)
            if value and str(value).strip():
                caption_parts.append(f"{display_name}: {_format_number(value)}")

    additional_details = parsed_data.get("additional_details")
    if additional_details:
        caption_parts.append("\n📝 **Additional Info**")
        for detail in additional_details:
            caption_parts.append(f" - {escape_markdown(detail)}")

    return "\n".join(caption_parts)


def generate_car_id():
    chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    char_map = {c: i for i, c in enumerate(chars)}
    n = len(chars)

    def increment_id(value: str) -> str:
        id_list = list(value)
        for idx in range(len(id_list) - 1, -1, -1):
            ch_index = char_map[id_list[idx]]
            if ch_index < n - 1:
                id_list[idx] = chars[ch_index + 1]
                return "".join(id_list)
            id_list[idx] = chars[0]
        log("WARNING: Car ID overflow. Resetting to start value.")
        return "C13s"

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT id FROM {CARS_TABLE}
                WHERE LENGTH(id) = 4
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """
            )
            row = cursor.fetchone()
            last_id = row[0] if row else None
            next_id = "C13s" if last_id is None else increment_id(last_id)
            for _ in range(1000):
                cursor.execute(
                    f"SELECT 1 FROM {CARS_TABLE} WHERE id = %s LIMIT 1",
                    (next_id,),
                )
                if not cursor.fetchone():
                    return next_id
                next_id = increment_id(next_id)
    raise RuntimeError("Unable to generate a unique car ID after many attempts.")


async def parse_caption(caption: str | None):
    if not caption or not ai_clients:
        log("AI client not available or empty caption. Skipping AI parse.")
        log(caption)
        return None

    schema = {
        "type": "object",
        "properties": {
            "is_for_sale_post": {"type": "boolean"},
            "make": {"type": ["string", "null"]},
            "model": {"type": ["string", "null"]},
            "year": {"type": ["string", "null"]},
            "mileage": {"type": ["string", "null"]},
            "transmission": {"type": ["string", "null"]},
            "price": {
                "type": ["object", "null"],
                "properties": {
                    "total": {"type": ["string", "null"]},
                    "cash": {"type": ["string", "null"]},
                    "bank": {"type": ["string", "null"]},
                    "monthly": {"type": ["string", "null"]},
                },
            },
            "plate_number": {"type": ["string", "null"]},
            "condition": {"type": ["string", "null"]},
            "fuel_type": {"type": ["string", "null"]},
            "cc": {"type": ["string", "null"]},
            "seats": {"type": ["string", "null"]},
            "engine": {"type": ["string", "null"]},
            "battery_capacity": {"type": ["string", "null"]},
            "driver_type": {"type": ["string", "null"]},
            "top_speed": {"type": ["string", "null"]},
            "color": {"type": ["string", "null"]},
            "body_type": {"type": ["string", "null"]},
            "additional_details": {"type": ["array", "null"], "items": {"type": "string"}},
        },
        "required": [
            "is_for_sale_post",
            "make",
            "model",
            "year",
            "mileage",
            "transmission",
            "price",
            "plate_number",
            "condition",
            "fuel_type",
            "cc",
            "seats",
            "engine",
            "battery_capacity",
            "driver_type",
            "top_speed",
            "color",
            "body_type",
            "additional_details",
        ],
    }

    prompt = f"""You are an expert car advertisement parser.
Your primary goal is to determine if the ad is for selling a car and then extract structured details into a single valid JSON object.

### Car Ad Text
"{caption}"

### Expected JSON Schema
{json.dumps(schema, indent=2)}

### Extraction Rules
1. First, determine if the text is an ad to sell a vehicle. Set is_for_sale_post to true/false.
2. If is_for_sale_post is false, return null for all other fields.
3. Response must be only one JSON object.
4. Always include all fields.
5. price: extract total/cash/bank/monthly. If only cash and bank are present, calculate total=cash+bank.
6. mileage: extract only numeric value.
7. additional_details: include relevant car details only.
8. transmission: standardize to Automatic or Manual.
"""

    try:
        client = random.choice(ai_clients)
        log("Sending caption to Hugging Face Router for parsing...")
        completion = await client.chat.completions.create(
            model=AI_MODEL_NAME,
            response_format={"type": "json_object"},
            messages=[
                {
                    "role": "system",
                    "content": "Return a single valid JSON object only.",
                },
                {"role": "user", "content": prompt},
            ],
        )
        response_content = completion.choices[0].message.content
        log(f"Received from Hugging Face Router: {response_content}")
        return json.loads(response_content)
    except Exception as e:
        log(f"Error calling Hugging Face Router or parsing response: {e}")
        return None


