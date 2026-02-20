import asyncio
import sys
import traceback
from pathlib import Path

import httpx
import uvicorn
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse

from config import (
    ADMIN_ID,
    CRON_SECRET,
    validate_required_settings,
)
from db import (
    get_car_row_by_id,
    init_db,
)
from bot import handle_bot_command, handle_bot_forward, setup_bot
from scraper import run_scraper_with_client
from services import escape_markdown, log

http_client = httpx.AsyncClient()

init_db(log)
@asynccontextmanager
async def lifespan(app: FastAPI):
    await setup_bot(http_client)
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
        await handle_bot_forward(http_client, message)
    elif "text" in message:
        await handle_bot_command(http_client, message)

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

if __name__ == "__main__":
    missing = validate_required_settings()
    if missing:
        log(f"FATAL: Missing required environment variables: {', '.join(missing)}")
    else:
        if len(sys.argv) > 1 and sys.argv[1] == '--scrape':
            asyncio.run(run_scraper_with_client())
        else:
            uvicorn.run("main:app", host="0.0.0.0", port=8080)
        
        
