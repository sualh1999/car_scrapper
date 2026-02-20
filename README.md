# Telegram Car Scraper

Scrapes Telegram car listings, parses captions with OpenRouter, reposts structured listings to a target channel, and exposes a small FastAPI interface.

## Features
- Scrape source Telegram channels via Pyrogram session
- Parse listing captions into structured JSON with OpenRouter models
- Repost listings with formatted caption and generated short ID
- Store listings in PostgreSQL
- Query listing details by ID from bot-forwarded posts and `/car?car_id=...`
- Trigger scraper via webhook endpoint (`/run-scraper`) or CLI (`--scrape`)

## Project Structure
- `main.py`: FastAPI routes, bot command/forward handlers, scraper orchestration
- `config.py`: environment loading and validation
- `db.py`: database schema/init + data access helpers
- `services.py`: AI parsing, caption formatting, ID generation, shared helpers

## Requirements
- Python 3.11+
- PostgreSQL
- Telegram API credentials + Pyrogram session
- Telegram bot token
- OpenRouter API key and model

Install dependencies:

```bash
pip install -r requirements.txt
```

## Environment Variables
Copy `.env.example` to `.env` and fill values:

```bash
cp .env.example .env
```

Required variables:
- `API_ID`
- `API_HASH`
- `SESSION_STRING`
- `BOT_TOKEN`
- `DATABASE_URL`
- `MY_CHANNEL_ID`
- `ADMIN_ID`

AI variables:
- `OPENROUTER_API_KEY` (or rotated keys: `OPENROUTER_API_KEY__0`, `OPENROUTER_API_KEY__1`, ...)
- `AI_MODEL_NAME` (example: `Qwen/Qwen3-Coder-Next:novita`)

## Run
Start API server:

```bash
python main.py
```

Run scraper once from CLI:

```bash
python main.py --scrape
```

## Endpoints
- `POST /telegram` Telegram bot webhook
- `POST /run-scraper` manual/cron scraper trigger (requires `X-CRON-SECRET`)
- `GET /car?car_id=XXXX` listing detail page
- `GET /` health check
