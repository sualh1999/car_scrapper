# Telegram Car Scraper

Scrapes Telegram car listings, parses captions with OpenAI-compatible LLM endpoints (Hugging Face Router or custom HF Spaces), reposts structured listings to a target channel, and exposes a small FastAPI interface.

## Features
- Scrape source Telegram channels via Pyrogram session
- Parse listing captions into structured JSON with an OpenAI-compatible model endpoint
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
- LLM endpoint (`AI_BASE_URL`) and model (`AI_MODEL_NAME`)

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
- `AI_BASE_URL` (default: `https://router.huggingface.co/v1`)
- `AI_MODEL_NAME` (router example: `Qwen/Qwen3-Coder-Next:novita`, HF Space example: `model.gguf`)
- `HF_TOKEN` (used for router by default)
- `AI_API_KEY` (optional, for custom endpoints if needed)

HF Space example:

```env
AI_BASE_URL=https://Salihq19-qwen0ai.hf.space/v1
AI_MODEL_NAME=model.gguf
AI_API_KEY=
```

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
