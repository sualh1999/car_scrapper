import psycopg2
from psycopg2.extras import RealDictCursor

from config import DATABASE_URL

SCHEMA = "my_schema"
CARS_TABLE = f"{SCHEMA}.cars"
CHANNEL_STATE_TABLE = f"{SCHEMA}.channel_state"
SOURCE_CHANNELS_TABLE = f"{SCHEMA}.source_channels"


def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


def init_db(log):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {CARS_TABLE} (
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
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {CHANNEL_STATE_TABLE} (
                    channel_id TEXT PRIMARY KEY,
                    last_processed_id BIGINT NOT NULL
                );
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SOURCE_CHANNELS_TABLE} (
                    username TEXT PRIMARY KEY NOT NULL
                );
                """
            )
            cursor.execute(f"SELECT COUNT(*) FROM {SOURCE_CHANNELS_TABLE}")
            if cursor.fetchone()[0] == 0:
                initial_channels = ["Mikycarboss", "Golden_car_market"]
                for channel in initial_channels:
                    cursor.execute(
                        f"INSERT INTO {SOURCE_CHANNELS_TABLE} (username) VALUES (%s) ON CONFLICT (username) DO NOTHING",
                        (channel,),
                    )
                log(f"Initialized source channels with: {', '.join(initial_channels)}")
        conn.commit()


def update_channel_state(channel_username: str, last_processed_id: int) -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO {CHANNEL_STATE_TABLE} (channel_id, last_processed_id)
                VALUES (%s, %s)
                ON CONFLICT (channel_id) DO UPDATE
                SET last_processed_id = EXCLUDED.last_processed_id
                """,
                (channel_username, last_processed_id),
            )
        conn.commit()


def get_last_processed_id(channel_username: str) -> int:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT last_processed_id FROM {CHANNEL_STATE_TABLE} WHERE channel_id = %s",
                (channel_username,),
            )
            row = cursor.fetchone()
            return row[0] if row else 0


def get_source_channels() -> list[str]:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT username FROM {SOURCE_CHANNELS_TABLE}")
            return [row[0] for row in cursor.fetchall()]


def add_source_channel(channel_username: str) -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO {SOURCE_CHANNELS_TABLE} (username) VALUES (%s)",
                (channel_username,),
            )
        conn.commit()


def remove_source_channel(channel_username: str) -> bool:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"DELETE FROM {SOURCE_CHANNELS_TABLE} WHERE username = %s",
                (channel_username,),
            )
            deleted = cursor.rowcount > 0
        conn.commit()
    return deleted


def get_car_by_id(car_id: str):
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                f"""
                SELECT parsed_data, caption_raw, source_chat_id, source_message_id, created_at
                FROM {CARS_TABLE}
                WHERE id = %s
                """,
                (car_id,),
            )
            return cursor.fetchone()


def get_car_row_by_id(car_id: str):
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(f"SELECT * FROM {CARS_TABLE} WHERE id = %s", (car_id,))
            return cursor.fetchone()
