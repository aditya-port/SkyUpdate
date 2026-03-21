import asyncio
import random
import uuid
import datetime
import re

import aiohttp
import asyncpg
import os
from dotenv import load_dotenv

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ConversationHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from scraper import run_scraper
from url_extract import get_weather_url
from insights import (
    generate_insights,
    generate_insights_split,
    generate_alert_message,
    generate_tomorrow_forecast,
    generate_weekly_forecast,
    generate_bonus_insights,
    generate_ask_response,
    generate_radar_chart,
    get_streak_history,
)
from insights_engine import build_streak_context

# ── Environment ───────────────────────────────────────────────────────────────
load_dotenv()  # loads .env from project root (src/.env)
BOT_TOKEN    = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

# ── Connection pool — reused across all DB calls, avoids per-request connect ──
_pool = None

async def get_pool():
    """Returns the shared asyncpg pool, creating it on first call."""
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            DATABASE_URL,
            ssl="require",
            min_size=2,
            max_size=10,
        )
    return _pool


# ── Conversation states ───────────────────────────────────────────────────────
NICKNAME_WAITING  = 1   # Waiting for user to type a saved-location nickname
EVENT_NAME_WAITING = 2  # Waiting for event name in /remind flow
EVENT_DATE_WAITING = 3  # Waiting for event date in /remind flow

# ── WMO code dictionaries ─────────────────────────────────────────────────────
WMO_CODES = {
    0: "Clear Sky", 1: "Mainly Clear", 2: "Partly Cloudy", 3: "Overcast",
    45: "Fog", 48: "Rime Fog",
    51: "Light Drizzle", 53: "Moderate Drizzle", 55: "Dense Drizzle",
    61: "Slight Rain", 63: "Moderate Rain", 65: "Heavy Rain",
    71: "Slight Snowfall", 73: "Moderate Snowfall", 75: "Heavy Snowfall",
    80: "Slight Showers", 81: "Moderate Showers", 82: "Violent Showers",
    95: "Thunderstorm", 96: "Thunderstorm with Hail", 99: "Thunderstorm with Heavy Hail",
}

WMO_CONDITION = {
    0: "clear",   1: "clear",   2: "cloudy",  3: "overcast",
    45: "fog",    48: "fog",
    51: "drizzle", 53: "drizzle", 55: "drizzle",
    61: "rain",   63: "rain",   65: "rain",
    71: "snow",   73: "snow",   75: "snow",
    80: "rain",   81: "rain",   82: "rain",
    95: "storm",  96: "storm",  99: "storm",
}

# ── Waiting line messages (shown while data loads) ────────────────────────────
_WAITING_MORNING = [
    "☀️ Fetching your morning forecast…",
    "🌅 Good morning! Checking what the sky has planned…",
    "☀️ Morning weather check in progress… ☕",
    "🌤️ Asking the sun what it's up to today…",
]
_WAITING_AFTERNOON = [
    "⏳ Checking afternoon conditions…",
    "🌤️ Fetching your midday forecast…",
    "⏳ Sky update in progress… won't take long!",
    "☁️ Summoning clouds for a status update…",
]
_WAITING_EVENING = [
    "🌆 Fetching tonight's outlook…",
    "🌙 Checking what the evening has in store…",
    "🌇 Evening forecast incoming…",
    "🌃 Almost done… sky is doing its thing 🔮",
]
_WAITING_NIGHT = [
    "🌙 Checking overnight conditions…",
    "🌌 Night forecast loading…",
    "⭐ Fetching your late-night weather update…",
]

def _waiting_line() -> str:
    """Returns a time-appropriate loading message."""
    import random
    h = datetime.datetime.now().hour
    if 5 <= h < 12:
        return random.choice(_WAITING_MORNING)
    elif 12 <= h < 17:
        return random.choice(_WAITING_AFTERNOON)
    elif 17 <= h < 21:
        return random.choice(_WAITING_EVENING)
    else:
        return random.choice(_WAITING_NIGHT)

# Legacy alias kept so any remaining _waiting_line() still works
WAITING_LINES = _WAITING_MORNING + _WAITING_AFTERNOON + _WAITING_EVENING + _WAITING_NIGHT


# ─────────────────────────────────────────────────────────────────────────────
# CONTACT HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def normalize_contact(text: str):
    """
    Validates and normalises a phone number or Gmail address.
    Strips +91 prefix from 12-digit Indian numbers.
    Returns the clean value or None if invalid.
    """
    text  = text.strip()
    phone = re.sub(r"\D", "", text)

    if phone.startswith("91") and len(phone) == 12:
        phone = phone[2:]

    if re.match(r"^[6-9]\d{9}$", phone):
        return phone

    if re.match(r"^[a-zA-Z0-9._%+-]+@gmail\.com$", text):
        return text.lower()

    return None


# ─────────────────────────────────────────────────────────────────────────────
# KEYBOARD BUILDERS
# ─────────────────────────────────────────────────────────────────────────────

def build_main_keyboard() -> ReplyKeyboardMarkup:
    """Single persistent 'Main Menu' button — always visible, never changes."""
    return ReplyKeyboardMarkup(
        [[KeyboardButton("🏠 Main Menu")]],
        resize_keyboard=True,
    )


def build_choice_keyboard() -> ReplyKeyboardMarkup:
    """
    DEPRECATED — choice is now presented as an inline keyboard via
    send_choice_inline(). Kept only as a fallback for edge cases.
    """
    return ReplyKeyboardMarkup(
        [[KeyboardButton("🌤️ See Weather"), KeyboardButton("💡 See Insights")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def _short_name(area_string: str, nickname: str = None) -> str:
    """
    Returns the display name for a location.
    Priority: saved nickname → first word before the first comma in area_string.
    E.g. "Siliguri, West Bengal, India" → "Siliguri"
         "Home" (nickname) → "Home"
    """
    if nickname:
        return nickname.strip()
    return (area_string or "").split(",")[0].strip()


def build_choice_inline(area_string: str, nickname: str = None) -> InlineKeyboardMarkup:
    """
    Inline choice prompt — encodes area directly into callback_data so it
    works even if context.user_data is cleared (e.g. after bot restart).
    Telegram callback_data limit is 64 bytes — area is truncated if needed.
    """
    # Encode: "choice_weather|area|nickname" — nickname blank if GPS share
    nick = nickname or ""
    weather_data  = f"choice_weather|{area_string}|{nick}"[:64]
    insights_data = f"choice_insights|{area_string}|{nick}"[:64]
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🌤️ Weather",  callback_data=weather_data),
        InlineKeyboardButton("💡 Insights", callback_data=insights_data),
    ]])


def build_feedback_keyboard() -> ReplyKeyboardMarkup:
    """
    Feedback keyboard shown every 4th successful run.
    Replaces the main keyboard — user must tap to restore normal flow.
    """
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("👍 Helpful"), KeyboardButton("👎 Not helpful")],
            [KeyboardButton("⏭️ Skip feedback")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def get_smart_keyboard(weather_code: int, run_id: int) -> InlineKeyboardMarkup:
    """
    Inline keyboard for the weather card.
    Row 1: Insights + Tomorrow + 7-Day.
    Row 2: Save Location.
    """
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("💡 Insights",       callback_data="insights"),
            InlineKeyboardButton("📅 Tomorrow",       callback_data="tomorrow"),
            InlineKeyboardButton("📆 7-Day",          callback_data="weekly_forecast"),
        ],
        [
            InlineKeyboardButton("💾 Save Location",  callback_data="save_location"),
        ],
    ])


def get_weather_followup_keyboard() -> InlineKeyboardMarkup:
    """Small inline keyboard appended below a weather message when shown after insights."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🌤️ See Weather", callback_data="show_weather")]
    ])


def get_insights_followup_keyboard() -> InlineKeyboardMarkup:
    """Small inline keyboard appended below an insights message."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🌤️ See Weather Card",   callback_data="show_weather")],
        [InlineKeyboardButton("📅 Tomorrow forecast",  callback_data="tomorrow"),
         InlineKeyboardButton("📆 7-Day Forecast",     callback_data="weekly_forecast")],
        [InlineKeyboardButton("🏠 Main Menu",          callback_data="main_menu")],
    ])


def _build_insights_keyboard(has_more: bool, area: str = "") -> InlineKeyboardMarkup:
    """
    Keyboard shown below the initial (collapsed) insights message.
    If has_more is True, shows a 'Show more' button that expands the rest.
    Area is encoded in the callback_data so it works even after bot restart
    (context.user_data would be lost, but callback_data persists in Telegram).
    """
    rows = []
    if has_more:
        # Encode area so insights_show_more_callback can re-run the engine
        # if context.user_data is gone (e.g. after restart).
        cb_data = f"insights_show_more|{area}"[:64]
        rows.append([InlineKeyboardButton("➕ Show more insights", callback_data=cb_data)])
    rows += [
        [InlineKeyboardButton("🌤️ See Weather Card",  callback_data="show_weather"),
         InlineKeyboardButton("🌧️ Rain Chart",         callback_data="radar")],
        [InlineKeyboardButton("📅 Tomorrow forecast",  callback_data="tomorrow"),
         InlineKeyboardButton("📆 7-Day Forecast",     callback_data="weekly_forecast")],
        [InlineKeyboardButton("🏠 Main Menu",          callback_data="main_menu")],
    ]
    return InlineKeyboardMarkup(rows)


# ─────────────────────────────────────────────────────────────────────────────
# DATABASE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

async def log_activity(user, action: str, detail: str = "", area: str = "",
                       lat=None, lon=None, url_requested=None,
                       session_id=None, condition=None):
    try:
        async with (await get_pool()).acquire() as conn:
            await conn.execute(
                """
                INSERT INTO user_activity
                (user_id, username, first_name, last_name, language_code,
                 is_premium, is_bot, action, detail, area, lat, lon,
                 url_requested, session_id, condition)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
                """,
                user.id, user.username, user.first_name, user.last_name,
                user.language_code, user.is_premium or False, user.is_bot,
                action, detail, area, lat, lon,
                url_requested, session_id, condition,
            )
    except Exception as e:
        print(f"[DB ERROR - activity] {e}")


async def upsert_customer(user):
    try:
        async with (await get_pool()).acquire() as conn:
            await conn.execute(
                """
                INSERT INTO users
                (user_id, username, first_name, last_name, language_code, is_premium, is_bot)
                VALUES ($1,$2,$3,$4,$5,$6,$7)
                ON CONFLICT (user_id) DO UPDATE SET
                    username      = EXCLUDED.username,
                    first_name    = EXCLUDED.first_name,
                    last_name     = EXCLUDED.last_name,
                    language_code = EXCLUDED.language_code,
                    is_premium    = EXCLUDED.is_premium,
                    last_seen     = NOW()
                """,
                user.id, user.username, user.first_name, user.last_name,
                user.language_code, user.is_premium or False, user.is_bot,
            )
    except Exception as e:
        print(f"[DB ERROR - users] {e}")


async def increment_weather_count(user_id: int):
    """Increments weather_checks and successful_runs. Returns the new successful_runs count."""
    async with (await get_pool()).acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE users
            SET weather_checks   = COALESCE(weather_checks, 0) + 1,
                successful_runs  = COALESCE(successful_runs, 0) + 1
            WHERE user_id = $1
            RETURNING successful_runs
            """,
            user_id,
        )
        return row["successful_runs"] if row else 0


async def get_latest_run(user_id: int):
    """Returns the most recent scraper run row (run_id, weather_code, area)."""
    try:
        async with (await get_pool()).acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT sr.id AS run_id, cw.weather_code, sr.area
                FROM scraper_runs sr
                LEFT JOIN current_weather cw ON cw.run_id = sr.id
                WHERE sr.user_id = $1
                ORDER BY sr.ran_at DESC
                LIMIT 1
                """,
                user_id,
            )
            return row
    except Exception as e:
        print(f"[DB ERROR - get_latest_run] {e}")
        return None


async def get_weather_code_for_run(run_id: int) -> str | None:
    """
    Fetches weather_code from current_weather for a known run_id.
    Retries once after 500ms if the scraper write hasn't landed yet
    (can happen because run_scraper is called via asyncio.to_thread and
    the DB commit may not be visible immediately to a new connection).
    Returns the WMO_CONDITION string (e.g. "rain") or None.
    """
    import asyncio
    for attempt in range(2):
        try:
            async with (await get_pool()).acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT weather_code FROM current_weather WHERE run_id = $1 LIMIT 1",
                    run_id,
                )
                if row and row["weather_code"] is not None:
                    return WMO_CONDITION.get(row["weather_code"])
        except Exception as e:
            print(f"[DB ERROR - get_weather_code_for_run attempt={attempt}] {e}")
        if attempt == 0:
            await asyncio.sleep(0.5)  # give scraper's commit time to be visible
    return None


async def get_user_last_area(user_id: int):
    """Returns the area string from the most recent scraper run for this user."""
    try:
        async with (await get_pool()).acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT area FROM scraper_runs
                WHERE user_id = $1
                ORDER BY ran_at DESC
                LIMIT 1
                """,
                user_id,
            )
            return row["area"] if row else None
    except Exception as e:
        print(f"[DB ERROR - get_user_last_area] {e}")
        return None


async def read_weather_db(user_id: int) -> str:
    """
    Main weather card format:

        📍 Jaigaon
        🌡 28°C  ·  ↑34°  ↓22°
        ⛅ Partly Cloudy

        💨 Wind: 45 km/h from NE
        ☀️ UV: 9 — Very High   ·   🏭 AQI: 87 — Moderate
        🌅 6:02 AM  |  🌇 5:48 PM  (12.4 hrs daylight)

        • 🔥 Feels like 38°C — dangerous heat midday
        • 🌧️ Rain likely 2pm–5pm (72%) — carry an umbrella
    """
    from datetime import date, datetime
    from insights import _week_tips, _WMO_SHORT

    def deg_to_compass(deg) -> str:
        if deg is None: return ""
        dirs = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
        return dirs[round(deg / 45) % 8]

    def uv_label(uv) -> str:
        if uv is None: return ""
        if uv <= 2:  return "Low"
        if uv <= 5:  return "Moderate"
        if uv <= 7:  return "High"
        if uv <= 10: return "Very High"
        return "Extreme"

    def fmt_time_ampm(val) -> str:
        if val is None: return "N/A"
        if isinstance(val, str): return val
        try: return val.strftime("%I:%M %p").lstrip("0")
        except Exception: return str(val)

    async with (await get_pool()).acquire() as conn:
        scraped = await conn.fetchrow(
            """
            SELECT ws.feels_like, ws.condition, ws.high, ws.low,
                   ws.wind_speed, ws.sunrise, ws.sunset,
                   ws.aqi_value, ws.aqi_category, ws.uv_index,
                   ws.moon_phase, ws.visibility,
                   ws.data_source, ws.timestamp AS scraped_at,
                   sr.id AS run_id
            FROM weather_scraped ws
            JOIN scraper_runs sr ON sr.id = ws.run_id
            WHERE ws.user_id = $1
            ORDER BY ws.timestamp DESC
            LIMIT 1
            """,
            user_id,
        )

        if not scraped:
            return "⚠️ No weather data found. Please try sharing your location again."

        today  = date.today()
        run_id = scraped["run_id"]

        # Current actual temperature + current UV from current_weather
        current = await conn.fetchrow(
            "SELECT temperature_2m, uv_index, is_day FROM current_weather WHERE run_id = $1 LIMIT 1",
            run_id,
        )

        # Daily row for max/min, wind, UV, daylight, condition
        daily = await conn.fetchrow(
            """
            SELECT temperature_2m_max, temperature_2m_min,
                   apparent_temperature_max, apparent_temperature_min,
                   weather_code_max, precipitation_sum, rain_sum,
                   snowfall_sum, precipitation_hours,
                   wind_speed_10m_max, wind_gusts_10m_max,
                   wind_direction_10m_dominant,
                   uv_index_max, daylight_duration,
                   sunrise, sunset
            FROM daily_weather
            WHERE run_id = $1 AND date = $2
            LIMIT 1
            """,
            run_id, today,
        )

        # Rain % — max precipitation_probability from remaining hours today
        # Sourced directly from Open-Meteo hourly, no calculation on our side
        rain_prob_row = await conn.fetchrow(
            """
            SELECT MAX(precipitation_probability) AS max_prob
            FROM hourly_weather
            WHERE run_id = $1
              AND DATE(timestamp) = $2
              AND timestamp >= NOW()
            """,
            run_id, today,
        )


    # ── Current temp ──────────────────────────────────────────────────────────
    curr_temp = None
    if current and current["temperature_2m"] is not None:
        curr_temp = round(current["temperature_2m"])

    # ── Daily max / min ───────────────────────────────────────────────────────
    if daily:
        tmax = round(daily["temperature_2m_max"] or 0)
        tmin = round(daily["temperature_2m_min"] or 0)
    else:
        # Fallback to scraped high/low strings
        tmax = scraped["high"] or "?"
        tmin = scraped["low"] or "?"

    if curr_temp is not None:
        temp_line = f"🌡 *{curr_temp}°C*  ·  ↑{tmax}°  ↓{tmin}°"
    else:
        temp_line = f"↑{tmax}°  ↓{tmin}°"

    # ── Condition ─────────────────────────────────────────────────────────────
    if daily and daily["weather_code_max"] is not None:
        cond_label, cond_emoji = _WMO_SHORT.get(daily["weather_code_max"], ("", "⛅"))
        cond_line = f"{cond_emoji} {cond_label}" if cond_label else ""
    else:
        cond_line = f"⛅ {scraped['condition']}" if scraped["condition"] else ""

    # ── Rain % (only if ≥ 10% — sourced directly from Open-Meteo) ────────────
    rain_line = ""
    rain_prob = None
    if rain_prob_row and rain_prob_row["max_prob"] is not None:
        rain_prob = round(rain_prob_row["max_prob"])
    if rain_prob is not None and rain_prob >= 10:
        rain_line = f"🌧 Rain chance: {rain_prob}% today"

    # ── Wind (speed + direction, no gusts) ────────────────────────────────────
    if daily and daily["wind_speed_10m_max"]:
        wind_spd = round(daily["wind_speed_10m_max"])
        wind_dir = deg_to_compass(daily["wind_direction_10m_dominant"])
        dir_str  = f" from {wind_dir}" if wind_dir else ""
        wind_line = f"💨 Wind: {wind_spd} km/h{dir_str}"
    else:
        wind_line = f"💨 Wind: {scraped['wind_speed'] or 'N/A'}"

    # ── UV + AQI ──────────────────────────────────────────────────────────────
    # UV display logic:
    #   - After sunset → hide UV entirely (already handled by _sun_has_set)
    #   - Daytime + current UV available → show current, daily max as context
    #   - Daytime + no current UV → fall back to daily max (less precise)
    curr_uv    = current.get("uv_index") if current else None
    curr_is_day = current.get("is_day", 0) if current else 0
    daily_uv_max = daily["uv_index_max"] if daily else None

    def _sun_has_set() -> bool:
        now = datetime.now()
        raw = (daily.get("sunset") if daily else None) or scraped.get("sunset")
        if raw is None:
            return False
        try:
            if isinstance(raw, str):
                sunset_dt = datetime.fromisoformat(raw)
            else:
                sunset_dt = datetime(now.year, now.month, now.day, raw.hour, raw.minute)
            return now > sunset_dt
        except Exception:
            return False

    if _sun_has_set():
        uv_str = ""   # Sun has set — UV meaningless
    elif curr_uv is not None and curr_is_day == 1:
        # Current UV reading available — show it with daily peak as context
        uv_str = f"☀️ UV: {round(curr_uv)} — {uv_label(curr_uv)}"
        if daily_uv_max is not None and round(daily_uv_max) > round(curr_uv):
            uv_str += f" (peaks {round(daily_uv_max)} today)"
    elif daily_uv_max is not None:
        uv_str = f"☀️ UV: {round(daily_uv_max)} — {uv_label(daily_uv_max)} (today's max)"
    elif scraped.get("uv_index"):
        uv_val = scraped["uv_index"]
        uv_str = f"☀️ UV: {round(uv_val)} — {uv_label(uv_val)}"
    else:
        uv_str = ""

    aqi_val = scraped["aqi_value"] or "N/A"
    aqi_cat = scraped["aqi_category"]
    aqi_str = f"🏭 AQI: {aqi_val} — {aqi_cat}" if aqi_cat else f"🏭 AQI: {aqi_val}"
    uv_aqi_line = f"{uv_str}   ·   {aqi_str}" if uv_str else aqi_str

    # ── Sunrise / Sunset / Daylight ───────────────────────────────────────────
    sr = fmt_time_ampm(daily.get("sunrise") or scraped.get("sunrise"))
    ss = fmt_time_ampm(daily.get("sunset")  or scraped.get("sunset"))
    if daily and daily["daylight_duration"]:
        daylight_hrs = round(daily["daylight_duration"] / 3600, 1)
        sun_line = f"🌅 {sr}  |  🌇 {ss}  ({daylight_hrs} hrs daylight)"
    else:
        sun_line = f"🌅 {sr}  |  🌇 {ss}"

    # ── Insight bullets ───────────────────────────────────────────────────────
    bullets = ""
    if daily:
        tips = [t for t in _week_tips(dict(daily)) if "UV" not in t]
        if tips:
            bullets = "\n" + "\n".join(f"• {t}" for t in tips)

    # ── Data freshness + source notice ───────────────────────────────────────
    # Show how old the data is. If on Open-Meteo fallback, warn the user so
    # they know the numbers are estimated rather than scraped from weather.com.
    footer_parts = []
    scraped_at = scraped.get("scraped_at")
    if scraped_at:
        from datetime import timezone
        now_naive = datetime.now()
        if hasattr(scraped_at, 'tzinfo') and scraped_at.tzinfo is not None:
            scraped_at = scraped_at.replace(tzinfo=None)
        age_mins = int((now_naive - scraped_at).total_seconds() / 60)
        if age_mins < 1:
            footer_parts.append("_Updated just now_")
        elif age_mins < 60:
            footer_parts.append(f"_Updated {age_mins} min ago_")
        else:
            footer_parts.append(f"_Updated {age_mins // 60}h ago_")

    data_source = scraped.get("data_source", "")
    if data_source == "open_meteo_fallback":
        footer_parts.append("_⚠️ Live scrape unavailable — showing Open-Meteo estimates_")

    footer = "\n".join(footer_parts) if footer_parts else ""

    # ── Assemble ──────────────────────────────────────────────────────────────
    lines = [temp_line]
    if cond_line:
        lines.append(cond_line)
    lines.append("")
    if rain_line:
        lines.append(rain_line)
    lines.append(wind_line)
    lines.append(uv_aqi_line)
    lines.append(sun_line)
    if bullets:
        lines.append(bullets)
    if footer:
        lines.append("")
        lines.append(footer)

    return "\n".join(lines)



async def save_location(user_id: int, nickname: str, area: str,
                        lat: float, lon: float, url: str):
    try:
        async with (await get_pool()).acquire() as conn:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM saved_locations WHERE user_id = $1", user_id
            )
            is_default = count == 0
            await conn.execute(
                """
                INSERT INTO saved_locations
                (user_id, nickname, area, lat, lon, url, is_default)
                VALUES ($1,$2,$3,$4,$5,$6,$7)
                ON CONFLICT DO NOTHING
                """,
                user_id, nickname, area, lat, lon, url, is_default,
            )
    except Exception as e:
        print(f"[DB ERROR - save_location] {e}")


async def get_saved_locations(user_id: int):
    try:
        async with (await get_pool()).acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, nickname, area, lat, lon, url, is_default
                FROM saved_locations
                WHERE user_id = $1
                ORDER BY is_default DESC, created_at ASC
                """,
                user_id,
            )
            return rows
    except Exception as e:
        print(f"[DB ERROR - get_saved_locations] {e}")
        return []


async def log_feedback(user_id: int, run_id: int, feedback: str):
    try:
        async with (await get_pool()).acquire() as conn:
            await conn.execute(
                """
                INSERT INTO insight_feedback (user_id, run_id, feedback)
                VALUES ($1, $2, $3)
                """,
                user_id, run_id, feedback,
            )
    except Exception as e:
        print(f"[DB ERROR - log_feedback] {e}")


async def get_community_insights(area: str):
    try:
        async with (await get_pool()).acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT COUNT(DISTINCT user_id) AS user_count,
                       MODE() WITHIN GROUP (ORDER BY condition) AS common_condition
                FROM weather_scraped
                WHERE area = $1
                  AND saved_at > NOW() - INTERVAL '1 hour'
                """,
                area,
            )
            if row and row["user_count"] >= 2:
                cond = row["common_condition"] or "similar conditions"
                return (
                    f"👥 {row['user_count']} users in your area checked weather recently "
                    f"— most seeing *{cond}*."
                )
            return None
    except Exception as e:
        print(f"[DB ERROR - community_insights] {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# CONTACT CAPTURE
# ─────────────────────────────────────────────────────────────────────────────

async def maybe_ask_contact(update, context, user_id: int) -> bool:
    """
    Checks whether the contact prompt should appear.
    Fires if the user has no contact saved AND this is their 3rd, 6th, 9th...
    weather check (divisible by 3). Sets contact_locked=True to block all
    other keyboard actions until the user submits or cancels.
    Returns True if the prompt was shown, False otherwise.
    """
    async with (await get_pool()).acquire() as conn:
        row = await conn.fetchrow(
            "SELECT weather_checks, contact FROM users WHERE user_id = $1",
            user_id,
        )

    if not row:
        return False

    checks  = row["weather_checks"] or 0
    contact = row["contact"]

    if contact is None and checks > 0 and checks % 3 == 0:
        keyboard = ReplyKeyboardMarkup(
            [
                [KeyboardButton("📩 Send Contact")],
                [KeyboardButton("❌ Cancel")],
            ],
            resize_keyboard=True,
            one_time_keyboard=True,
        )
        await update.effective_chat.send_message(
            "📩 Unlock early access.\n\nPlease share your Gmail or mobile number:",
            reply_markup=keyboard,
        )
        context.user_data["awaiting_contact"] = True
        context.user_data["contact_locked"]   = True
        return True

    return False


# ─────────────────────────────────────────────────────────────────────────────
# CORE WEATHER PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

# Shared aiohttp session — created once, reused for all HTTP calls
_http_session = None

async def get_http_session() -> aiohttp.ClientSession:
    global _http_session
    if _http_session is None or _http_session.closed:
        _http_session = aiohttp.ClientSession(
            headers={"User-Agent": "SkyUpdateBot"},
            timeout=aiohttp.ClientTimeout(total=10),
        )
    return _http_session


async def reverse_geocode(lat: float, lon: float):
    """Returns (city, area, state, area_string) from OpenStreetMap Nominatim."""
    url    = "https://nominatim.openstreetmap.org/reverse"
    params = {"lat": lat, "lon": lon, "format": "json", "zoom": 18, "addressdetails": 1}

    session = await get_http_session()
    async with session.get(url, params=params) as resp:
        data = await resp.json()
        addr = data.get("address", {})

        city  = (addr.get("city") or addr.get("town") or addr.get("municipality")
                 or addr.get("village") or "Unknown")
        area  = (addr.get("suburb") or addr.get("neighbourhood") or addr.get("quarter")
                 or addr.get("residential") or addr.get("hamlet") or addr.get("road")
                 or addr.get("postcode") or "Unknown")
        state = addr.get("state") or "Unknown"

        parts       = [p for p in [city, area, state] if p and p != "Unknown"]
        area_string = ", ".join(parts)
        return city, area, state, area_string


async def fetch_and_store_weather(user_id: int, lat: float, lon: float,
                                  area_string: str, session_id: str) -> str:
    """
    Cache check (30 min, weather.com only) → URL extraction → scraper run.
    Returns the URL string or "cached" if cache hit.
    """
    async with (await get_pool()).acquire() as conn:
        cache_row = await conn.fetchrow(
            """
            SELECT timestamp FROM weather_scraped
            WHERE area = $1
              AND timestamp > NOW() - INTERVAL '30 minutes'
              AND data_source = 'weather.com'
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            area_string,
        )
        if cache_row:
            print(f"[{user_id}] Cache hit — skipping pipeline")
            return "cached"

    print(f"[{user_id}] Cache miss — running pipeline")
    url = await asyncio.to_thread(get_weather_url, area_string)
    await asyncio.to_thread(run_scraper, lat, lon, url, user_id, area_string)
    return url


async def _get_user_output_mode(user_id: int) -> str:
    """Returns 'visual' or 'text' based on users.output_mode. Defaults to visual."""
    try:
        async with (await get_pool()).acquire() as conn:
            row = await conn.fetchrow("SELECT output_mode FROM users WHERE user_id = $1", user_id)
            return (row["output_mode"] or "visual") if row else "visual"
    except Exception:
        return "visual"




async def build_weather_message(user_id: int, area_string: str, nickname: str = None):
    """
    Returns (payload, keyboard, mode) where:
      mode == "visual" → payload is a BytesIO photo buffer  → use send_photo / reply_photo
      mode == "text"   → payload is a Markdown string       → use send_message / reply_text
    All callers should use the send_weather() helper below instead of calling
    send_photo / reply_text directly.
    """
    from datetime import date, datetime

    run_info     = await get_latest_run(user_id)
    weather_code = run_info["weather_code"] if run_info and run_info["weather_code"] else 0
    run_id       = run_info["run_id"]       if run_info else 0
    keyboard     = get_smart_keyboard(weather_code, run_id)
    mode         = await _get_user_output_mode(user_id)

    # ── TEXT mode — original behaviour ────────────────────────────────────
    if mode == "text":
        short_area     = area_string.split(",")[0].strip()
        location_label = f"📍 *{nickname}*" if nickname else f"📍 *{short_area}*"
        weather_text   = f"{location_label}\n" + await read_weather_db(user_id)
        community = await get_community_insights(area_string)
        if community:
            weather_text += f"\n{community}"
        return weather_text, keyboard, "text"

    # ── VISUAL mode — image card ───────────────────────────────────────────
    from weather_card import build_weather_card

    short_area   = area_string.split(",")[0].strip()
    display_area = nickname if nickname else short_area

    async with (await get_pool()).acquire() as conn:
        scraped = await conn.fetchrow(
            """
            SELECT ws.feels_like, ws.condition, ws.high, ws.low,
                   ws.wind_speed, ws.sunrise, ws.sunset,
                   ws.aqi_value, ws.aqi_category, ws.uv_index,
                   ws.moon_phase, ws.visibility,
                   ws.data_source, ws.timestamp AS scraped_at,
                   sr.id AS run_id
            FROM weather_scraped ws
            JOIN scraper_runs sr ON sr.id = ws.run_id
            WHERE ws.user_id = $1
              AND sr.area = $2
            ORDER BY ws.timestamp DESC
            LIMIT 1
            """,
            user_id, area_string,
        )
        if not scraped:
            # Fallback to text if no data
            short_area     = area_string.split(",")[0].strip()
            location_label = f"📍 *{nickname}*" if nickname else f"📍 *{short_area}*"
            weather_text   = f"{location_label}\n" + await read_weather_db(user_id)
            return weather_text, keyboard, "text"

        today  = date.today()
        rid    = scraped["run_id"]

        current = await conn.fetchrow(
            """SELECT temperature_2m, relative_humidity_2m, wind_speed_10m,
                      wind_gusts_10m, wind_direction_10m, uv_index, is_day,
                      us_aqi, scraped_aqi_value, scraped_aqi_category
               FROM current_weather WHERE run_id = $1 LIMIT 1""",
            rid,
        )
        daily = await conn.fetchrow(
            """SELECT temperature_2m_max, temperature_2m_min,
                      apparent_temperature_max, apparent_temperature_min,
                      weather_code_max, wind_speed_10m_max, wind_gusts_10m_max,
                      wind_direction_10m_dominant, uv_index_max,
                      daylight_duration, sunrise, sunset, precipitation_sum
               FROM daily_weather
               WHERE run_id = $1 AND date = $2 LIMIT 1""",
            rid, today,
        )
        rain_row = await conn.fetchrow(
            """SELECT MAX(precipitation_probability) AS max_prob
               FROM hourly_weather
               WHERE run_id = $1 AND DATE(timestamp) = $2 AND timestamp >= NOW()""",
            rid, today,
        )
        scraped_details = await conn.fetchrow(
            """SELECT humidity, pressure, dew_point
               FROM weather_scraped WHERE run_id = $1 LIMIT 1""",
            rid,
        )

    def deg_to_compass(deg):
        if deg is None: return ""
        return ["N","NE","E","SE","S","SW","W","NW"][round(deg/45)%8]

    def fmt_ampm(val):
        if val is None: return None
        if isinstance(val, str): return val
        try: return val.strftime("%I:%M %p").lstrip("0")
        except Exception: return str(val)

    from insights import _WMO_SHORT
    import re as _re

    # Temperature
    temp = None
    if current and current["temperature_2m"] is not None:
        temp = round(current["temperature_2m"])
    elif daily:
        temp = round(daily["temperature_2m_max"] or 0)

    # Condition
    condition = ""
    if daily and daily["weather_code_max"] is not None:
        cond_label, _ = _WMO_SHORT.get(daily["weather_code_max"], ("",""))
        condition = cond_label
    if not condition and scraped["condition"]:
        condition = scraped["condition"]

    # Wind
    wind_speed, wind_dir, wind_gusts = None, "", None
    if current and current.get("wind_speed_10m"):
        wind_speed  = round(current["wind_speed_10m"])
        wind_dir    = deg_to_compass(current.get("wind_direction_10m"))
        if current.get("wind_gusts_10m"):
            wind_gusts = round(current["wind_gusts_10m"])
    elif daily and daily.get("wind_speed_10m_max"):
        wind_speed  = round(daily["wind_speed_10m_max"])
        wind_dir    = deg_to_compass(daily.get("wind_direction_10m_dominant"))
        if daily.get("wind_gusts_10m_max"):
            wind_gusts = round(daily["wind_gusts_10m_max"])

    # UV — raw text directly from weather_scraped.uv_index (weather.com)
    uv = scraped.get("uv_index") or None

    # AQI — use scraped_aqi_value from current_weather (weather.com primary)
    # fallback to us_aqi from Open-Meteo if scraper missed it
    aqi, aqi_cat = None, ""
    cw_scraped_val = current.get("scraped_aqi_value") if current else None
    cw_scraped_cat = current.get("scraped_aqi_category") if current else None
    if cw_scraped_val:
        try: aqi = round(float(cw_scraped_val))
        except Exception: pass
        aqi_cat = cw_scraped_cat or ""
    if aqi is None and current and current.get("us_aqi") is not None:
        try: aqi = round(float(current["us_aqi"]))
        except Exception: pass
        aqi_cat = scraped.get("aqi_category") or ""
    if aqi is None and scraped["aqi_value"]:
        try: aqi = round(float(scraped["aqi_value"]))
        except Exception: pass
        aqi_cat = scraped.get("aqi_category") or ""

    # Rain
    rain_chance, rain_mm = None, None
    if rain_row and rain_row["max_prob"] is not None:
        rc = round(rain_row["max_prob"])
        if rc >= 10:
            rain_chance = rc
    if daily and daily.get("precipitation_sum"):
        try:
            mm = round(float(daily["precipitation_sum"]), 1)
            if mm > 0: rain_mm = mm
        except Exception: pass

    # Feels like
    feels_like = None
    if scraped["feels_like"]:
        try: feels_like = round(float(str(scraped["feels_like"]).replace("°","").strip()))
        except Exception: pass
    elif daily and daily.get("apparent_temperature_max"):
        feels_like = round(daily["apparent_temperature_max"])

    # H / L
    high, low = None, None
    if daily:
        high = round(daily["temperature_2m_max"] or 0)
        low  = round(daily["temperature_2m_min"] or 0)

    # Sunrise / Sunset / Daylight
    sunrise_str  = fmt_ampm((daily.get("sunrise") if daily else None) or scraped.get("sunrise"))
    sunset_str   = fmt_ampm((daily.get("sunset")  if daily else None) or scraped.get("sunset"))
    daylight_hrs = None
    if daily and daily.get("daylight_duration"):
        try: daylight_hrs = round(daily["daylight_duration"] / 3600, 1)
        except Exception: pass

    # Humidity / Pressure / Dew point
    humidity, pressure, dew_point = None, None, None
    if scraped_details and scraped_details.get("humidity"):
        # Prefer weather.com scraped humidity — strip "%" and round
        try: humidity = round(float(str(scraped_details["humidity"]).replace("%", "").strip()))
        except Exception: pass
    if humidity is None and current and current.get("relative_humidity_2m") is not None:
        # Fallback to Open-Meteo only when scrape was unavailable
        humidity = round(current["relative_humidity_2m"])
    if scraped_details:
        if scraped_details.get("pressure"):
            try: pressure = round(float(scraped_details["pressure"]))
            except Exception: pass
        if scraped_details.get("dew_point"):
            try: dew_point = round(float(str(scraped_details["dew_point"]).replace("°","")))
            except Exception: pass

    # Freshness footer
    updated_str = ""
    scraped_at = scraped.get("scraped_at")
    if scraped_at:
        try:
            if hasattr(scraped_at, "tzinfo") and scraped_at.tzinfo:
                scraped_at = scraped_at.replace(tzinfo=None)
            age = int((datetime.now() - scraped_at).total_seconds() / 60)
            updated_str = "Updated just now" if age < 1 else (
                f"Updated {age} min ago" if age < 60 else f"Updated {age//60}h ago"
            )
        except Exception:
            pass

    # Visibility — stored as TEXT e.g. "10 km", extract the number
    visibility = None
    raw_vis = scraped.get("visibility")
    if raw_vis:
        try:
            visibility = round(float(str(raw_vis).replace("km","").replace("mi","").strip()), 1)
        except Exception:
            pass

    # Moon phase — directly from weather_scraped
    moon_phase = scraped.get("moon_phase") or None

    # Stale data warning — if scraped_at > 3 hours ago, flag it
    stale_warning = ""
    if scraped_at:
        try:
            age_mins = int((datetime.datetime.now() - (scraped_at.replace(tzinfo=None) if hasattr(scraped_at, "tzinfo") and scraped_at.tzinfo else scraped_at)).total_seconds() / 60)
            if age_mins > 180:
                stale_warning = f"⚠️ Data from {age_mins//60}h ago"
        except Exception:
            pass

    card_data = {
        "temperature":  temp or 0,
        "condition":    condition,
        "area":         display_area,
        "humidity":     humidity,
        "wind_speed":   wind_speed,
        "wind_dir":     wind_dir,
        "wind_gusts":   wind_gusts,
        "uv_index":     uv,
        "feels_like":   feels_like,
        "aqi":          aqi,
        "aqi_category": aqi_cat,
        "rain_chance":  rain_chance,
        "rain_mm":      rain_mm,
        "high":         high,
        "low":          low,
        "sunrise":      sunrise_str,
        "sunset":       sunset_str,
        "daylight_hrs":  daylight_hrs,
        "pressure":     pressure,
        "dew_point":    dew_point,
        "visibility":   visibility,
        "moon_phase":   moon_phase,
        "data_source":  scraped.get("data_source",""),
        "updated":      (stale_warning + "  " + updated_str).strip() if stale_warning else updated_str,
    }

    photo_buf = build_weather_card(card_data)
    return photo_buf, keyboard, "visual"


async def send_weather(target, payload, keyboard, mode: str):
    """
    Universal weather send helper.
    target = update.effective_chat / update.callback_query.message / query.message
    """
    if mode == "visual":
        await target.reply_photo(photo=payload, reply_markup=keyboard)
    else:
        await target.reply_text(payload, parse_mode="Markdown", reply_markup=keyboard)


# ─────────────────────────────────────────────────────────────────────────────
# POST-FETCH FLOW HELPER
# Called after every successful weather fetch to show the choice prompt and
# optionally show the feedback keyboard (every 4th successful run).
# ─────────────────────────────────────────────────────────────────────────────

async def _store_insight_history(user_id: int, area: str, triggered_tiers: list):
    """
    Stores today's triggered insight tier numbers in insight_history table.
    Used to detect multi-day streaks (e.g. bad AQI 4 days in a row).
    """
    try:
        async with (await get_pool()).acquire() as conn:
            import json as _json
            await conn.execute(
                """
                INSERT INTO insight_history (user_id, area, insight_date, tiers_json)
                VALUES ($1, $2, CURRENT_DATE, $3)
                ON CONFLICT (user_id, area, insight_date) DO UPDATE
                    SET tiers_json = EXCLUDED.tiers_json
                """,
                user_id, area, _json.dumps(triggered_tiers),
            )
    except Exception as e:
        print(f"[DB ERROR - insight_history] {e}")


async def _get_streak_context(user_id: int, area: str, tier: int) -> str:
    """
    Returns a streak context string if the same tier has been active
    for 3+ consecutive days. E.g. "⚠️ AQI has been unhealthy 4 days in a row."
    Returns "" if no streak.
    """
    try:
        async with (await get_pool()).acquire() as conn:
            import json as _json
            rows = await conn.fetch(
                """
                SELECT tiers_json FROM insight_history
                WHERE user_id = $1 AND area = $2
                  AND insight_date >= CURRENT_DATE - INTERVAL '7 days'
                ORDER BY insight_date DESC
                LIMIT 7
                """,
                user_id, area,
            )

        if len(rows) < 3:
            return ""

        streak = 0
        for row in rows:
            tiers = _json.loads(row["tiers_json"] or "[]")
            if tier in tiers:
                streak += 1
            else:
                break

        if streak >= 3:
            tier_labels = {1: "dangerous conditions", 2: "rain", 3: "poor air quality",
                           4: "high UV", 5: "strong winds"}
            label = tier_labels.get(tier, "these conditions")
            return f"\n\n📊 _Streak: {label} has been active {streak} days in a row._"
        return ""
    except Exception as e:
        print(f"[DB ERROR - get_streak] {e}")
        return ""


async def post_fetch_flow(update, context, user_id: int, area_string: str):
    """
    Increments counters, then ALWAYS shows the weather/insight choice keyboard first.
    Feedback and contact prompts are queued to fire AFTER the user picks weather or
    insights — never before. This ensures the user always gets their content first.
    """
    successful_runs = await increment_weather_count(user_id)

    # Queue feedback for after the choice — don't show it yet
    if successful_runs % 4 == 0 and successful_runs > 0:
        run_info = await get_latest_run(user_id)
        run_id   = run_info["run_id"] if run_info else 0
        context.user_data["feedback_run_id"]       = run_id
        context.user_data["feedback_pending_post"] = True  # show AFTER weather/insights

    # Always show the choice as an inline keyboard — disappears when tapped
    context.user_data["choice_pending"] = True
    context.user_data["pending_area"]   = area_string
    nickname = context.user_data.get("pending_nickname")
    await update.effective_chat.send_message(
        "What would you like to see?",
        reply_markup=build_choice_inline(area_string, nickname),
    )
    # Contact prompt is now called from _post_choice_prompts after content is shown


# ─────────────────────────────────────────────────────────────────────────────
# /start
# ─────────────────────────────────────────────────────────────────────────────

async def _send_main_menu(target, user_id: int, parse_mode: str = "Markdown"):
    """
    Single source of truth for the main menu message.
    Used by /start, the 🏠 Main Menu reply button, and the main_menu inline callback.
    target must have a reply_text (message) or send_message (bot/chat) method.
    """
    saved = await get_saved_locations(user_id)
    saved_rows = []

    # Quick weather button for default location only — no duplicate saved location buttons
    default_loc = next((loc for loc in saved if loc["is_default"]), None)
    if default_loc:
        area_cb = default_loc["area"].replace("|", "_")[:40]
        nick    = default_loc["nickname"] or default_loc["area"].split(",")[0]
        saved_rows.append([InlineKeyboardButton(
            f"🌤️ Quick weather — {nick}",
            callback_data=f"choice_weather|{area_cb}|{nick}",
        )])

    inline_kb = InlineKeyboardMarkup(saved_rows) if saved_rows else None

    text = (
        "🌤️ *Welcome to SkyUpdate*\n\n"
        "Get started:\n"
        "/sharelocation — share your GPS location\n"
        "/settings — saved locations, alerts, appearance\n\n"
        "Forecast:\n"
        "/insights — full insights for your location\n"
        "/radar — 8-hour rain chart\n\n"
        "/ask <question> — ask anything about your weather\n\n"
        "More:\n"
        "/stats — your weather check stats\n"
        "/remind — event weather reminder\n"
        "/pause — pause morning alerts\n\n"
        "_Tap 🏠 Main Menu any time to come back here._"
    )

    await target.reply_text(text, reply_markup=inline_kb, parse_mode=parse_mode)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user  = update.effective_user
    await log_activity(user, "start")

    # Check if brand new user BEFORE upserting
    is_new = False
    try:
        async with (await get_pool()).acquire() as conn:
            row = await conn.fetchrow(
                "SELECT user_id FROM users WHERE user_id = $1", user.id
            )
            is_new = row is None
    except Exception as e:
        print(f"[DB ERROR - start new user check] {e}")

    await upsert_customer(user)

    # Send welcome card only on first ever /start
    if is_new:
        try:
            from welcome_card import build_welcome_card
            buf = build_welcome_card(user.first_name or "there")
            await update.message.reply_photo(photo=buf)
        except Exception as e:
            print(f"[ERROR - welcome card] {e}")
            # Card failed — skip silently, don't crash

    await _send_main_menu(update.message, user.id)
    await update.message.reply_text("👇 Use the button below anytime.", reply_markup=build_main_keyboard())

async def sharelocation_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sends a one-time location request button."""
    await update.message.reply_text(
        "📍 Tap below to share your location:",
        reply_markup=ReplyKeyboardMarkup(
            [[KeyboardButton("📍 Share My Location", request_location=True)]],
            resize_keyboard=True,
            one_time_keyboard=True,
        ),
    )


async def savedlocations_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shows saved locations inline — same as tapping 💾 Saved Locations."""
    user = update.effective_user
    await _show_saved_locations(update.message, user.id)


# ─────────────────────────────────────────────────────────────────────────────
# LOCATION HANDLER — shared GPS location
# ─────────────────────────────────────────────────────────────────────────────

async def _log_scraper_error(user_id: int, area: str, error_msg: str):
    """Logs scraper failures to scraper_errors table for operational monitoring."""
    try:
        async with (await get_pool()).acquire() as conn:
            await conn.execute(
                """
                INSERT INTO scraper_errors (user_id, area, error_msg, timestamp)
                VALUES ($1, $2, $3, NOW())
                """,
                user_id, area, error_msg[:500],
            )
    except Exception as e:
        print(f"[DB ERROR - scraper_errors] {e}")


async def _check_rate_limit(user_id: int) -> int:
    """
    Returns seconds since last scraper request for this user.
    Returns 999 if no record exists (first request).
    """
    try:
        async with (await get_pool()).acquire() as conn:
            row = await conn.fetchrow(
                "SELECT ran_at FROM scraper_runs WHERE user_id = $1 ORDER BY ran_at DESC LIMIT 1",
                user_id,
            )
        if not row:
            return 999
        age = (datetime.datetime.now() - row["ran_at"].replace(tzinfo=None)).total_seconds()
        return int(age)
    except Exception:
        return 999


async def location_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user       = update.effective_user
    loc        = update.message.location
    lat, lon   = loc.latitude, loc.longitude
    session_id = str(uuid.uuid4())

    city, area, state, area_string = await reverse_geocode(lat, lon)
    await upsert_customer(user)

    # Rate limit — if last scrape was <30s ago, serve cached data instead
    age_secs = await _check_rate_limit(user.id)
    if age_secs < 30:
        await update.message.reply_text(
            f"⏳ Just fetched {age_secs}s ago — your data is still fresh! Showing latest.",
            reply_markup=build_main_keyboard(),
        )
        await post_fetch_flow(update, context, user.id, area_string)
        return

    waiting = await update.message.reply_text(_waiting_line())

    try:
        url = await fetch_and_store_weather(user.id, lat, lon, area_string, session_id)

        run_info  = await get_latest_run(user.id)
        run_id    = run_info["run_id"] if run_info else None
        condition = await get_weather_code_for_run(run_id) if run_id else None

        asyncio.create_task(log_activity(user, "location_shared", area=area_string, lat=lat, lon=lon,
                           url_requested=url, session_id=session_id, condition=condition))

        # Replace the waiting message with a simple confirmation — no weather card yet.
        # The user picks what they want to see via the choice keyboard below.
        await waiting.edit_text(f"✅ Got your location — *{_short_name(area_string)}*", parse_mode="Markdown")

        # Store context for save-location conversation and choice flow
        context.user_data["pending_area"]    = area_string
        context.user_data["pending_lat"]     = lat
        context.user_data["pending_lon"]     = lon
        context.user_data["pending_url"]     = url
        context.user_data["pending_session"] = session_id
        context.user_data["pending_nickname"] = None  # no nickname for raw GPS shares

        await post_fetch_flow(update, context, user.id, area_string)

    except Exception as e:
        print(f"[ERROR - location_handler] user={user.id} error={e}")
        await _log_scraper_error(user.id, area_string, str(e))
        await waiting.edit_text("⚠️ Something went wrong fetching your weather. Please try again.")


# ─────────────────────────────────────────────────────────────────────────────
# MANUAL LAT,LON HANDLER
# ─────────────────────────────────────────────────────────────────────────────

async def manual_latlon(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user       = update.effective_user
    session_id = str(uuid.uuid4())

    try:
        lat, lon = map(float, update.message.text.split(","))
    except ValueError:
        await update.message.reply_text(
            "❌ Invalid format. Use: `28.6156,77.3910`", parse_mode="Markdown"
        )
        return

    city, area, state, area_string = await reverse_geocode(lat, lon)
    await upsert_customer(user)

    # Rate limit check
    age_secs = await _check_rate_limit(user.id)
    if age_secs < 30:
        await update.message.reply_text(
            f"⏳ Just fetched {age_secs}s ago — your data is still fresh! Showing latest.",
            reply_markup=build_main_keyboard(),
        )
        await post_fetch_flow(update, context, user.id, area_string)
        return

    waiting = await update.message.reply_text(_waiting_line())

    try:
        url = await fetch_and_store_weather(user.id, lat, lon, area_string, session_id)

        run_info  = await get_latest_run(user.id)
        run_id    = run_info["run_id"] if run_info else None
        condition = await get_weather_code_for_run(run_id) if run_id else None

        await log_activity(user, "manual_latlon", area=area_string, lat=lat, lon=lon,
                           url_requested=url, session_id=session_id, condition=condition)

        await waiting.edit_text(f"✅ Got your location — *{_short_name(area_string)}*", parse_mode="Markdown")

        context.user_data["pending_area"]     = area_string
        context.user_data["pending_lat"]      = lat
        context.user_data["pending_lon"]      = lon
        context.user_data["pending_url"]      = url
        context.user_data["pending_session"]  = session_id
        context.user_data["pending_nickname"] = None

        await post_fetch_flow(update, context, user.id, area_string)

    except Exception as e:
        print(f"[ERROR - manual_latlon] user={user.id} error={e}")
        await _log_scraper_error(user.id, area_string, str(e))
        await waiting.edit_text("⚠️ Something went wrong fetching your weather. Please try again.")



# ─────────────────────────────────────────────────────────────────────────────
# POST-CHOICE PROMPTS HELPER
# Called AFTER weather or insights content has been sent. Fires any queued
# feedback or contact prompts in order, then restores the main keyboard.
# ─────────────────────────────────────────────────────────────────────────────

async def _post_choice_prompts(update, context, user):
    """
    Shows feedback keyboard (if queued on this run) and/or contact prompt
    (if due), always AFTER the user has already seen their weather/insights.
    Restores the main keyboard if neither prompt fires.
    """
    showed_something = False

    # Feedback — queued by post_fetch_flow on every 4th run
    if context.user_data.pop("feedback_pending_post", False):
        run_id = context.user_data.get("feedback_run_id", 0)
        context.user_data["awaiting_feedback"] = True
        await update.effective_chat.send_message(
            "Was today's weather info helpful? Your feedback helps us improve 🙏",
            reply_markup=build_feedback_keyboard(),
        )
        showed_something = True

    # Contact — only fires if feedback was NOT shown this turn (avoid double-prompting)
    if not showed_something:
        contact_shown = await maybe_ask_contact(update, context, user.id)
        if contact_shown:
            showed_something = True

    if not showed_something:
        await update.effective_chat.send_message(
            "Back to main menu 👇", reply_markup=build_main_keyboard()
        )



# ─────────────────────────────────────────────────────────────────────────────
# POST-CHOICE PROMPTS — INLINE VERSION
# For choice callbacks (choice_weather / choice_insights) which run in an
# inline callback context rather than a message context.
# ─────────────────────────────────────────────────────────────────────────────

async def _post_choice_prompts_inline(query, context, user):
    """
    Same logic as _post_choice_prompts but uses query.message.reply_text
    instead of update.effective_chat.send_message.
    """
    showed_something = False

    if context.user_data.pop("feedback_pending_post", False):
        run_id = context.user_data.get("feedback_run_id", 0)
        context.user_data["awaiting_feedback"] = True
        await query.message.reply_text(
            "Was today's weather info helpful? Your feedback helps us improve 🙏",
            reply_markup=build_feedback_keyboard(),
        )
        showed_something = True

    # Contact check inline — only fires if feedback was NOT shown this turn
    if not showed_something:
        async with (await get_pool()).acquire() as conn:
            row = await conn.fetchrow(
                "SELECT weather_checks, contact FROM users WHERE user_id = $1", user.id
            )

        if row:
            checks  = row["weather_checks"] or 0
            contact = row["contact"]
            if contact is None and checks > 0 and checks % 3 == 0:
                keyboard = ReplyKeyboardMarkup(
                    [[KeyboardButton("📩 Send Contact")], [KeyboardButton("❌ Cancel")]],
                    resize_keyboard=True, one_time_keyboard=True,
                )
                await query.message.reply_text(
                    "📩 Unlock early access.\n\nPlease share your Gmail or mobile number:",
                    reply_markup=keyboard,
                )
                context.user_data["awaiting_contact"] = True
                context.user_data["contact_locked"]   = True
                showed_something = True

    if not showed_something:
        await query.message.reply_text("Back to main menu 👇", reply_markup=build_main_keyboard())


# ─────────────────────────────────────────────────────────────────────────────
# MAIN TEXT HANDLER — routes all reply-keyboard interactions
# ─────────────────────────────────────────────────────────────────────────────

async def log_any_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Central router for all text messages.
    Priority order:
      1. Contact locked — intercept everything
      2. Feedback awaiting — intercept 👍/👎/skip
      3. Choice pending — intercept weather/insight choice
      4. Saved location buttons
      5. My Saved Locations button
      6. Generic activity log
    """
    user = update.effective_user
    text = update.message.text.strip() if update.message.text else ""

    # ── 0. Alert time awaiting ────────────────────────────────────────────
    if context.user_data.get("awaiting_alert_time"):
        import re as _re, datetime as _dt
        # Accept: 7:00  07:00  7:30  (AM suffix optional and ignored — all times are AM)
        m = _re.match(r"^(\d{1,2}):(\d{2})(?:\s*[Aa][Mm])?$", text.strip())
        if not m:
            await update.message.reply_text(
                "❌ Couldn't read that time. Please reply like `7:00` or `11:30`.",
                parse_mode="Markdown",
            )
            return

        hour, minute = int(m.group(1)), int(m.group(2))

        # Enforce 6:00 AM – 11:59 AM only
        if not (6 <= hour <= 11 and 0 <= minute <= 59):
            await update.message.reply_text(
                "⏰ Please choose a time between *6:00* and *11:59* (morning only).",
                parse_mode="Markdown",
            )
            return

        try:
            # Pass a datetime.time object — required by asyncpg for TIME columns
            alert_t = _dt.time(hour, minute)
            async with (await get_pool()).acquire() as conn:
                await conn.execute(
                    "UPDATE users SET alert_time = $1 WHERE user_id = $2",
                    alert_t, user.id,
                )
            context.user_data["awaiting_alert_time"] = False
            await update.message.reply_text(
                f"✅ Morning alert set for *{hour:02d}:{minute:02d} AM*.",
                parse_mode="Markdown",
                reply_markup=build_main_keyboard(),
            )
        except Exception as e:
            print(f"[ERROR - set_alert_time] {e}")
            await update.message.reply_text("⚠️ Couldn't save your alert time. Please try again.")
        return

    # ── 0b. Rename location awaiting ──────────────────────────────────────
    if context.user_data.get("awaiting_rename_loc_id"):
        loc_id = context.user_data.pop("awaiting_rename_loc_id")
        new_nick = text.strip()[:50]
        if not new_nick:
            await update.message.reply_text("❌ Name can't be empty. Try again.")
            context.user_data["awaiting_rename_loc_id"] = loc_id
            return
        try:
            async with (await get_pool()).acquire() as conn:
                await conn.execute(
                    "UPDATE saved_locations SET nickname = $1 WHERE id = $2 AND user_id = $3",
                    new_nick, loc_id, user.id,
                )
            await update.message.reply_text(
                f"✅ Renamed to *{new_nick}*.",
                parse_mode="Markdown",
                reply_markup=build_main_keyboard(),
            )
        except Exception as e:
            print(f"[ERROR - rename_loc] {e}")
            await update.message.reply_text("⚠️ Couldn't rename. Try again.")
        return

    # ── 1. Contact locked ──────────────────────────────────────────────────
    if context.user_data.get("contact_locked"):
        if text == "❌ Cancel":
            context.user_data["awaiting_contact"] = False
            context.user_data["contact_locked"]   = False
            await update.message.reply_text(
                "Cancelled.", reply_markup=build_main_keyboard()
            )
            return

        if text == "📩 Send Contact":
            await update.message.reply_text(
                "Enter your @gmail.com or 10-digit mobile:"
            )
            return

        normalized = normalize_contact(text)
        if not normalized:
            await update.message.reply_text(
                "❌ Enter a valid @gmail.com or 10-digit mobile.\nOr tap Cancel."
            )
            return

        async with (await get_pool()).acquire() as conn:
            await conn.execute(
                "UPDATE users SET contact = $1 WHERE user_id = $2",
                normalized, user.id,
            )

        context.user_data["awaiting_contact"] = False
        context.user_data["contact_locked"]   = False
        await update.message.reply_text(
            "✅ Contact saved!", reply_markup=build_main_keyboard()
        )
        return

    # ── 2. Feedback awaiting ───────────────────────────────────────────────
    if context.user_data.get("awaiting_feedback"):
        run_id = context.user_data.get("feedback_run_id", 0)

        if text == "👍 Helpful":
            await log_feedback(user.id, run_id, "positive")
            await log_activity(user, "feedback_positive")
            await update.message.reply_text(
                "Thanks! Glad it was helpful 👍", reply_markup=build_main_keyboard()
            )
        elif text == "👎 Not helpful":
            await log_feedback(user.id, run_id, "negative")
            await log_activity(user, "feedback_negative")
            await update.message.reply_text(
                "Thanks for the feedback — we'll improve! 👎",
                reply_markup=build_main_keyboard(),
            )
        else:
            # "⏭️ Skip feedback" or anything else clears the state
            await update.message.reply_text(
                "No problem! 👌", reply_markup=build_main_keyboard()
            )

        context.user_data["awaiting_feedback"] = False
        context.user_data["feedback_run_id"]   = None
        return

    # ── 3. 🏠 Main Menu button ─────────────────────────────────────────────
    if text == "🏠 Main Menu":
        await _send_main_menu(update.message, user.id)
        return

    # ── 4. Generic activity log ────────────────────────────────────────────
    url = text if text.startswith("http://") or text.startswith("https://") else None
    await log_activity(user, "message", detail=text, url_requested=url)
    await upsert_customer(user)


# ─────────────────────────────────────────────────────────────────────────────
# INLINE BUTTON CALLBACKS
# ─────────────────────────────────────────────────────────────────────────────


async def insights_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await update.callback_query.answer("💡 Loading insights…")
    asyncio.create_task(upsert_customer(user))
    asyncio.create_task(log_activity(user, "insights_button"))

    area = await get_user_last_area(user.id)
    if not area:
        await update.callback_query.message.reply_text(
            "⚠️ No location found. Please share your location first."
        )
        return

    try:
        area_label      = f"📍 *{_short_name(area)}*\n\n"
        (visible, hidden), bonus = await asyncio.gather(
            generate_insights_split(user.id, area),
            generate_bonus_insights(user.id, area),
        )
        full_insights   = visible + ("\n\n" + hidden if hidden else "")
        full_text       = area_label + full_insights
        if bonus:
            full_text += "\n\n" + bonus
        _tier_map = {"🔥":1,"🚨":1,"⛈":1,"❄":1,"🥶":1,"🌨":1,
                     "🌧":2,"🌦":2,"⚠":3,"😷":3,"🕶":4,"☀":4,"💨":5,"🌬":5}
        _tiers_today = list({v for k,v in _tier_map.items() if k in full_insights})
        history = await get_streak_history(user.id, area)
        await _store_insight_history(user.id, area, _tiers_today)
        streak_ctx = build_streak_context(history, _tiers_today)
        full_text += streak_ctx
        await update.callback_query.message.reply_text(
            full_text,
            reply_markup=_build_insights_keyboard(False, area),
        )
    except Exception as e:
        print(f"[ERROR - insights_callback] user={user.id} error={e}")
        await update.callback_query.message.reply_text(
            "⚠️ Couldn't load insights right now. Please try again shortly."
        )


async def insights_show_more_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Reveals the hidden tier 3–6 insights when user taps ➕ Show more.
    Area is encoded in callback_data as 'insights_show_more|area' so this
    works even after a bot restart (context.user_data would be empty).
    Falls back to re-running the engine from DB if memory is gone.
    """
    query = update.callback_query
    await query.answer()

    # Parse area from callback_data (format: "insights_show_more|area")
    parts = query.data.split("|", 1)
    area_from_cb = parts[1] if len(parts) > 1 else ""

    hidden = context.user_data.pop("insights_hidden", "")

    if not hidden and area_from_cb:
        # Memory lost (bot restarted) — re-run the engine to get hidden insights
        try:
            user = update.effective_user
            _, hidden = await generate_insights_split(user.id, area_from_cb)
        except Exception as e:
            print(f"[ERROR - insights_show_more re-run] {e}")
            hidden = ""

    if hidden:
        # Show more is only reachable in text mode (visual card already shows all)
        await query.message.reply_text(hidden, reply_markup=_build_insights_keyboard(False))
    else:
        await query.answer("Nothing more to show.", show_alert=True)




async def tomorrow_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await update.callback_query.answer("📅 Loading tomorrow's forecast…")
    await log_activity(user, "tomorrow_button")

    try:
        text = await generate_tomorrow_forecast(user.id)
        await update.callback_query.message.reply_text(text)
    except Exception as e:
        print(f"[ERROR - tomorrow_callback] {e}")
        await update.callback_query.message.reply_text(
            "⚠️ Couldn't load tomorrow's forecast right now."
        )


async def show_weather_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Inline 'See Weather' button — shown below an insights message."""
    user = update.effective_user
    await update.callback_query.answer()
    area = await get_user_last_area(user.id)
    if not area:
        await update.callback_query.message.reply_text(
            "⚠️ No location found. Share your location first."
        )
        return
    payload, keyboard, mode = await build_weather_message(user.id, area)
    await send_weather(update.callback_query.message, payload, keyboard, mode)


async def weekly_forecast_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handles the 📅 7-Day / 📆 7-Day Forecast inline button.
    Sends compact overview + per-day expand buttons.
    Stores days_data in context.user_data so expand callbacks can read tips
    without touching the DB again.
    """
    user = update.effective_user
    await update.callback_query.answer("📆 Loading 7-day forecast…")
    await log_activity(user, "weekly_forecast_button")

    try:
        compact_text, days_data = await generate_weekly_forecast(user.id)

        # Build one row of buttons per day so user can tap to expand
        rows = []
        for day in days_data:
            label = f"📅 {day['label']} ({day['date_str']})"
            rows.append([InlineKeyboardButton(label, callback_data=f"weekly_expand_{day['index']}")])

        keyboard = InlineKeyboardMarkup(rows) if rows else None

        # Cache days_data so expand callbacks don't need to re-query the DB
        context.user_data["weekly_days_data"] = days_data

        await update.callback_query.message.reply_text(
            compact_text,
            parse_mode="Markdown",
            reply_markup=keyboard,
        )
    except Exception as e:
        print(f"[ERROR - weekly_forecast_callback] {e}")
        await update.callback_query.message.reply_text(
            "⚠️ Couldn't load the weekly forecast right now. Try again shortly."
        )


async def weekly_expand_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handles per-day expand buttons (weekly_expand_0 … weekly_expand_6).
    Reads pre-cached days_data from context.user_data and renders a full
    detail card for the selected day. No DB call needed.

    Card format:
        📅 Wednesday (15 Mar) — Full Detail

        🌡️ Actual: 19–34°C  |  Feels like: 17–38°C
        ⛅ Partly Cloudy

        🌧️ Rain: 12mm over 3 hrs
        💨 Wind: up to 45 km/h (gusts 62 km/h) from NE
        ☀️ UV: 9 (Very High)
        🌅 Sunrise 6:02 AM  |  Sunset 6:28 PM  (12.4 hrs daylight)

        • 🔥 Feels like 38°C — dangerous heat midday
        • ☂️ Carry an umbrella (12mm expected)
        ...
    """
    def deg_to_compass(deg) -> str:
        """Convert wind direction degrees to compass label."""
        if deg is None:
            return "—"
        dirs = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
        return dirs[round(deg / 45) % 8]

    def uv_label(uv) -> str:
        if uv is None: return "—"
        if uv <= 2:  return "Low"
        if uv <= 5:  return "Moderate"
        if uv <= 7:  return "High"
        if uv <= 10: return "Very High"
        return "Extreme"

    def fmt_time_str(val) -> str:
        """Format a datetime or ISO string to 12-hour clock."""
        if val is None: return "—"
        from datetime import datetime as _dt
        if isinstance(val, str):
            try: val = _dt.fromisoformat(val)
            except Exception: return val
        return val.strftime("%I:%M %p").lstrip("0")

    query = update.callback_query
    await query.answer()

    try:
        idx       = int(query.data.split("_")[-1])
        days_data = context.user_data.get("weekly_days_data", [])
        day       = next((d for d in days_data if d["index"] == idx), None)

        if not day:
            await query.message.reply_text("⚠️ Day data not found. Tap 📆 7-Day again.")
            return

        r    = day["row"]
        tips = day["tips"]

        # ── Temperatures ──────────────────────────────────────────────────
        tmax  = round(r.get("temperature_2m_max") or 0)
        tmin  = round(r.get("temperature_2m_min") or 0)
        fmax  = round(r.get("apparent_temperature_max") or 0)
        fmin  = round(r.get("apparent_temperature_min") or 0)
        wmo   = r.get("weather_code_max") or 0
        from insights import _WMO_SHORT
        cond_label, cond_emoji = _WMO_SHORT.get(wmo, ("Mixed", "🌡️"))

        # ── Precipitation ─────────────────────────────────────────────────
        rain_mm    = r.get("rain_sum") or r.get("precipitation_sum") or 0
        snow_mm    = r.get("snowfall_sum") or 0
        precip_hrs = r.get("precipitation_hours") or 0

        # ── Wind ──────────────────────────────────────────────────────────
        wind_max  = round(r.get("wind_speed_10m_max") or 0)
        gust_max  = round(r.get("wind_gusts_10m_max") or 0)
        wind_dir  = deg_to_compass(r.get("wind_direction_10m_dominant"))

        # ── UV ────────────────────────────────────────────────────────────
        uv = round(r.get("uv_index_max") or 0, 1)

        # ── Daylight ──────────────────────────────────────────────────────
        daylight_secs  = r.get("daylight_duration") or 0
        daylight_hrs   = round(daylight_secs / 3600, 1)
        sunrise_str    = fmt_time_str(r.get("sunrise"))
        sunset_str     = fmt_time_str(r.get("sunset"))

        # ── Build card ────────────────────────────────────────────────────
        lines = [
            f"📅 *{day['label']} ({day['date_str']}) — Full Detail*\n",
            f"🌡️ Actual: {tmin}–{tmax}°C  |  Feels like: {fmin}–{fmax}°C",
            f"{cond_emoji} {cond_label}\n",
        ]

        # Rain / snow block
        if rain_mm >= 1:
            hrs_str = f"over {round(precip_hrs)} hr{'s' if precip_hrs != 1 else ''}" if precip_hrs >= 1 else ""
            lines.append(f"🌧️ Rain: {round(rain_mm, 1)}mm{' ' + hrs_str if hrs_str else ''}")
        if snow_mm >= 1:
            lines.append(f"❄️ Snow: {round(snow_mm, 1)}mm")
        if rain_mm < 1 and snow_mm < 1:
            lines.append("☀️ No significant precipitation expected")

        # Wind block
        if gust_max > 0:
            lines.append(f"💨 Wind: up to {wind_max} km/h (gusts {gust_max} km/h) from {wind_dir}")
        else:
            lines.append(f"💨 Wind: up to {wind_max} km/h from {wind_dir}")

        # UV block
        lines.append(f"☀️ UV: {uv} ({uv_label(uv)})")

        # Daylight block
        lines.append(f"🌅 Sunrise {sunrise_str}  |  Sunset {sunset_str}  ({daylight_hrs} hrs daylight)\n")

        # Actionable tips
        if tips:
            for tip in tips:
                lines.append(f"• {tip}")
        else:
            lines.append("✅ No specific alerts for this day")

        await query.message.reply_text("\n".join(lines), parse_mode="Markdown")

    except Exception as e:
        print(f"[ERROR - weekly_expand_callback] {e}")
        await query.message.reply_text("⚠️ Couldn't expand this day. Try again.")


async def main_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🏠 Main Menu inline button — identical to /start."""
    user = update.effective_user
    await update.callback_query.answer()
    await _send_main_menu(update.callback_query.message, user.id)


async def choice_weather_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Inline 🌤️ Weather button — area encoded in callback_data, no user_data needed."""
    user  = update.effective_user
    query = update.callback_query
    await query.answer()

    # Parse: "choice_weather|area_string|nickname"
    parts       = query.data.split("|", 2)
    area_string = parts[1] if len(parts) > 1 else ""
    nickname    = parts[2] if len(parts) > 2 and parts[2] else None

    if not area_string:
        await query.message.reply_text("⚠️ No location found. Please share your location first.")
        return

    context.user_data["choice_pending"] = False

    try:
        payload, inline_kb, mode = await build_weather_message(user.id, area_string, nickname=nickname)
        await send_weather(query.message, payload, inline_kb, mode)
    except Exception as e:
        print(f"[ERROR - choice_weather_callback] {e}")
        await query.message.reply_text("⚠️ Couldn't load weather. Try again shortly.")

    await _post_choice_prompts_inline(query, context, user)


async def choice_insights_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Inline 💡 Insights button — area encoded in callback_data, no user_data needed."""
    user  = update.effective_user
    query = update.callback_query
    await query.answer()

    # Parse: "choice_insights|area_string|nickname"
    parts       = query.data.split("|", 2)
    area_string = parts[1] if len(parts) > 1 else ""
    nickname    = parts[2] if len(parts) > 2 and parts[2] else None

    if not area_string:
        await query.message.reply_text("⚠️ No location found. Please share your location first.")
        return

    context.user_data["choice_pending"] = False

    try:
        label           = f"📍 *{_short_name(area_string, nickname)}*"
        (visible, hidden), bonus = await asyncio.gather(
            generate_insights_split(user.id, area_string),
            generate_bonus_insights(user.id, area_string),
        )
        full_insights   = visible + ("\n\n" + hidden if hidden else "")
        full_text       = f"{label}\n\n{full_insights}"
        if bonus:
            full_text += "\n\n" + bonus
        _tier_map = {"🔥":1,"🚨":1,"⛈":1,"❄":1,"🥶":1,"🌨":1,
                     "🌧":2,"🌦":2,"⚠":3,"😷":3,"🕶":4,"☀":4,"💨":5,"🌬":5}
        _tiers_today = list({v for k,v in _tier_map.items() if k in full_insights})
        history = await get_streak_history(user.id, area_string)
        await _store_insight_history(user.id, area_string, _tiers_today)
        streak_ctx = build_streak_context(history, _tiers_today)
        full_text += streak_ctx
        await query.message.reply_text(full_text, reply_markup=_build_insights_keyboard(False, area_string))
    except Exception as e:
        print(f"[ERROR - choice_insights_callback] {e}")
        await query.message.reply_text("⚠️ Couldn't load insights. Try again shortly.")

    await _post_choice_prompts_inline(query, context, user)


async def feedback_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles legacy inline feedback buttons (kept for backward compatibility)."""
    user = update.effective_user
    data = update.callback_query.data

    try:
        parts    = data.split("_")
        feedback = parts[1]
        run_id   = int(parts[2])
        await log_feedback(user.id, run_id, feedback)
        await log_activity(user, f"feedback_{feedback}")
        msg = "Thanks! Glad it was helpful 👍" if feedback == "positive" else "Thanks for the feedback 👎"
        await update.callback_query.answer(msg)
    except Exception as e:
        print(f"[ERROR - feedback_callback] {e}")
        await update.callback_query.answer("Feedback recorded.")


# ─────────────────────────────────────────────────────────────────────────────
# SAVED LOCATIONS
# ─────────────────────────────────────────────────────────────────────────────

async def _show_saved_locations(message, user_id: int):
    """
    Shared logic for showing saved locations list.
    `message` is any object with a reply_text method (update.message or query.message).
    `user_id` is the Telegram user ID.
    """
    saved = await get_saved_locations(user_id)

    if not saved:
        await message.reply_text(
            "📌 You have no saved locations yet.\n\n"
            "After sharing a location, tap *💾 Save Location* to save it.",
            parse_mode="Markdown",
        )
        return

    text    = "📌 *Your Saved Locations*\n\n"
    buttons = []
    for loc in saved:
        star  = "⭐ " if loc["is_default"] else ""
        text += f"{star}*{loc['nickname']}* — {_short_name(loc['area'])}\n"
        buttons.append([
            InlineKeyboardButton(
                f"{star}{loc['nickname']}",
                callback_data=f"manage_loc_{loc['id']}",
            )
        ])

    await message.reply_text(
        text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(buttons)
    )


async def load_location_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user       = update.effective_user
    loc_id     = int(update.callback_query.data.split("_")[-1])
    session_id = str(uuid.uuid4())

    await update.callback_query.answer("📍 Loading your saved location…")
    await upsert_customer(user)

    try:
        async with (await get_pool()).acquire() as conn:
            loc = await conn.fetchrow(
                "SELECT * FROM saved_locations WHERE id = $1 AND user_id = $2",
                loc_id, user.id,
            )
    except Exception as e:
        print(f"[ERROR - load_location_callback] {e}")
        await update.callback_query.message.reply_text("⚠️ Couldn't load that location.")
        return

    if not loc:
        await update.callback_query.message.reply_text("⚠️ Location not found.")
        return

    waiting = await update.callback_query.message.reply_text(_waiting_line())

    try:
        url = await fetch_and_store_weather(
            user.id, loc["lat"], loc["lon"], loc["area"], session_id
        )
        run_info  = await get_latest_run(user.id)
        run_id    = run_info["run_id"] if run_info else None
        condition = await get_weather_code_for_run(run_id) if run_id else None

        await log_activity(user, "load_saved_location", area=loc["area"],
                           lat=loc["lat"], lon=loc["lon"],
                           session_id=session_id, condition=condition)

        await waiting.edit_text(
            f"✅ Got *{loc['nickname']}*", parse_mode="Markdown"
        )

        # Store nickname so choice flow labels the card correctly
        context.user_data["pending_area"]     = loc["area"]
        context.user_data["pending_lat"]      = loc["lat"]
        context.user_data["pending_lon"]      = loc["lon"]
        context.user_data["pending_url"]      = url
        context.user_data["pending_nickname"] = loc["nickname"]

        # Use bot.send_message because this is a callback_query update
        # (no update.message available). Mirrors post_fetch_flow logic exactly.
        successful_runs = await increment_weather_count(user.id)

        if successful_runs % 4 == 0 and successful_runs > 0:
            run_info2 = await get_latest_run(user.id)
            context.user_data["feedback_run_id"]       = run_info2["run_id"] if run_info2 else 0
            context.user_data["feedback_pending_post"] = True  # fires AFTER choice, not instead

        context.user_data["choice_pending"] = True
        await context.bot.send_message(
            chat_id=user.id,
            text="What would you like to see?",
            reply_markup=build_choice_inline(loc["area"], loc.get("nickname")),
        )

    except Exception as e:
        print(f"[ERROR - load_location_callback fetch] {e}")
        await waiting.edit_text("⚠️ Couldn't fetch weather for this location.")


# ─────────────────────────────────────────────────────────────────────────────
# MANAGE SAVED LOCATION
# ─────────────────────────────────────────────────────────────────────────────

async def manage_location_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Fires when user taps a saved location from the manage list.
    Shows options: Load Weather / Rename / Delete / Set as Default.
    callback_data format: manage_loc_{loc_id}
    """
    query  = update.callback_query
    user   = update.effective_user
    loc_id = int(query.data.split("_")[-1])
    await query.answer()

    try:
        async with (await get_pool()).acquire() as conn:
            loc = await conn.fetchrow(
                "SELECT * FROM saved_locations WHERE id = $1 AND user_id = $2",
                loc_id, user.id,
            )
    except Exception as e:
        print(f"[ERROR - manage_location_callback] {e}")
        await query.message.reply_text("⚠️ Couldn't load that location.")
        return

    if not loc:
        await query.message.reply_text("⚠️ Location not found.")
        return

    star = "⭐ " if loc["is_default"] else ""
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🌤️ Load Weather",      callback_data=f"load_location_{loc_id}")],
        [InlineKeyboardButton("✏️ Rename",             callback_data=f"loc_rename_{loc_id}"),
         InlineKeyboardButton("🗑️ Delete",             callback_data=f"loc_delete_{loc_id}")],
        [InlineKeyboardButton("⭐ Set as Default",     callback_data=f"loc_setdefault_{loc_id}")],
    ] if not loc["is_default"] else [
        [InlineKeyboardButton("🌤️ Load Weather",      callback_data=f"load_location_{loc_id}")],
        [InlineKeyboardButton("✏️ Rename",             callback_data=f"loc_rename_{loc_id}"),
         InlineKeyboardButton("🗑️ Delete",             callback_data=f"loc_delete_{loc_id}")],
    ])

    await query.message.reply_text(
        f"📌 *{star}{loc['nickname']}*\n{_short_name(loc['area'])}\n\nWhat would you like to do?",
        parse_mode="Markdown",
        reply_markup=kb,
    )


async def loc_setdefault_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sets a saved location as the default."""
    query  = update.callback_query
    user   = update.effective_user
    loc_id = int(query.data.split("_")[-1])
    await query.answer()

    try:
        async with (await get_pool()).acquire() as conn:
            await conn.execute(
                "UPDATE saved_locations SET is_default = FALSE WHERE user_id = $1", user.id
            )
            await conn.execute(
                "UPDATE saved_locations SET is_default = TRUE  WHERE id = $1 AND user_id = $2",
                loc_id, user.id,
            )
            loc = await conn.fetchrow(
                "SELECT nickname FROM saved_locations WHERE id = $1", loc_id
            )
        nick = loc["nickname"] if loc else "location"
        await query.message.reply_text(
            f"⭐ *{nick}* is now your default location.",
            parse_mode="Markdown",
            reply_markup=build_main_keyboard(),
        )
    except Exception as e:
        print(f"[ERROR - loc_setdefault] {e}")
        await query.message.reply_text("⚠️ Couldn't update default. Try again.")


async def loc_delete_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Deletes a saved location after confirmation."""
    query  = update.callback_query
    user   = update.effective_user
    loc_id = int(query.data.split("_")[-1])
    await query.answer()

    try:
        async with (await get_pool()).acquire() as conn:
            loc = await conn.fetchrow(
                "SELECT nickname FROM saved_locations WHERE id = $1 AND user_id = $2",
                loc_id, user.id,
            )
            if not loc:
                await query.message.reply_text("⚠️ Location not found.")
                return
            await conn.execute(
                "DELETE FROM saved_locations WHERE id = $1 AND user_id = $2",
                loc_id, user.id,
            )
        await query.message.reply_text(
            f"🗑️ *{loc['nickname']}* deleted.",
            parse_mode="Markdown",
            reply_markup=build_main_keyboard(),
        )
    except Exception as e:
        print(f"[ERROR - loc_delete] {e}")
        await query.message.reply_text("⚠️ Couldn't delete. Try again.")


async def loc_rename_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Prompts user to type a new name for the location."""
    query  = update.callback_query
    user   = update.effective_user
    loc_id = int(query.data.split("_")[-1])
    await query.answer()

    try:
        async with (await get_pool()).acquire() as conn:
            loc = await conn.fetchrow(
                "SELECT nickname FROM saved_locations WHERE id = $1 AND user_id = $2",
                loc_id, user.id,
            )
    except Exception as e:
        print(f"[ERROR - loc_rename] {e}")
        await query.message.reply_text("⚠️ Couldn't load location. Try again.")
        return

    if not loc:
        await query.message.reply_text("⚠️ Location not found.")
        return

    context.user_data["awaiting_rename_loc_id"] = loc_id
    await query.message.reply_text(
        f"✏️ Current name: *{loc['nickname']}*\n\nType the new name:",
        parse_mode="Markdown",
    )


# ─────────────────────────────────────────────────────────────────────────────
# SAVE LOCATION CONVERSATION
# ─────────────────────────────────────────────────────────────────────────────

async def save_location_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    area = context.user_data.get("pending_area")

    if not area:
        await update.callback_query.message.reply_text(
            "⚠️ No location to save. Please share your location first."
        )
        return ConversationHandler.END

    await update.callback_query.message.reply_text(
        f"💾 What would you like to call *{area}*?\n\n"
        f"Type a nickname like *Home*, *Office*, or *Nani ka ghar* 😄",
        parse_mode="Markdown",
    )
    return NICKNAME_WAITING


async def receive_nickname(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user     = update.effective_user
    nickname = update.message.text.strip()

    if not nickname or len(nickname) > 30:
        await update.message.reply_text("❌ Please enter a nickname under 30 characters.")
        return NICKNAME_WAITING

    area = context.user_data.get("pending_area")
    lat  = context.user_data.get("pending_lat")
    lon  = context.user_data.get("pending_lon")
    url  = context.user_data.get("pending_url", "")

    if not area or lat is None or lon is None:
        await update.message.reply_text("⚠️ Session expired. Please share your location again.")
        return ConversationHandler.END

    await save_location(user.id, nickname, area, lat, lon, url)
    await log_activity(user, "save_location", area=area, detail=nickname)

    await update.message.reply_text(
        f"✅ Saved as *{nickname}*! Load it anytime from *💾 My Saved Locations*.",
        parse_mode="Markdown",
    )
    return ConversationHandler.END


async def cancel_nickname(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Save cancelled.")
    return ConversationHandler.END


# ─────────────────────────────────────────────────────────────────────────────
# /REMIND — EVENT WEATHER CONVERSATION
# ─────────────────────────────────────────────────────────────────────────────

async def remind_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entry point: /remind — asks for the event name."""
    await update.message.reply_text(
        "📅 What's the event? (e.g. *Wedding*, *Match*, *Trek*)",
        parse_mode="Markdown",
    )
    return EVENT_NAME_WAITING


async def remind_receive_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Receives the event name, then asks for the date."""
    name = update.message.text.strip()
    if not name or len(name) > 50:
        await update.message.reply_text("❌ Keep it under 50 characters please.")
        return EVENT_NAME_WAITING

    context.user_data["remind_event_name"] = name
    await update.message.reply_text(
        f"Got it — *{name}*!\n\nWhat date? (send in format *DD-MM-YYYY*)",
        parse_mode="Markdown",
    )
    return EVENT_DATE_WAITING


async def remind_receive_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Receives the event date, saves the reminder, confirms."""
    user      = update.effective_user
    text      = update.message.text.strip()
    event_name = context.user_data.get("remind_event_name", "Event")

    try:
        event_date = datetime.datetime.strptime(text, "%d-%m-%Y").date()
    except ValueError:
        await update.message.reply_text(
            "❌ Invalid date format. Please send as DD-MM-YYYY (e.g. 25-12-2025)."
        )
        return EVENT_DATE_WAITING

    if event_date <= datetime.date.today():
        await update.message.reply_text(
            "❌ That date is in the past. Please send a future date."
        )
        return EVENT_DATE_WAITING

    # Get the user's area from their most recent run
    area = await get_user_last_area(user.id)

    async with (await get_pool()).acquire() as conn:
        lat_row = await conn.fetchrow(
            "SELECT lat, lon FROM user_activity WHERE user_id = $1 AND lat IS NOT NULL ORDER BY timestamp DESC LIMIT 1",
            user.id,
        )
        lat = lat_row["lat"] if lat_row else None
        lon = lat_row["lon"] if lat_row else None

        await conn.execute(
            """
            INSERT INTO event_reminders (user_id, event_name, event_date, area, lat, lon)
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            user.id, event_name, event_date, area, lat, lon,
        )

    await update.message.reply_text(
        f"✅ Reminder set! I'll send you a weather forecast for *{event_name}* "
        f"on *{event_date.strftime('%d %B %Y')}* the day before.",
        parse_mode="Markdown",
    )
    return ConversationHandler.END


async def cancel_remind(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Reminder cancelled.")
    return ConversationHandler.END


# ─────────────────────────────────────────────────────────────────────────────
# /STATS — personal weather stats command
# ─────────────────────────────────────────────────────────────────────────────

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shows the user their personal SkyUpdate usage and weather statistics."""
    user = update.effective_user
    await log_activity(user, "stats_command")

    async with (await get_pool()).acquire() as conn:
        # Basic usage stats
        user_row = await conn.fetchrow(
            "SELECT weather_checks, successful_runs FROM users WHERE user_id = $1",
            user.id,
        )

        # Rainy days in the user's primary area this month
        today = datetime.date.today()
        rain_count = await conn.fetchval(
            """
            SELECT COUNT(DISTINCT dw.date)
            FROM daily_weather dw
            JOIN scraper_runs sr ON sr.id = dw.run_id
            WHERE sr.user_id = $1
              AND EXTRACT(MONTH FROM dw.date) = $2
              AND EXTRACT(YEAR FROM dw.date) = $3
              AND dw.precipitation_sum > 1
            """,
            user.id, today.month, today.year,
        )

        # Hottest day this month
        hot_row = await conn.fetchrow(
            """
            SELECT dw.date, dw.temperature_2m_max
            FROM daily_weather dw
            JOIN scraper_runs sr ON sr.id = dw.run_id
            WHERE sr.user_id = $1
              AND EXTRACT(MONTH FROM dw.date) = $2
              AND EXTRACT(YEAR FROM dw.date) = $3
              AND dw.temperature_2m_max IS NOT NULL
            ORDER BY dw.temperature_2m_max DESC
            LIMIT 1
            """,
            user.id, today.month, today.year,
        )

        # Saved locations count
        saved_count = await conn.fetchval(
            "SELECT COUNT(*) FROM saved_locations WHERE user_id = $1", user.id
        )


    checks   = user_row["weather_checks"] if user_row else 0
    runs     = user_row["successful_runs"] if user_row else 0
    rains    = rain_count or 0
    hot_temp = f"{round(hot_row['temperature_2m_max'])}°C on {hot_row['date'].strftime('%d %b')}" if hot_row else "N/A"
    saved    = saved_count or 0

    text = (
        f"📊 *Your SkyUpdate Stats*\n\n"
        f"🌦️ Weather checks: {checks}\n"
        f"✅ Successful fetches: {runs}\n"
        f"🌧️ Rainy days this month: {rains}\n"
        f"🌡️ Hottest day this month: {hot_temp}\n"
        f"📌 Saved locations: {saved}"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


# ─────────────────────────────────────────────────────────────────────────────
# /PAUSE — disable morning alerts for 7 days
# ─────────────────────────────────────────────────────────────────────────────

async def pause_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Disables morning alerts for 7 days by setting alerts_enabled = FALSE."""
    user = update.effective_user
    await log_activity(user, "pause_alerts")

    async with (await get_pool()).acquire() as conn:
        await conn.execute(
            "UPDATE users SET alerts_enabled = FALSE WHERE user_id = $1",
            user.id,
        )

    await update.message.reply_text(
        "🔕 Morning alerts paused. I'll stop sending the 7 AM summary.\n\n"
        "To re-enable, send /resume.",
    )



# ─────────────────────────────────────────────────────────────────────────────
# /SETTINGS — hub for saved locations, alert config, appearance
# ─────────────────────────────────────────────────────────────────────────────

async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Central settings hub shown as an inline keyboard menu."""
    user = update.effective_user
    await log_activity(user, "settings_command")

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📌 Manage Saved Locations", callback_data="settings_locations")],
        [InlineKeyboardButton("🎨 Appearance",             callback_data="settings_appearance")],
        [InlineKeyboardButton("⏰ Alert Time",             callback_data="settings_alert_time")],
        [InlineKeyboardButton("🔕 Pause Alerts",           callback_data="settings_pause")],
    ])
    await update.message.reply_text(
        "⚙️ *Settings*\n\nChoose what you want to manage:",
        parse_mode="Markdown",
        reply_markup=keyboard,
    )


async def settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles all settings_ inline buttons."""
    query  = update.callback_query
    action = query.data
    await query.answer()

    if action == "settings_locations":
        # Reuse existing saved locations display
        await _show_saved_locations(query.message, query.from_user.id)

    elif action == "settings_appearance":
        await appearance_inline(query)

    elif action == "settings_alert_time":
        context.user_data["awaiting_alert_time"] = True
        await query.message.reply_text(
            "⏰ What time would you like your morning alert?\n\n"
            "Reply with any time from *6:00* to *11:59* (morning only)\n"
            "Examples: `7:00`, `8:30`, `11:00`",
            parse_mode="Markdown",
        )

    elif action == "settings_pause":
        await query.message.reply_text(
            "🔕 Use /pause to pause alerts for 7 days, or /resume to turn them back on."
        )


# ─────────────────────────────────────────────────────────────────────────────
# /APPEARANCE — let user choose text or visual weather card
# ─────────────────────────────────────────────────────────────────────────────

async def appearance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sends the appearance choice inline keyboard."""
    await _send_appearance_menu(update.message)


async def appearance_inline(query):
    """Sends appearance menu from an inline button context."""
    await _send_appearance_menu(query.message)


async def _send_appearance_menu(message):
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🖼️ Visual card  (image)",  callback_data="appearance_visual"),
            InlineKeyboardButton("📝 Text card  (classic)",  callback_data="appearance_text"),
        ],
    ])
    await message.reply_text(
        "🎨 *Appearance*\n\n"
        "Choose how you want your weather card delivered:\n\n"
        "🖼️ *Visual* — a dark image card with all details\n"
        "📝 *Text* — the classic Markdown text format\n\n"
        "_Your preference is saved and used for every weather check._",
        parse_mode="Markdown",
        reply_markup=keyboard,
    )


async def appearance_choice_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Saves the user's appearance preference to the DB."""
    query = update.callback_query
    await query.answer()
    choice = "visual" if query.data == "appearance_visual" else "text"

    async with (await get_pool()).acquire() as conn:
        await conn.execute(
            "UPDATE users SET output_mode = $1 WHERE user_id = $2",
            choice, query.from_user.id,
        )

    label   = "🖼️ Visual card" if choice == "visual" else "📝 Text card"
    confirm = "Great choice! Your weather will now be delivered as an image." \
              if choice == "visual" else \
              "Got it! Switching back to the classic text format."
    await query.message.reply_text(f"✅ *{label}* selected.\n\n{confirm}", parse_mode="Markdown")

async def resume_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Re-enables morning alerts."""
    user = update.effective_user
    async with (await get_pool()).acquire() as conn:
        await conn.execute(
            "UPDATE users SET alerts_enabled = TRUE WHERE user_id = $1",
            user.id,
        )

    await update.message.reply_text("✅ Morning alerts re-enabled! See you at 7 AM tomorrow.")


# ─────────────────────────────────────────────────────────────────────────────
# COMMAND SHORTCUTS
# ─────────────────────────────────────────────────────────────────────────────

async def insights_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    msg  = update.effective_message
    await log_activity(user, "insights_command")
    await upsert_customer(user)

    area = await get_user_last_area(user.id)
    if not area:
        await msg.reply_text("⚠️ No location found. Please share your location first.")
        return

    try:
        area_label      = f"📍 *{_short_name(area)}*\n\n"
        (visible, hidden), bonus = await asyncio.gather(
            generate_insights_split(user.id, area),
            generate_bonus_insights(user.id, area),
        )
        full_insights   = visible + ("\n\n" + hidden if hidden else "")
        full_text       = area_label + full_insights
        if bonus:
            full_text += "\n\n" + bonus
        await msg.reply_text(
            full_text,
            reply_markup=_build_insights_keyboard(False, area),
        )
    except Exception as e:
        print(f"[ERROR - insights_command] user={user.id} error={e}")
        await msg.reply_text("⚠️ Couldn't load insights right now.")


async def locations_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await _show_saved_locations(update.message, user.id)


async def ask_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/ask <question> — natural language weather query answered by AI."""
    user = update.effective_user
    msg  = update.effective_message
    await log_activity(user, "ask_command")
    await upsert_customer(user)

    question = " ".join(context.args).strip() if context.args else ""
    if not question:
        await msg.reply_text(
            "💬 Just write your question after /ask\n\nExample: /ask will it rain before 6pm?",
            parse_mode="Markdown",
        )
        return

    area = await get_user_last_area(user.id)
    if not area:
        await msg.reply_text("⚠️ No location found. Share your location first.")
        return

    thinking = await msg.reply_text("💭 Thinking...")
    try:
        answer = await generate_ask_response(user.id, area, question)
        await thinking.edit_text(answer)
    except Exception as e:
        print(f"[ERROR - ask_command] user={user.id} error={e}")
        await thinking.edit_text("⚠️ Couldn't answer that right now. Try again shortly.")


async def radar_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/radar — 8-hour rain probability chart."""
    user = update.effective_user
    msg  = update.effective_message
    await log_activity(user, "radar_command")
    await upsert_customer(user)

    area = await get_user_last_area(user.id)
    if not area:
        await msg.reply_text("⚠️ No location found. Share your location first.")
        return

    thinking = await msg.reply_text("📊 Building rain chart...")
    try:
        buf, err = await generate_radar_chart(user.id, area)
        if err:
            await thinking.edit_text(err)
            return
        short_area = area.split(",")[0].strip()
        await thinking.delete()
        await msg.reply_photo(
            photo=buf,
            caption=f"🌧️ *Rain forecast — {short_area}*\nNext 8 hours",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("💡 Full Insights", callback_data="insights"),
                InlineKeyboardButton("📅 Tomorrow",      callback_data="tomorrow"),
            ]]),
        )
    except Exception as e:
        print(f"[ERROR - radar_command] user={user.id} error={e}")
        await thinking.edit_text("⚠️ Couldn't build the chart right now. Try again shortly.")


async def radar_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Inline 🌧️ Rain Chart button."""
    user  = update.effective_user
    query = update.callback_query
    await query.answer("📊 Building rain chart…")
    area = await get_user_last_area(user.id)
    if not area:
        await query.message.reply_text("⚠️ No location found. Share your location first.")
        return
    try:
        buf, err = await generate_radar_chart(user.id, area)
        if err:
            await query.message.reply_text(err)
            return
        short_area = area.split(",")[0].strip()
        await query.message.reply_photo(
            photo=buf,
            caption=f"🌧️ *Rain forecast — {short_area}*\nNext 8 hours",
            parse_mode="Markdown",
        )
    except Exception as e:
        print(f"[ERROR - radar_callback] user={user.id} error={e}")
        await query.message.reply_text("⚠️ Couldn't build the chart right now.")


# ─────────────────────────────────────────────────────────────────────────────
# SCHEDULED JOBS
# ─────────────────────────────────────────────────────────────────────────────

async def send_night_prewarning(context: ContextTypes.DEFAULT_TYPE):
    """
    Runs every evening at 8:30 PM.
    If tomorrow has a tier-1 danger (extreme heat, storm, heavy rain, snow),
    sends a short heads-up message to users with alerts enabled.
    Deduplicates using alerts_sent table with type 'night_prewarning'.
    """
    today    = datetime.date.today()
    tomorrow = today + datetime.timedelta(days=1)
    now      = datetime.datetime.now()

    # Only fire between 8 PM and 9 PM
    if not (20 <= now.hour < 21):
        return

    async with (await get_pool()).acquire() as conn:
        users = await conn.fetch(
            """
            SELECT sl.user_id, sl.area
            FROM saved_locations sl
            JOIN users u ON u.user_id = sl.user_id
            WHERE sl.is_default = TRUE AND u.alerts_enabled = TRUE
              AND sl.user_id NOT IN (
                  SELECT user_id FROM alerts_sent
                  WHERE alert_type = 'night_prewarning'
                    AND sent_at::date = $1
              )
            """,
            today,
        )

    for row in users:
        try:
            async with (await get_pool()).acquire() as conn:
                run_row = await conn.fetchrow(
                    """SELECT id FROM scraper_runs WHERE user_id = $1
                       ORDER BY ran_at DESC LIMIT 1""",
                    row["user_id"],
                )
                if not run_row:
                    continue
                danger_rows = await conn.fetch(
                    """SELECT weather_code_max, temperature_2m_max,
                              precipitation_sum, wind_gusts_10m_max
                       FROM daily_weather
                       WHERE run_id = $1 AND date = $2 LIMIT 1""",
                    run_row["id"], tomorrow,
                )

            if not danger_rows:
                continue

            d = dict(danger_rows[0])
            wmo   = d.get("weather_code_max") or 0
            tmax  = d.get("temperature_2m_max") or 0
            rain  = d.get("precipitation_sum") or 0
            gusts = d.get("wind_gusts_10m_max") or 0

            # Detect tier-1 danger
            danger = None
            if wmo in (95, 96, 99):
                danger = "⛈️ Thunderstorm expected tomorrow"
            elif tmax >= 38:
                danger = f"🔥 Dangerous heat tomorrow ({round(tmax)}°C forecast)"
            elif rain >= 20:
                danger = f"🌧️ Heavy rain tomorrow (~{round(rain)}mm expected)"
            elif gusts >= 65:
                danger = f"🌀 Severe wind gusts tomorrow ({round(gusts)} km/h)"

            if not danger:
                continue

            await context.bot.send_message(
                chat_id=row["user_id"],
                text=f"⚠️ Heads up — {danger}\nCheck your morning alert for details.",
                parse_mode="Markdown",
            )

            async with (await get_pool()).acquire() as log_conn:
                await log_conn.execute(
                    """INSERT INTO alerts_sent (user_id, alert_type, area)
                       VALUES ($1, 'night_prewarning', $2) ON CONFLICT DO NOTHING""",
                    row["user_id"], row["area"],
                )

            print(f"[NIGHT PREWARNING] Sent to user {row['user_id']}")

        except Exception as e:
            print(f"[ERROR - night_prewarning user={row['user_id']}] {e}")


async def send_morning_alerts(context: ContextTypes.DEFAULT_TYPE):
    """
    Runs every minute via run_repeating.
    For each user whose alert_time (stored in users.alert_time) matches the
    current HH:MM, sends the morning alert if not already sent today.
    Defaults to 07:00 if alert_time is NULL.
    Logs to morning_alerts_log to prevent re-sending on bot restart.
    """
    now   = datetime.datetime.now()
    today = datetime.date.today()

    async with (await get_pool()).acquire() as conn:
        users = await conn.fetch(
            """
            SELECT sl.user_id, sl.area, sl.lat, sl.lon,
                   COALESCE(u.alert_time, '07:00'::time) AS alert_time
            FROM saved_locations sl
            JOIN users u ON u.user_id = sl.user_id
            WHERE sl.is_default = TRUE
              AND u.alerts_enabled = TRUE
              AND sl.user_id NOT IN (
                  SELECT user_id FROM morning_alerts_log
                  WHERE alert_date = $1
              )
            """,
            today,
        )

    # Only process users whose alert_time matches the current minute
    due = [
        r for r in users
        if r["alert_time"].hour == now.hour and r["alert_time"].minute == now.minute
    ]
    if not due:
        return

    for row in due:
        try:
            alert_text = await generate_alert_message(row["user_id"], row["area"])
            if not alert_text:
                continue

            # Action buttons — area encoded in callback_data so they work
            # without any context.user_data (morning alert is proactive, not
            # triggered by a button tap, so there's no user_data session)
            area_cb = row["area"].replace("|", "_")[:40]
            alert_keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("💡 Today's Insights",  callback_data=f"choice_insights|{area_cb}|"),
                    InlineKeyboardButton("📅 7-Day",             callback_data="weekly_forecast"),
                ],
                [
                    InlineKeyboardButton("🌤️ Weather Card",     callback_data="show_weather"),
                ],
            ])

            await context.bot.send_message(
                chat_id=row["user_id"],
                text=alert_text,
                reply_markup=alert_keyboard,
            )

            # Log to prevent double-send
            async with (await get_pool()).acquire() as log_conn:
                await log_conn.execute(
                    """
                    INSERT INTO morning_alerts_log (user_id, alert_date, area)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (user_id, alert_date) DO NOTHING
                    """,
                    row["user_id"], today, row["area"],
                )

            print(f"[MORNING ALERT] Sent to user {row['user_id']} ({row['area']})")

        except Exception as e:
            print(f"[ERROR - morning_alert user={row['user_id']}] {e}")


async def send_rain_proximity_alerts(context: ContextTypes.DEFAULT_TYPE):
    """
    Runs every hour.
    Checks if any user's area has rain probability > 80% in the next 2 hours.
    Sends a one-off alert if so, but only if alerts_enabled and not already alerted today.
    Uses alerts_sent table for deduplication.
    """
    now   = datetime.datetime.now()
    today = datetime.date.today()

    async with (await get_pool()).acquire() as conn:
        users = await conn.fetch(
            """
            SELECT sl.user_id, sl.area
            FROM saved_locations sl
            JOIN users u ON u.user_id = sl.user_id
            WHERE sl.is_default = TRUE
              AND u.alerts_enabled = TRUE
            """
        )

    for row in users:
        try:
            async with (await get_pool()).acquire() as conn:
                # Check if rain alert already sent today for this user
                already_sent = await conn.fetchval(
                    """
                    SELECT 1 FROM alerts_sent
                    WHERE user_id = $1
                      AND alert_type = 'rain_proximity'
                      AND sent_at::date = $2
                    """,
                    row["user_id"], today,
                )
                if already_sent:
                    continue

                # Get the latest run
                run_row = await conn.fetchrow(
                    """
                    SELECT id FROM scraper_runs
                    WHERE user_id = $1 AND area = $2
                    ORDER BY ran_at DESC LIMIT 1
                    """,
                    row["user_id"], row["area"],
                )
                if not run_row:
                    continue

                # Look at next 2 hours of rain probability
                rain_rows = await conn.fetch(
                    """
                    SELECT timestamp, precipitation_probability, rain
                    FROM hourly_weather
                    WHERE run_id = $1
                      AND timestamp BETWEEN $2 AND $2 + INTERVAL '2 hours'
                    ORDER BY timestamp ASC
                    """,
                    run_row["id"], now,
                )

            if not rain_rows:
                continue

            max_prob = max(r["precipitation_probability"] or 0 for r in rain_rows)
            if max_prob < 80:
                continue

            # Send alert
            await context.bot.send_message(
                chat_id=row["user_id"],
                text=f"🌧️ Rain alert for {row['area']}: {round(max_prob)}% chance of rain in the next 2 hours. Carry an umbrella!",
            )

            # Log to alerts_sent
            async with (await get_pool()).acquire() as log_conn:
                await log_conn.execute(
                    """
                    INSERT INTO alerts_sent (user_id, alert_type, area)
                    VALUES ($1, 'rain_proximity', $2)
                    ON CONFLICT DO NOTHING
                    """,
                    row["user_id"], row["area"],
                )

            print(f"[RAIN ALERT] Sent to user {row['user_id']} ({row['area']})")

        except Exception as e:
            print(f"[ERROR - rain_proximity user={row['user_id']}] {e}")


async def send_event_reminders(context: ContextTypes.DEFAULT_TYPE):
    """
    Runs every day at 7:00 AM (same job slot as morning alerts but fires after).
    - If event is TOMORROW: sends a day-before weather preview.
    - If event is TODAY: sends a 'good luck today' message with today's forecast.
    Marks sent=TRUE after the on-day message so it doesn't repeat.
    """
    today    = datetime.date.today()
    tomorrow = today + datetime.timedelta(days=1)

    async with (await get_pool()).acquire() as conn:
        # Day-before reminders — event is tomorrow, not yet sent
        day_before_events = await conn.fetch(
            """
            SELECT id, user_id, event_name, area
            FROM event_reminders
            WHERE event_date = $1 AND sent = FALSE
            """,
            tomorrow,
        )

        # On-the-day reminders — event is today, not yet sent
        today_events = await conn.fetch(
            """
            SELECT id, user_id, event_name, area
            FROM event_reminders
            WHERE event_date = $1 AND sent = FALSE
            """,
            today,
        )

    # ── Day-before: send tomorrow's forecast ─────────────────────────────
    for event in day_before_events:
        try:
            forecast = await generate_tomorrow_forecast(event["user_id"])
            location = f" in *{event['area']}*" if event["area"] else ""
            text = (
                f"📅 *Reminder:* *{event['event_name']}* is tomorrow{location}!\n\n"
                f"Here's what the weather looks like:\n\n{forecast}"
            )
            await context.bot.send_message(
                chat_id=event["user_id"], text=text, parse_mode="Markdown"
            )
            print(f"[EVENT DAY-BEFORE] Sent to user {event['user_id']} for {event['event_name']}")
        except Exception as e:
            print(f"[ERROR - event_day_before id={event['id']}] {e}")

    # ── On the day: send today's alert digest as the event morning message ─
    for event in today_events:
        try:
            area       = event["area"] or await get_user_last_area(event["user_id"])
            alert_text = await generate_alert_message(event["user_id"], area) if area else None
            location   = f" in *{event['area']}*" if event["area"] else ""
            header     = f"🎉 *{event['event_name']}* is today{location}! Here's today's weather:\n\n"
            text       = header + (alert_text or "No forecast data available yet.")

            await context.bot.send_message(
                chat_id=event["user_id"], text=text, parse_mode="Markdown"
            )

            # Mark as fully sent now that both messages have gone out
            async with (await get_pool()).acquire() as update_conn:
                await update_conn.execute(
                    "UPDATE event_reminders SET sent = TRUE WHERE id = $1", event["id"]
                )

            print(f"[EVENT ON-DAY] Sent to user {event['user_id']} for {event['event_name']}")

        except Exception as e:
            print(f"[ERROR - event_on_day id={event['id']}] {e}")


async def send_weekly_digest(context: ContextTypes.DEFAULT_TYPE):
    """
    Runs every Sunday at 8:00 AM.
    Sends a 7-day summary to users with a default saved location.
    Skips users who already received a digest this week.
    """
    today      = datetime.date.today()
    week_start = today - datetime.timedelta(days=today.weekday())

    async with (await get_pool()).acquire() as conn:
        users = await conn.fetch(
            """
            SELECT sl.user_id, sl.area
            FROM saved_locations sl
            WHERE sl.is_default = TRUE
              AND sl.user_id NOT IN (
                  SELECT user_id FROM weekly_digest_log WHERE week_start = $1
              )
            """,
            week_start,
        )

        for row in users:
            try:
                daily_rows = await conn.fetch(
                    """
                    SELECT dw.date, dw.temperature_2m_max, dw.temperature_2m_min,
                           dw.weather_code_max, dw.precipitation_sum,
                           dw.uv_index_max, dw.wind_gusts_10m_max
                    FROM daily_weather dw
                    JOIN scraper_runs sr ON sr.id = dw.run_id
                    WHERE sr.user_id = $1 AND dw.date >= $2
                    ORDER BY dw.date ASC
                    LIMIT 7
                    """,
                    row["user_id"], today,
                )

                if not daily_rows:
                    continue

                best_day  = max(daily_rows, key=lambda d: (
                    (d["temperature_2m_max"] or 0) - (d["precipitation_sum"] or 0) * 5
                ))
                worst_day = max(daily_rows, key=lambda d: (
                    (d["precipitation_sum"] or 0) * 3 + (d["wind_gusts_10m_max"] or 0) * 0.5
                ))

                def day_name(d):
                    return d["date"].strftime("%A")

                best_cond  = WMO_CODES.get(best_day["weather_code_max"],  "Pleasant")
                worst_cond = WMO_CODES.get(worst_day["weather_code_max"], "Challenging")

                total_rain_week = sum((d["precipitation_sum"] or 0) for d in daily_rows)
                hottest_day     = max(daily_rows, key=lambda d: d["temperature_2m_max"] or 0)

                text = (
                    f"📅 *Your Week Ahead — {row['area']}*\n\n"
                    f"🌟 *Best day:* {day_name(best_day)} — {best_cond}, "
                    f"high {round(best_day['temperature_2m_max'] or 0)}°C\n"
                    f"🌡️ *Hottest:* {day_name(hottest_day)} — {round(hottest_day['temperature_2m_max'] or 0)}°C\n"
                    f"⚠️ *Watch out:* {day_name(worst_day)} — {worst_cond}, "
                    f"{round(worst_day['precipitation_sum'] or 0)}mm rain expected\n"
                    f"🌧️ *Total rainfall:* {round(total_rain_week, 1)}mm this week\n\n"
                )

                max_uv = max((d["uv_index_max"] or 0) for d in daily_rows)
                if max_uv >= 8:
                    uv_day = max(daily_rows, key=lambda d: d["uv_index_max"] or 0)
                    text += (
                        f"☀️ *UV alert:* Very high UV ({round(max_uv)}) on "
                        f"{day_name(uv_day)} — sunscreen essential.\n\n"
                    )

                text += "_Have a great week! 🌤️_"

                await context.bot.send_message(
                    chat_id=row["user_id"],
                    text=text,
                    parse_mode="Markdown",
                )

                await conn.execute(
                    "INSERT INTO weekly_digest_log (user_id, week_start) VALUES ($1, $2)",
                    row["user_id"], week_start,
                )

                print(f"[DIGEST] Sent to user {row['user_id']}")

            except Exception as e:
                print(f"[ERROR - weekly_digest user={row['user_id']}] {e}")



# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    from telegram.request import HTTPXRequest
    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .request(HTTPXRequest(connect_timeout=30, read_timeout=30, write_timeout=30))
        .build()
    )

    # ── Conversation: save location nickname ──────────────────────────────
    nickname_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(save_location_prompt, pattern="^save_location$")],
        states={
            NICKNAME_WAITING: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_nickname)
            ]
        },
        fallbacks=[CommandHandler("cancel", cancel_nickname)],
        per_message=False,
    )

    # ── Conversation: /remind event weather ───────────────────────────────
    remind_conv = ConversationHandler(
        entry_points=[CommandHandler("remind", remind_start)],
        states={
            EVENT_NAME_WAITING: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, remind_receive_name)
            ],
            EVENT_DATE_WAITING: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, remind_receive_date)
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel_remind)],
    )

    # ── Commands ──────────────────────────────────────────────────────────
    app.add_handler(CommandHandler("start",          start))
    app.add_handler(CommandHandler("sharelocation",  sharelocation_command))
    app.add_handler(CommandHandler("savedlocations", savedlocations_command))
    app.add_handler(CommandHandler("insights",       insights_command))
    app.add_handler(CommandHandler("locations",      locations_command))
    app.add_handler(CommandHandler("stats",          stats_command))
    app.add_handler(CommandHandler("pause",          pause_command))
    app.add_handler(CommandHandler("resume",         resume_command))
    app.add_handler(CommandHandler("settings",       settings_command))
    app.add_handler(CommandHandler("appearance",     appearance_command))
    app.add_handler(CommandHandler("ask",            ask_command))
    app.add_handler(CommandHandler("radar",          radar_command))
    app.add_handler(CallbackQueryHandler(settings_callback,          pattern="^settings_"))
    app.add_handler(CallbackQueryHandler(appearance_choice_callback, pattern="^appearance_"))

    # ── Conversations (must be before generic text handler) ───────────────
    app.add_handler(nickname_conv)
    app.add_handler(remind_conv)

    # ── Inline callbacks ──────────────────────────────────────────────────
    app.add_handler(CallbackQueryHandler(insights_callback,           pattern="^insights$"))
    app.add_handler(CallbackQueryHandler(insights_show_more_callback, pattern="^insights_show_more"))
    app.add_handler(CallbackQueryHandler(tomorrow_callback,           pattern="^tomorrow$"))
    app.add_handler(CallbackQueryHandler(weekly_forecast_callback,    pattern="^weekly_forecast$"))
    app.add_handler(CallbackQueryHandler(weekly_expand_callback,      pattern="^weekly_expand_"))
    app.add_handler(CallbackQueryHandler(main_menu_callback,          pattern="^main_menu$"))
    app.add_handler(CallbackQueryHandler(choice_weather_callback,     pattern="^choice_weather\\|"))
    app.add_handler(CallbackQueryHandler(choice_insights_callback,    pattern="^choice_insights\\|"))
    app.add_handler(CallbackQueryHandler(show_weather_callback,       pattern="^show_weather$"))
    app.add_handler(CallbackQueryHandler(radar_callback,               pattern="^radar$"))
    app.add_handler(CallbackQueryHandler(feedback_callback,           pattern="^feedback_"))
    app.add_handler(CallbackQueryHandler(save_location_prompt,        pattern="^save_location$"))
    app.add_handler(CallbackQueryHandler(manage_location_callback,    pattern="^manage_loc_"))
    app.add_handler(CallbackQueryHandler(loc_setdefault_callback,     pattern="^loc_setdefault_"))
    app.add_handler(CallbackQueryHandler(loc_delete_callback,         pattern="^loc_delete_"))
    app.add_handler(CallbackQueryHandler(loc_rename_callback,         pattern="^loc_rename_"))
    app.add_handler(CallbackQueryHandler(load_location_callback,      pattern="^load_location_"))

    # ── Message handlers ──────────────────────────────────────────────────
    app.add_handler(MessageHandler(filters.LOCATION, location_handler))
    app.add_handler(MessageHandler(
        filters.Regex(r"^-?\d+(\.\d+)?,\s*-?\d+(\.\d+)?$"), manual_latlon
    ))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, log_any_message))

    # ── Scheduled jobs ────────────────────────────────────────────────────
    # Morning alert — runs every 60 seconds, fires per-user based on their alert_time
    app.job_queue.run_repeating(
        send_morning_alerts,
        interval=60,
        first=10,
        name="morning_alerts",
    )

    # Event reminders — every day at 7:00 AM (fires 30s after morning alerts to avoid collision)
    app.job_queue.run_daily(
        send_event_reminders,
        time=datetime.time(7, 0, 30),
        name="event_reminders",
    )

    # Weekly digest — every Sunday at 8:00 AM
    app.job_queue.run_daily(
        send_weekly_digest,
        time=datetime.time(8, 0, 0),
        days=(6,),
        name="weekly_digest",
    )

    # Night pre-warning — every evening at 8:30 PM
    app.job_queue.run_daily(
        send_night_prewarning,
        time=datetime.time(20, 30, 0),
        name="night_prewarning",
    )

    # Rain proximity alert — every hour
    app.job_queue.run_repeating(
        send_rain_proximity_alerts,
        interval=3600,
        first=60,
        name="rain_proximity",
    )

    async def _error_handler(update, context):
        import traceback
        print(f"\n[ERROR] {type(context.error).__name__}: {context.error}")
        traceback.print_exception(type(context.error), context.error, context.error.__traceback__)
    app.add_error_handler(_error_handler)

    print("🤖 SkyUpdate bot is running...")
    app.run_polling(
        drop_pending_updates=True,
        bootstrap_retries=0,   # fail fast — show the real error immediately
        close_loop=False,      # don't close the loop on exit (Windows fix)
    )


if __name__ == "__main__":
    main()