"""
insights.py
───────────
DB layer for the SkyUpdate Insights engine.
Fetches data from PostgreSQL for a given user and passes it to the engine
functions in insights_engine.py and alerts_engine.py for calculation.

Public API (called from bot.py):
    await generate_insights(user_id, area)         → full insight string (rest of today)
    await generate_alert_message(user_id, area)    → 3-bullet morning alert string
    await generate_tomorrow_forecast(user_id)      → tomorrow summary string

All functions return fully formatted strings ready to send to Telegram.
No formatting logic lives here — that lives in the engine files.
"""

import asyncio
import asyncpg
import os
from datetime import datetime, date, timedelta
from dotenv import load_dotenv

from insights_engine import (
    generate_insights_from_data,
    generate_insights_split as _engine_insights_split,
    get_tomorrow_summary,
    detect_anomaly,
    get_best_run_time,
    get_laundry_score,
    get_golden_hour,
    get_exercise_air_score,
)
from alerts_engine import pick_top_3, format_alert_message

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")


# ─────────────────────────────────────────────────────────────────────────────
# SHARED HELPER — find latest run_id for a user+area
# ─────────────────────────────────────────────────────────────────────────────

async def _get_run_id(conn, user_id: int, area: str):
    """Returns the most recent run_id for this user+area, or None."""
    row = await conn.fetchrow(
        """
        SELECT id FROM scraper_runs
        WHERE user_id = $1 AND area = $2
        ORDER BY ran_at DESC
        LIMIT 1
        """,
        user_id, area
    )
    return row["id"] if row else None


# ─────────────────────────────────────────────────────────────────────────────
# SHARED HELPER — fetch historical average temperature for anomaly detection
# Reads past daily_weather rows for this user's area in the current month
# and returns the mean of temperature_2m_max. Returns None if < 7 rows found
# (not enough history to make a meaningful comparison).
# ─────────────────────────────────────────────────────────────────────────────

async def _get_historical_avg(conn, user_id: int, area: str) -> float | None:
    today = date.today()
    rows = await conn.fetch(
        """
        SELECT dw.temperature_2m_max
        FROM daily_weather dw
        JOIN scraper_runs sr ON sr.id = dw.run_id
        WHERE sr.user_id = $1
          AND sr.area    = $2
          AND EXTRACT(MONTH FROM dw.date) = $3
          AND dw.date < $4
          AND dw.temperature_2m_max IS NOT NULL
        ORDER BY dw.date DESC
        LIMIT 30
        """,
        user_id, area, today.month, today
    )
    if len(rows) < 7:
        return None
    vals = [r["temperature_2m_max"] for r in rows]
    return sum(vals) / len(vals)


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC: generate_insights
# Full insight dump for the rest of today — called when user taps 💡 Insights.
# ─────────────────────────────────────────────────────────────────────────────

async def generate_insights(user_id: int, area: str) -> str:
    """
    Main entry point called from bot.py when the user requests insights.
    Fetches today's remaining hourly data, AQI, daily summary, and current
    conditions, then passes everything to insights_engine and returns the
    fully formatted string.
    """
    conn = await asyncpg.connect(DATABASE_URL, ssl="require")
    try:
        run_id = await _get_run_id(conn, user_id, area)
        if not run_id:
            return "⚠️ No data available yet. Please share your location first."

        now   = datetime.now()
        today = date.today()

        hourly_rows = await conn.fetch(
            """
            SELECT timestamp, temperature_2m, relative_humidity_2m,
                   dew_point_2m, apparent_temperature, precipitation_probability,
                   precipitation, rain, snowfall, cloud_cover,
                   pressure_msl, surface_pressure, wind_speed_10m,
                   wind_direction_10m, wind_gusts_10m, visibility,
                   uv_index, weather_code, sunshine_duration,
                   is_day, freezing_level_height
            FROM hourly_weather
            WHERE run_id = $1 AND timestamp >= $2 AND DATE(timestamp) = $3
            ORDER BY timestamp ASC
            """,
            run_id, now, today
        )
        aqi_rows = await conn.fetch(
            """
            SELECT timestamp, pm10, pm2_5, carbon_monoxide,
                   nitrogen_dioxide, ozone, sulphur_dioxide,
                   us_aqi, aqi_category, dust, uv_index,
                   uv_index_clear_sky, alder_pollen, birch_pollen, grass_pollen
            FROM hourly_aqi
            WHERE run_id = $1 AND timestamp >= $2 AND DATE(timestamp) = $3
            ORDER BY timestamp ASC
            """,
            run_id, now, today
        )
        daily_row = await conn.fetchrow(
            """
            SELECT date, temperature_2m_max, temperature_2m_min,
                   apparent_temperature_max, apparent_temperature_min,
                   sunrise, sunset, precipitation_sum, rain_sum,
                   snowfall_sum, precipitation_hours, daylight_duration,
                   wind_speed_10m_max, wind_gusts_10m_max,
                   wind_direction_10m_dominant, uv_index_max,
                   shortwave_radiation_sum, et0_fao_evapotranspiration,
                   weather_code_max
            FROM daily_weather WHERE run_id = $1 AND date = $2 LIMIT 1
            """,
            run_id, today
        )
        current_row = await conn.fetchrow(
            """
            SELECT timestamp, temperature_2m, relative_humidity_2m,
                   apparent_temperature, precipitation, rain,
                   cloud_cover, pressure_msl, wind_speed_10m,
                   wind_direction_10m, wind_gusts_10m, visibility,
                   uv_index, is_day, weather_code, us_aqi,
                   scraped_aqi_value, scraped_aqi_category
            FROM current_weather WHERE run_id = $1 LIMIT 1
            """,
            run_id
        )

    finally:
        await conn.close()

    if not hourly_rows:
        return "✅ No more forecast data for today. Check back tomorrow morning!"

    hours   = [dict(row) for row in hourly_rows]
    aqi_h   = [dict(row) for row in aqi_rows]
    daily   = dict(daily_row) if daily_row else {}
    current = dict(current_row) if current_row else {}

    return generate_insights_from_data(hours, aqi_h, daily, current)


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC: generate_insights_split
# Same DB fetch as generate_insights but returns (visible, hidden) tuple.
# visible = tier 1+2 (dangerous + rain) always shown immediately.
# hidden  = tier 3+ shown only when user taps "Show N more…" button.
# ─────────────────────────────────────────────────────────────────────────────

async def generate_insights_split(user_id: int, area: str) -> tuple[str, str]:
    """
    Fetches same data as generate_insights, but calls _engine_insights_split
    to return (visible_text, hidden_text) so bot.py can show a collapsed view.
    """
    conn = await asyncpg.connect(DATABASE_URL, ssl="require")
    try:
        run_id = await _get_run_id(conn, user_id, area)
        if not run_id:
            return "⚠️ No data available yet. Please share your location first.", ""

        now   = datetime.now()
        today = date.today()

        hourly_rows = await conn.fetch(
            """
            SELECT timestamp, temperature_2m, relative_humidity_2m,
                   dew_point_2m, apparent_temperature, precipitation_probability,
                   precipitation, rain, snowfall, cloud_cover,
                   pressure_msl, surface_pressure, wind_speed_10m,
                   wind_direction_10m, wind_gusts_10m, visibility,
                   uv_index, weather_code, sunshine_duration,
                   is_day, freezing_level_height
            FROM hourly_weather
            WHERE run_id = $1 AND timestamp >= $2 AND DATE(timestamp) = $3
            ORDER BY timestamp ASC
            """, run_id, now, today
        )
        aqi_rows = await conn.fetch(
            """
            SELECT timestamp, pm10, pm2_5, carbon_monoxide,
                   nitrogen_dioxide, ozone, sulphur_dioxide,
                   us_aqi, aqi_category, dust, uv_index,
                   uv_index_clear_sky, alder_pollen, birch_pollen, grass_pollen
            FROM hourly_aqi
            WHERE run_id = $1 AND timestamp >= $2 AND DATE(timestamp) = $3
            ORDER BY timestamp ASC
            """, run_id, now, today
        )
        daily_row = await conn.fetchrow(
            """
            SELECT date, temperature_2m_max, temperature_2m_min,
                   apparent_temperature_max, apparent_temperature_min,
                   sunrise, sunset, precipitation_sum, rain_sum,
                   snowfall_sum, precipitation_hours, daylight_duration,
                   wind_speed_10m_max, wind_gusts_10m_max,
                   wind_direction_10m_dominant, uv_index_max,
                   shortwave_radiation_sum, et0_fao_evapotranspiration, weather_code_max
            FROM daily_weather WHERE run_id = $1 AND date = $2 LIMIT 1
            """, run_id, today
        )
        current_row = await conn.fetchrow(
            """
            SELECT timestamp, temperature_2m, relative_humidity_2m,
                   apparent_temperature, precipitation, rain, cloud_cover,
                   pressure_msl, wind_speed_10m, wind_direction_10m,
                   wind_gusts_10m, visibility, uv_index, is_day,
                   weather_code, us_aqi, scraped_aqi_value, scraped_aqi_category
            FROM current_weather WHERE run_id = $1 LIMIT 1
            """, run_id
        )
    finally:
        await conn.close()

    if not hourly_rows:
        return "✅ No more forecast data for today. Check back tomorrow morning!", ""

    hours   = [dict(r) for r in hourly_rows]
    aqi_h   = [dict(r) for r in aqi_rows]
    daily   = dict(daily_row) if daily_row else {}
    current = dict(current_row) if current_row else {}

    return _engine_insights_split(hours, aqi_h, daily, current)
# Produces the 3-bullet morning alert string sent at 7 AM by the scheduler.
# Fetches ALL of today's hourly data (not just remaining), so it works
# correctly even when called at 7 AM before much of the day has elapsed.
# ─────────────────────────────────────────────────────────────────────────────

async def generate_alert_message(user_id: int, area: str) -> str:
    """
    Called by the 7 AM JobQueue scheduler in bot.py.
    Fetches the full day's hourly forecast (all hours for today),
    scores them through alerts_engine, picks the top 3 severity bullets,
    and returns the formatted morning alert string.
    """
    conn = await asyncpg.connect(DATABASE_URL, ssl="require")
    try:
        run_id = await _get_run_id(conn, user_id, area)
        if not run_id:
            return None  # No data — skip this user silently

        today = date.today()

        # Full day's hourly weather (not filtered by NOW — full day view for morning alert)
        hourly_rows = await conn.fetch(
            """
            SELECT timestamp, temperature_2m, relative_humidity_2m,
                   apparent_temperature, precipitation_probability,
                   precipitation, rain, snowfall, cloud_cover,
                   pressure_msl, wind_speed_10m, wind_gusts_10m,
                   visibility, uv_index, weather_code,
                   sunshine_duration, is_day, freezing_level_height
            FROM hourly_weather
            WHERE run_id = $1
              AND DATE(timestamp) = $2
            ORDER BY timestamp ASC
            """,
            run_id, today
        )

        # Full day's AQI
        aqi_rows = await conn.fetch(
            """
            SELECT timestamp, pm10, pm2_5, ozone, us_aqi,
                   aqi_category, dust, grass_pollen
            FROM hourly_aqi
            WHERE run_id = $1
              AND DATE(timestamp) = $2
            ORDER BY timestamp ASC
            """,
            run_id, today
        )

        # Daily summary for golden hour and weather_code_max
        daily_row = await conn.fetchrow(
            """
            SELECT date, temperature_2m_max, temperature_2m_min,
                   sunset, sunrise, uv_index_max,
                   precipitation_sum, wind_gusts_10m_max, weather_code_max
            FROM daily_weather
            WHERE run_id = $1 AND date = $2
            LIMIT 1
            """,
            run_id, today
        )

        # Historical average for anomaly detection
        hist_avg = await _get_historical_avg(conn, user_id, area)

        # Current snapshot — carries scraped_aqi_value (weather.com) for
        # _alert_aqi() to use as the display number instead of raw us_aqi.
        current_row = await conn.fetchrow(
            """
            SELECT us_aqi, scraped_aqi_value, scraped_aqi_category
            FROM current_weather
            WHERE run_id = $1
            LIMIT 1
            """,
            run_id,
        )

    finally:
        await conn.close()

    if not hourly_rows:
        return None

    hours   = [dict(row) for row in hourly_rows]
    aqi_h   = [dict(row) for row in aqi_rows]
    daily   = dict(daily_row) if daily_row else {}
    current = dict(current_row) if current_row else {}

    bullets = pick_top_3(hours, aqi_h, daily, current, hist_avg)
    return format_alert_message(area, bullets)


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC: generate_tomorrow_forecast
# Fetches tomorrow's hourly data and returns a compact summary string.
# Called when user taps the 📅 Tomorrow button in bot.py.
# ─────────────────────────────────────────────────────────────────────────────

async def generate_tomorrow_forecast(user_id: int) -> str:
    """
    Fetches tomorrow's hourly weather and AQI rows and returns a compact
    tomorrow-forecast string via insights_engine.get_tomorrow_summary().
    """
    conn = await asyncpg.connect(DATABASE_URL, ssl="require")
    try:
        # Get latest run for this user (any area — tomorrow doesn't need area filter)
        row = await conn.fetchrow(
            """
            SELECT id, area FROM scraper_runs
            WHERE user_id = $1
            ORDER BY ran_at DESC
            LIMIT 1
            """,
            user_id
        )
        if not row:
            return "⚠️ No data available. Please share your location first."

        run_id = row["id"]
        tomorrow = date.today() + timedelta(days=1)

        hourly_rows = await conn.fetch(
            """
            SELECT timestamp, apparent_temperature, precipitation_probability,
                   rain, uv_index, is_day, cloud_cover,
                   wind_gusts_10m, snowfall
            FROM hourly_weather
            WHERE run_id = $1
              AND DATE(timestamp) = $2
            ORDER BY timestamp ASC
            """,
            run_id, tomorrow
        )

        aqi_rows = await conn.fetch(
            """
            SELECT timestamp, us_aqi, pm2_5, ozone
            FROM hourly_aqi
            WHERE run_id = $1
              AND DATE(timestamp) = $2
            ORDER BY timestamp ASC
            """,
            run_id, tomorrow
        )

    finally:
        await conn.close()

    hours = [dict(r) for r in hourly_rows]
    aqi_h = [dict(r) for r in aqi_rows]
    return get_tomorrow_summary(hours, aqi_h)



# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC: generate_weekly_forecast
# 7-day forecast — compact overview by default, expandable tips per day.
# Called from bot.py when user taps 📅 7-Day Forecast button.
# Returns (compact_text, days_data) — bot.py handles the inline expand buttons.
# ─────────────────────────────────────────────────────────────────────────────

# WMO code → short label + emoji
_WMO_SHORT = {
    0: ("Clear", "☀️"),         1: ("Mainly Clear", "🌤️"),
    2: ("Partly Cloudy", "⛅"), 3: ("Overcast", "☁️"),
    45: ("Fog", "🌫️"),           48: ("Fog", "🌫️"),
    51: ("Drizzle", "🌦️"),       53: ("Drizzle", "🌦️"),    55: ("Drizzle", "🌦️"),
    61: ("Rain", "🌧️"),          63: ("Rain", "🌧️"),        65: ("Heavy Rain", "🌧️"),
    71: ("Snow", "❄️"),          73: ("Snow", "❄️"),         75: ("Heavy Snow", "❄️"),
    80: ("Showers", "🌦️"),       81: ("Showers", "🌦️"),    82: ("Heavy Showers", "⛈️"),
    95: ("Thunderstorm", "⛈️"),  96: ("Storm+Hail", "⛈️"),  99: ("Storm+Hail", "⛈️"),
}


def _week_tips(row: dict) -> list[str]:
    """
    Generates rule-based actionable tips for a single daily_weather row.
    Tips are ordered by the same priority ranking as the insights engine:
    Dangerous → Rain → AQI proxy → UV → Wind.
    Returns a list of tip strings (may be empty).
    """
    tips = []
    rain = row.get("precipitation_sum") or 0
    uv   = row.get("uv_index_max") or 0
    gust = row.get("wind_gusts_10m_max") or 0
    tmax = row.get("temperature_2m_max") or 0
    tmin = row.get("temperature_2m_min") or 0
    wmo  = row.get("weather_code_max") or 0

    # Tier 1 — Dangerous
    if wmo in (95, 96, 99):
        tips.append("⛈️ Thunderstorm possible — avoid open areas")
    if tmax >= 38:
        tips.append("🔥 Dangerous heat — avoid going out midday")
    elif tmax >= 33:
        tips.append("🌡️ Hot day — stay hydrated")
    if tmin <= 10:
        tips.append("🧥 Cold morning — layer up")

    # Tier 2 — Rain (skip if storm already added)
    if wmo not in (95, 96, 99):
        if rain >= 10:
            tips.append(f"☂️ Carry an umbrella ({round(rain)}mm expected)")
        elif rain >= 3:
            tips.append("🌂 Light rain likely — keep an umbrella handy")

    # Tier 3 — AQI proxy (fog/haze only — no daily AQI in DB)
    if wmo in (45, 48):
        tips.append("😷 Foggy — reduced visibility, mask if sensitive")

    # Tier 4 — UV
    if uv >= 8:
        tips.append(f"🧴 Very high UV ({round(uv)}) — sunscreen + hat essential")
    elif uv >= 5:
        tips.append(f"🕶️ Moderate UV ({round(uv)}) — apply sunscreen")

    # Tier 5 — Wind
    if gust >= 60:
        tips.append(f"💨 Strong gusts ({round(gust)} km/h) — secure loose items")
    elif gust >= 40:
        tips.append(f"🌬️ Windy ({round(gust)} km/h gusts)")

    return tips


def _build_compact_line(row: dict, day_label: str) -> str:
    """Single compact forecast line: 'Mon: 22–34°C ⛅ Partly Cloudy'"""
    tmax = round(row.get("temperature_2m_max") or 0)
    tmin = round(row.get("temperature_2m_min") or 0)
    wmo  = row.get("weather_code_max") or 0
    cond_label, cond_emoji = _WMO_SHORT.get(wmo, ("Mixed", "🌡️"))
    return f"*{day_label}:* {tmin}–{tmax}°C {cond_emoji} {cond_label}"


async def generate_weekly_forecast(user_id: int) -> tuple[str, list[dict]]:
    """
    Returns (compact_text, days_data) where:
      - compact_text: 7-line overview to send immediately
      - days_data: list of dicts {index, label, date_str, tips} used by
        bot.py to build per-day inline expand buttons and handle
        weekly_expand_N callbacks.

    Compact format per day (one line):
      Mon: 22–34°C ⛅ Partly Cloudy

    Expanded format (sent on demand when user taps a day button):
      📅 Monday (12 Mar) tips:
        • ☂️ Carry an umbrella (14mm expected)
        • 🧴 Very high UV (9) — sunscreen + hat essential
    """
    conn = await asyncpg.connect(DATABASE_URL, ssl="require")
    try:
        row = await conn.fetchrow(
            """
            SELECT id, area FROM scraper_runs
            WHERE user_id = $1
            ORDER BY ran_at DESC
            LIMIT 1
            """,
            user_id
        )
        if not row:
            return "⚠️ No data found. Please share your location first.", []

        run_id = row["id"]
        area   = row["area"]
        today  = date.today()

        daily_rows = await conn.fetch(
            """
            SELECT date,
                   temperature_2m_max, temperature_2m_min,
                   apparent_temperature_max, apparent_temperature_min,
                   weather_code_max,
                   precipitation_sum, rain_sum, snowfall_sum, precipitation_hours,
                   daylight_duration,
                   wind_speed_10m_max, wind_gusts_10m_max, wind_direction_10m_dominant,
                   uv_index_max,
                   sunrise, sunset
            FROM daily_weather
            WHERE run_id = $1
              AND date >= $2
            ORDER BY date ASC
            LIMIT 7
            """,
            run_id, today
        )
    finally:
        await conn.close()

    if not daily_rows:
        return "⚠️ No forecast data available. Please share your location first.", []

    compact_lines = [f"📅 *7-Day Forecast — {area}*\n"]
    days_data = []

    for i, r in enumerate(daily_rows):
        r = dict(r)
        d = r["date"]

        if d == today:
            day_label = "Today"
        elif d == today + timedelta(days=1):
            day_label = "Tomorrow"
        else:
            day_label = d.strftime("%A")

        compact_lines.append(_build_compact_line(r, day_label))
        tips = _week_tips(r)
        days_data.append({
            "index":    i,
            "label":    day_label,
            "date_str": d.strftime("%a %d %b"),
            "tips":     tips,
            "row":      r,   # full daily row — used by expand card in bot.py
        })

    compact_lines.append("\n_Tap a day below to see tips 👇_")
    return "\n".join(compact_lines), days_data

# Returns the bonus lifestyle insights (best run time, laundry, golden hour,
# exercise air score, anomaly) as a single formatted string.
# Called from bot.py when the user has already seen the main insights and
# wants more context. All 5 checks are optional — None results are filtered.
# ─────────────────────────────────────────────────────────────────────────────

async def generate_bonus_insights(user_id: int, area: str) -> str:
    """
    Runs the 5 lifestyle-oriented insight functions and returns a formatted
    string. Designed to be sent as a follow-up to the main insights message.
    """
    conn = await asyncpg.connect(DATABASE_URL, ssl="require")
    try:
        run_id = await _get_run_id(conn, user_id, area)
        if not run_id:
            return "⚠️ No data found. Please share your location first."

        now   = datetime.now()
        today = date.today()

        hourly_rows = await conn.fetch(
            """
            SELECT timestamp, temperature_2m, relative_humidity_2m,
                   apparent_temperature, precipitation_probability,
                   rain, cloud_cover, wind_speed_10m, wind_gusts_10m,
                   uv_index, is_day, sunshine_duration
            FROM hourly_weather
            WHERE run_id = $1
              AND timestamp >= $2
              AND DATE(timestamp) = $3
            ORDER BY timestamp ASC
            """,
            run_id, now, today
        )
        aqi_rows = await conn.fetch(
            """
            SELECT timestamp, us_aqi, pm2_5, ozone
            FROM hourly_aqi
            WHERE run_id = $1
              AND timestamp >= $2
              AND DATE(timestamp) = $3
            ORDER BY timestamp ASC
            """,
            run_id, now, today
        )
        daily_row = await conn.fetchrow(
            """
            SELECT sunset, weather_code_max
            FROM daily_weather
            WHERE run_id = $1 AND date = $2
            LIMIT 1
            """,
            run_id, today
        )

        hist_avg = await _get_historical_avg(conn, user_id, area)

    finally:
        await conn.close()

    hours = [dict(r) for r in hourly_rows]
    aqi_h = [dict(r) for r in aqi_rows]
    daily = dict(daily_row) if daily_row else {}

    # Order: actionable first, informational last.
    # get_exercise_air_score suppresses itself if get_best_run_time would fire,
    # so these two will never produce contradictory messages.
    best_run      = get_best_run_time(hours, aqi_h)
    exercise_air  = get_exercise_air_score(hours, aqi_h)
    laundry       = get_laundry_score(hours)
    golden        = get_golden_hour(daily)
    anomaly       = detect_anomaly(hours, hist_avg)

    results = [r for r in [best_run, exercise_air, laundry, golden, anomaly] if r is not None]

    if not results:
        return "✅ Nothing extra to flag — conditions are well within normal today."

    header = f"🌿 Lifestyle insights for today ({len(results)} tips)\n\n"
    return header + "\n\n".join(results)