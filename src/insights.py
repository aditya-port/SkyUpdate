"""
insights.py
───────────
DB layer for the SkyUpdate Insights engine.
Fetches data from PostgreSQL for a given user and passes it to the engine
functions in insights_engine.py and alerts_engine.py for calculation.

Public API (called from bot.py):
    await generate_insights(user_id, area)         → full insight string (rest of today)
    await generate_insights_split(user_id, area)   → (visible_text, hidden_text) tuple
    await generate_alert_message(user_id, area)    → 3-bullet morning alert string
    await generate_tomorrow_forecast(user_id)      → tomorrow summary string
    await generate_bonus_insights(user_id, area)   → lifestyle insights string or ""
    await generate_weekly_forecast(user_id)        → (compact_text, days_data)

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
    if len(rows) < 5:   # lowered from 7 — more responsive for newer users
        return None
    vals = [r["temperature_2m_max"] for r in rows]
    return sum(vals) / len(vals)


# ─────────────────────────────────────────────────────────────────────────────
# SHARED HELPER — fetch 7-day daily rows for rain streak detection
# ─────────────────────────────────────────────────────────────────────────────

async def _get_7day_daily_rows(conn, run_id: int) -> list:
    """
    Fetches the last 7 daily_weather rows for this run's area.
    Used by insight_rain_streak() which needs consecutive rainy days.
    """
    today = date.today()
    rows = await conn.fetch(
        """
        SELECT date, precipitation_sum
        FROM daily_weather
        WHERE run_id = $1
          AND date >= $2
        ORDER BY date ASC
        LIMIT 7
        """,
        run_id, today - timedelta(days=6)
    )
    return [dict(r) for r in rows]


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC: generate_insights
# Full insight dump for the rest of today — kept for backward compatibility.
# ─────────────────────────────────────────────────────────────────────────────

async def generate_insights(user_id: int, area: str) -> str:
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
        daily_rows_7 = await _get_7day_daily_rows(conn, run_id)

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
# Returns (visible_text, hidden_text) tuple — main path used by bot.py.
# Fetches 7-day daily rows and passes them to the engine for rain streak.
# ─────────────────────────────────────────────────────────────────────────────

async def generate_insights_split(user_id: int, area: str) -> tuple:
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
        # 7-day daily rows for rain streak detection
        daily_rows_7 = await _get_7day_daily_rows(conn, run_id)

    finally:
        await conn.close()

    if not hourly_rows:
        return "✅ No more forecast data for today. Check back tomorrow morning!", ""

    hours   = [dict(r) for r in hourly_rows]
    aqi_h   = [dict(r) for r in aqi_rows]
    daily   = dict(daily_row) if daily_row else {}
    current = dict(current_row) if current_row else {}

    return _engine_insights_split(hours, aqi_h, daily, current, daily_rows=daily_rows_7)


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC: generate_alert_message
# 3-bullet morning alert — full day's hourly data.
# ─────────────────────────────────────────────────────────────────────────────

async def generate_alert_message(user_id: int, area: str) -> str:
    conn = await asyncpg.connect(DATABASE_URL, ssl="require")
    try:
        run_id = await _get_run_id(conn, user_id, area)
        if not run_id:
            return None

        today = date.today()

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
        hist_avg = await _get_historical_avg(conn, user_id, area)
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
# ─────────────────────────────────────────────────────────────────────────────

async def generate_tomorrow_forecast(user_id: int) -> str:
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
            return "⚠️ No data available. Please share your location first."

        run_id   = row["id"]
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


def _week_tips(row: dict) -> list:
    tips = []
    rain = row.get("precipitation_sum") or 0
    uv   = row.get("uv_index_max") or 0
    gust = row.get("wind_gusts_10m_max") or 0
    tmax = row.get("temperature_2m_max") or 0
    tmin = row.get("temperature_2m_min") or 0
    wmo  = row.get("weather_code_max") or 0

    if wmo in (95, 96, 99):
        tips.append("⛈️ Thunderstorm possible — avoid open areas")
    if tmax >= 38:
        tips.append("🔥 Dangerous heat — avoid going out midday")
    elif tmax >= 33:
        tips.append("🌡️ Hot day — stay hydrated")
    if tmin <= 10:
        tips.append("🧥 Cold morning — layer up")

    if wmo not in (95, 96, 99):
        if rain >= 10:
            tips.append(f"☂️ Carry an umbrella ({round(rain)}mm expected)")
        elif rain >= 3:
            tips.append("🌂 Light rain likely — keep an umbrella handy")

    if wmo in (45, 48):
        tips.append("😷 Foggy — reduced visibility, mask if sensitive")

    if uv >= 8:
        tips.append(f"🧴 Very high UV ({round(uv)}) — sunscreen + hat essential")
    elif uv >= 5:
        tips.append(f"🕶️ Moderate UV ({round(uv)}) — apply sunscreen")

    if gust >= 60:
        tips.append(f"💨 Strong gusts ({round(gust)} km/h) — secure loose items")
    elif gust >= 40:
        tips.append(f"🌬️ Windy ({round(gust)} km/h gusts)")

    return tips


def _build_compact_line(row: dict, day_label: str) -> str:
    tmax = round(row.get("temperature_2m_max") or 0)
    tmin = round(row.get("temperature_2m_min") or 0)
    wmo  = row.get("weather_code_max") or 0
    cond_label, cond_emoji = _WMO_SHORT.get(wmo, ("Mixed", "🌡️"))
    return f"*{day_label}:* {tmin}–{tmax}°C {cond_emoji} {cond_label}"


async def generate_weekly_forecast(user_id: int) -> tuple:
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
            "row":      r,
        })

    compact_lines.append("\n_Tap a day below to see tips 👇_")
    return "\n".join(compact_lines), days_data


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC: generate_bonus_insights
# Lifestyle insights — best run time, laundry, golden hour, exercise air, anomaly.
# Returns "" (empty string) when nothing fires — bot.py checks for empty string,
# not for a filler message. This eliminates the "Nothing to flag" noise.
# ─────────────────────────────────────────────────────────────────────────────

async def generate_bonus_insights(user_id: int, area: str) -> str:
    conn = await asyncpg.connect(DATABASE_URL, ssl="require")
    try:
        run_id = await _get_run_id(conn, user_id, area)
        if not run_id:
            return ""

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

    best_run     = get_best_run_time(hours, aqi_h)
    exercise_air = get_exercise_air_score(hours, aqi_h)
    laundry      = get_laundry_score(hours)
    golden       = get_golden_hour(daily)
    anomaly      = detect_anomaly(hours, hist_avg)

    results = [r for r in [best_run, exercise_air, laundry, golden, anomaly] if r is not None]

    if not results:
        return ""  # Empty string — bot.py checks `if bonus:` to decide whether to append

    header = f"🌿 Lifestyle insights ({len(results)} tip{'s' if len(results) > 1 else ''})\n\n"
    return header + "\n\n".join(results)


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC: generate_ask_response
# Natural language weather query — user types a question, we answer it using
# their actual forecast data as context sent to the Claude API.
# ─────────────────────────────────────────────────────────────────────────────

async def generate_ask_response(user_id: int, area: str, question: str) -> str:
    """
    Fetches today's remaining forecast + AQI, builds a compact context block,
    and calls the Claude API (claude-haiku — fast, cheap) with the user's question.
    Returns a plain-English answer grounded in real pipeline data.
    """
    conn = await asyncpg.connect(DATABASE_URL, ssl="require")
    try:
        run_id = await _get_run_id(conn, user_id, area)
        if not run_id:
            return "⚠️ No weather data found for your location. Share your location first."

        now   = datetime.now()
        today = date.today()

        hourly_rows = await conn.fetch(
            """
            SELECT timestamp, apparent_temperature, precipitation_probability,
                   rain, uv_index, is_day, wind_gusts_10m, cloud_cover,
                   wind_speed_10m, relative_humidity_2m
            FROM hourly_weather
            WHERE run_id = $1 AND timestamp >= $2 AND DATE(timestamp) = $3
            ORDER BY timestamp ASC
            """,
            run_id, now, today
        )
        aqi_rows = await conn.fetch(
            """
            SELECT timestamp, us_aqi, pm2_5, grass_pollen
            FROM hourly_aqi
            WHERE run_id = $1 AND timestamp >= $2 AND DATE(timestamp) = $3
            ORDER BY timestamp ASC
            """,
            run_id, now, today
        )
        daily_row = await conn.fetchrow(
            """
            SELECT temperature_2m_max, temperature_2m_min, sunrise, sunset,
                   precipitation_sum, wind_gusts_10m_max, uv_index_max
            FROM daily_weather WHERE run_id = $1 AND date = $2 LIMIT 1
            """,
            run_id, today
        )
        current_row = await conn.fetchrow(
            """
            SELECT temperature_2m, apparent_temperature, scraped_aqi_value,
                   scraped_aqi_category, us_aqi
            FROM current_weather WHERE run_id = $1 LIMIT 1
            """,
            run_id
        )
    finally:
        await conn.close()

    # Build compact hourly context (max 12 hours to keep prompt small)
    def _fmt(dt):
        if dt is None: return "N/A"
        if isinstance(dt, str):
            try: dt = datetime.fromisoformat(dt)
            except: return dt
        return dt.strftime("%I%p").lstrip("0")

    hours = [dict(r) for r in hourly_rows[:12]]
    aqi_h = [dict(r) for r in aqi_rows[:12]]
    daily = dict(daily_row) if daily_row else {}
    current = dict(current_row) if current_row else {}

    # Build hourly summary lines
    hourly_lines = []
    for h in hours:
        ts = h.get("timestamp")
        rain_p = round(h.get("precipitation_probability") or 0)
        app_t = round(h.get("apparent_temperature") or 0)
        uv = round(h.get("uv_index") or 0, 1)
        gusts = round(h.get("wind_gusts_10m") or 0)
        hourly_lines.append(
            f"  {_fmt(ts)}: feels {app_t}°C, rain {rain_p}%, UV {uv}, gusts {gusts}km/h"
        )

    # AQI line
    aqi_val = current.get("scraped_aqi_value") or current.get("us_aqi")
    aqi_cat = current.get("scraped_aqi_category") or ""
    aqi_line = f"AQI: {aqi_val} ({aqi_cat})" if aqi_val else "AQI: unavailable"

    # Pollen
    pollen_vals = []
    for row in aqi_h:
        if row.get("grass_pollen"):
            pollen_vals.append(row["grass_pollen"])
    pollen_line = f"Grass pollen peak: {round(max(pollen_vals), 1)}" if pollen_vals else ""

    daily_line = ""
    if daily:
        tmax = round(daily.get("temperature_2m_max") or 0)
        tmin = round(daily.get("temperature_2m_min") or 0)
        rain_sum = round(daily.get("precipitation_sum") or 0, 1)
        daily_line = f"Today's range: {tmin}–{tmax}°C, total rain: {rain_sum}mm"

    context_block = f"""Location: {area}
{daily_line}
{aqi_line}
{pollen_line}

Hourly forecast (remaining today):
{chr(10).join(hourly_lines)}
""".strip()

    # Call Groq API (free, no billing required)
    import aiohttp as _aiohttp
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        return "⚠️ /ask needs a GROQ_API_KEY in your .env file. Get one free at console.groq.com"

    body = {
        "model": "llama-3.1-8b-instant",
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are SkyUpdate, a weather assistant. Answer using ONLY the forecast "
                    "data provided. Be direct — give a clear yes/no/time answer when possible, "
                    "then one sentence of context. Under 3 sentences. No markdown, no bullet points."
                )
            },
            {
                "role": "user",
                "content": f"Forecast data:\n{context_block}\n\nQuestion: {question}"
            }
        ],
        "max_tokens": 200,
        "temperature": 0.3,
    }

    try:
        async with _aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.groq.com/openai/v1/chat/completions",
                json=body,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                timeout=_aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 429:
                    return "⚠️ Too many requests — try again in a moment."
                if resp.status != 200:
                    err = await resp.text()
                    print(f"[ASK ERROR] status={resp.status} body={err[:200]}")
                    return "⚠️ Could not get an answer right now. Try again in a moment."
                data = await resp.json()
                answer = data["choices"][0]["message"]["content"].strip()
                return f"💬 {answer}"
    except Exception as e:
        print(f"[ASK ERROR] {e}")
        return "⚠️ Could not reach the answer service. Try again shortly."


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC: generate_radar_chart
# 8-hour rain probability bar chart as a BytesIO PNG.
# Built from existing hourly_weather.precipitation_probability — no new API.
# ─────────────────────────────────────────────────────────────────────────────

async def generate_radar_chart(user_id: int, area: str):
    """
    Returns a BytesIO PNG buffer showing rain probability for the next 8 hours.
    Each bar is colour-coded: green (<30%), yellow (30–59%), orange (60–79%), red (80%+).
    Returns None if no data is available.
    """
    conn = await asyncpg.connect(DATABASE_URL, ssl="require")
    try:
        run_id = await _get_run_id(conn, user_id, area)
        if not run_id:
            return None, "⚠️ No data found. Share your location first."

        now   = datetime.now()
        today = date.today()

        rows = await conn.fetch(
            """
            SELECT timestamp, precipitation_probability, rain
            FROM hourly_weather
            WHERE run_id = $1 AND timestamp >= $2 AND DATE(timestamp) = $3
            ORDER BY timestamp ASC
            LIMIT 8
            """,
            run_id, now, today
        )
    finally:
        await conn.close()

    if not rows:
        return None, "⚠️ No hourly forecast data available right now."

    from PIL import Image, ImageDraw, ImageFont
    from io import BytesIO
    import os as _os

    hours = [dict(r) for r in rows]

    # ── Layout ─────────────────────────────────────────────────────────────
    W, H       = 680, 320
    PAD_L      = 56    # left for y-axis labels
    PAD_R      = 24
    PAD_T      = 48    # top for title
    PAD_B      = 64    # bottom for x labels
    chart_w    = W - PAD_L - PAD_R
    chart_h    = H - PAD_T - PAD_B
    n          = len(hours)
    bar_gap    = 8
    bar_w      = (chart_w - bar_gap * (n + 1)) // n

    BG         = (15, 15, 18)
    GRID       = (40, 40, 52)
    WHITE      = (255, 255, 255)
    MUTED      = (110, 112, 130)
    TITLE_C    = (222, 222, 232)

    def bar_color(prob):
        if prob >= 80: return (220, 60, 60)
        if prob >= 60: return (230, 130, 40)
        if prob >= 30: return (200, 200, 50)
        return (60, 180, 100)

    img  = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)

    # Top accent line
    for i, c in enumerate([(85,150,255),(95,162,255),(108,172,255),(125,182,255)]):
        draw.rectangle([0, i, W, i+1], fill=c)

    # Try loading font — fall back gracefully
    _base = _os.path.dirname(_os.path.abspath(__file__))
    _fonts = _os.path.join(_base, "fonts")
    def _try_font(name, size):
        p = _os.path.join(_fonts, name)
        if _os.path.exists(p):
            return ImageFont.truetype(p, size)
        try:
            return ImageFont.truetype("DejaVuSans.ttf", size)
        except:
            return ImageFont.load_default()

    f_title = _try_font("Poppins-Medium.ttf", 22)
    f_label = _try_font("Poppins-Regular.ttf", 16)
    f_small = _try_font("Poppins-Light.ttf",   13)

    # Title
    title = f"Rain forecast — next {n} hours"
    draw.text((PAD_L, 14), title, font=f_title, fill=TITLE_C)

    # Y-axis grid lines at 0, 25, 50, 75, 100
    for pct in (0, 25, 50, 75, 100):
        y = PAD_T + chart_h - int(chart_h * pct / 100)
        draw.line([(PAD_L, y), (W - PAD_R, y)], fill=GRID, width=1)
        draw.text((PAD_L - 6, y - 8), f"{pct}%", font=f_small, fill=MUTED,
                  anchor="rm" if hasattr(draw, 'textlength') else None)

    # Bars
    for i, h in enumerate(hours):
        prob   = round(h.get("precipitation_probability") or 0)
        rain_mm = round(h.get("rain") or 0, 1)
        x0     = PAD_L + bar_gap + i * (bar_w + bar_gap)
        x1     = x0 + bar_w
        bar_px = int(chart_h * prob / 100)
        y0     = PAD_T + chart_h - bar_px
        y1     = PAD_T + chart_h

        col = bar_color(prob)

        # Bar body
        if bar_px > 0:
            draw.rounded_rectangle([x0, y0, x1, y1], radius=4, fill=col)

        # Prob label on top of bar
        cx = (x0 + x1) // 2
        lbl = f"{prob}%"
        ty = y0 - 20 if bar_px > 24 else PAD_T + chart_h - 24
        draw.text((cx, ty), lbl, font=f_small, fill=col, anchor="mm" if hasattr(draw, 'textlength') else None)

        # Rain mm below label if notable
        if rain_mm >= 0.5:
            draw.text((cx, PAD_T + chart_h + 4), f"~{rain_mm}mm",
                      font=f_small, fill=MUTED, anchor="mt" if hasattr(draw, 'textlength') else None)

        # Time label
        ts = h.get("timestamp")
        if ts:
            def _fmt_h(dt):
                if isinstance(dt, str):
                    try: dt = __import__("datetime").datetime.fromisoformat(dt)
                    except: return dt
                return dt.strftime("%I%p").lstrip("0")
            draw.text((cx, H - PAD_B + 16), _fmt_h(ts),
                      font=f_label, fill=MUTED, anchor="mt" if hasattr(draw, 'textlength') else None)

    # Legend pills
    legend = [("< 30%", (60,180,100)), ("30–59%", (200,200,50)),
              ("60–79%", (230,130,40)), ("80%+", (220,60,60))]
    lx = PAD_L
    for label, col in legend:
        draw.rounded_rectangle([lx, H - 22, lx + 10, H - 12], radius=3, fill=col)
        draw.text((lx + 14, H - 22), label, font=f_small, fill=MUTED)
        lx += int(draw.textlength(label, font=f_small) if hasattr(draw, 'textlength') else 60) + 28

    buf = BytesIO()
    img.save(buf, format="PNG", optimize=True)
    buf.seek(0)
    return buf, None



# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC: generate_temp_chart
# Temperature / feels-like curve for today as a BytesIO PNG line chart.
# Shows apparent_temperature for all hours today from hourly_weather.
# Colour zones: blue <15°C, green 15-28°C, orange 28-38°C, red >38°C
# Marks current time, sunrise, and sunset as vertical lines.
# ─────────────────────────────────────────────────────────────────────────────

async def generate_temp_chart(user_id: int, area: str):
    """
    Returns (BytesIO PNG, None) on success or (None, error_str) on failure.
    Shows feels-like temperature curve for today with colour-coded zones,
    current time marker, and sunrise/sunset markers.
    """
    conn = await asyncpg.connect(DATABASE_URL, ssl="require")
    try:
        run_id = await _get_run_id(conn, user_id, area)
        if not run_id:
            return None, "⚠️ No data found. Share your location first."

        today = date.today()
        now   = datetime.now()

        rows = await conn.fetch(
            """
            SELECT timestamp, apparent_temperature, temperature_2m,
                   uv_index, is_day, precipitation_probability
            FROM hourly_weather
            WHERE run_id = $1 AND DATE(timestamp) = $2
            ORDER BY timestamp ASC
            """,
            run_id, today
        )
        daily = await conn.fetchrow(
            """
            SELECT sunrise, sunset, temperature_2m_max, temperature_2m_min,
                   apparent_temperature_max, apparent_temperature_min
            FROM daily_weather WHERE run_id = $1 AND date = $2 LIMIT 1
            """,
            run_id, today
        )
    finally:
        await conn.close()

    if not rows:
        return None, "⚠️ No hourly data available for today."

    from PIL import Image, ImageDraw, ImageFont
    from io import BytesIO
    import os as _os, math as _math

    hours = [dict(r) for r in rows]
    daily = dict(daily) if daily else {}

    # ── Layout ─────────────────────────────────────────────────────────────
    W, H    = 720, 340
    PAD_L   = 58   # left for y-axis labels
    PAD_R   = 28
    PAD_T   = 52   # top for title
    PAD_B   = 56   # bottom for x labels
    cw      = W - PAD_L - PAD_R
    ch      = H - PAD_T - PAD_B
    n       = len(hours)

    BG      = (15, 15, 18)
    GRID    = (38, 38, 50)
    WHITE   = (255, 255, 255)
    MUTED   = (108, 110, 130)
    TITLE_C = (222, 222, 232)
    NOW_C   = (255, 220, 80)    # current time marker
    SR_C    = (255, 185, 65)    # sunrise
    SS_C    = (100, 140, 255)   # sunset

    def temp_color(t):
        if t is None: return (110, 112, 130)
        if t >= 38:   return (220, 60, 60)
        if t >= 28:   return (230, 130, 40)
        if t >= 15:   return (60, 180, 100)
        return (80, 140, 230)

    img  = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)

    # Top accent
    for i, c in enumerate([(85,150,255),(95,162,255),(108,172,255),(125,182,255)]):
        draw.rectangle([0, i, W, i+1], fill=c)

    _base  = _os.path.dirname(_os.path.abspath(__file__))
    _fonts = _os.path.join(_base, "fonts")
    def _try_font(name, size):
        p = _os.path.join(_fonts, name)
        if _os.path.exists(p):
            return ImageFont.truetype(p, size)
        try: return ImageFont.truetype("DejaVuSans.ttf", size)
        except: return ImageFont.load_default()

    f_title = _try_font("Poppins-Medium.ttf", 20)
    f_label = _try_font("Poppins-Regular.ttf", 14)
    f_small = _try_font("Poppins-Light.ttf",   12)

    # Gather temps
    temps = [h.get("apparent_temperature") for h in hours]
    valid_temps = [t for t in temps if t is not None]
    if not valid_temps:
        return None, "⚠️ No temperature data available."

    t_min = min(valid_temps) - 2
    t_max = max(valid_temps) + 2
    t_range = max(t_max - t_min, 1)

    def x_pos(i):
        return PAD_L + int(i / max(n-1, 1) * cw)

    def y_pos(t):
        if t is None: return PAD_T + ch // 2
        return PAD_T + ch - int((t - t_min) / t_range * ch)

    # Title
    short = area.split(",")[0].strip()
    tmax_val = daily.get("apparent_temperature_max")
    tmin_val = daily.get("apparent_temperature_min")
    range_str = f"  ·  {round(tmin_val)}°–{round(tmax_val)}°C" if tmax_val and tmin_val else ""
    draw.text((PAD_L, 14), f"Temperature today — {short}{range_str}", font=f_title, fill=TITLE_C)

    # Y-axis grid lines
    step = 5
    y_start = int(t_min // step) * step
    for temp_tick in range(y_start, int(t_max) + step, step):
        yy = y_pos(temp_tick)
        if PAD_T <= yy <= PAD_T + ch:
            draw.line([(PAD_L, yy), (W - PAD_R, yy)], fill=GRID, width=1)
            draw.text((PAD_L - 6, yy - 8), f"{temp_tick}°",
                      font=f_small, fill=MUTED, anchor="rm")

    # X-axis labels (every 3 hours)
    for i, h in enumerate(hours):
        ts = h.get("timestamp")
        if ts and (i == 0 or ts.hour % 3 == 0):
            xx = x_pos(i)
            label = ts.strftime("%-I%p").lower() if hasattr(ts, 'strftime') else str(i)
            draw.text((xx, PAD_T + ch + 10), label, font=f_small, fill=MUTED, anchor="mm")

    # Sunrise/sunset vertical lines
    def _parse_iso(v):
        if v is None: return None
        if isinstance(v, str):
            try:
                from datetime import datetime as _dt
                return _dt.fromisoformat(v)
            except: return None
        return v

    sr = _parse_iso(daily.get("sunrise"))
    ss = _parse_iso(daily.get("sunset"))

    for dt_mark, col, label in [(sr, SR_C, "🌅"), (ss, SS_C, "🌇")]:
        if dt_mark and dt_mark.date() == today:
            frac = (dt_mark.hour * 60 + dt_mark.minute) / (24 * 60)
            xm   = PAD_L + int(frac * cw)
            draw.line([(xm, PAD_T), (xm, PAD_T + ch)], fill=col, width=1)
            draw.text((xm, PAD_T - 4), label, font=f_small, fill=col, anchor="mm")

    # Current time vertical line
    now_frac = (now.hour * 60 + now.minute) / (24 * 60)
    now_x    = PAD_L + int(now_frac * cw)
    draw.line([(now_x, PAD_T), (now_x, PAD_T + ch)], fill=NOW_C, width=2)
    draw.text((now_x, PAD_T + ch + 32), "now", font=f_small, fill=NOW_C, anchor="mm")

    # Draw filled area under the line
    poly_pts = [(PAD_L, PAD_T + ch)]
    for i, h in enumerate(hours):
        t = h.get("apparent_temperature")
        poly_pts.append((x_pos(i), y_pos(t)))
    poly_pts.append((x_pos(n-1), PAD_T + ch))

    # Soft fill
    fill_img  = Image.new("RGB", (W, H), BG)
    fill_draw = ImageDraw.Draw(fill_img)
    fill_draw.polygon(poly_pts, fill=(40, 80, 60))
    # Blend fill lightly
    from PIL import Image as _PIL
    img = _PIL.blend(img, fill_img, 0.25)
    draw = ImageDraw.Draw(img)

    # Draw line segments coloured by temperature
    for i in range(n - 1):
        t1 = hours[i].get("apparent_temperature")
        t2 = hours[i+1].get("apparent_temperature")
        if t1 is None or t2 is None:
            continue
        avg_t = (t1 + t2) / 2
        col   = temp_color(avg_t)
        x1, y1 = x_pos(i),   y_pos(t1)
        x2, y2 = x_pos(i+1), y_pos(t2)
        draw.line([(x1, y1), (x2, y2)], fill=col, width=3)

    # Dot at current hour
    now_hour = now.hour
    cur_rows = [h for h in hours if hasattr(h.get("timestamp"), "hour") and h["timestamp"].hour == now_hour]
    if cur_rows:
        ct = cur_rows[0].get("apparent_temperature")
        if ct is not None:
            ci = hours.index(cur_rows[0])
            cx, cy = x_pos(ci), y_pos(ct)
            col = temp_color(ct)
            draw.ellipse([cx-6, cy-6, cx+6, cy+6], fill=col, outline=WHITE, width=2)
            draw.text((cx, cy - 16), f"{round(ct)}°C", font=f_label, fill=col, anchor="mm")

    # Colour legend bottom right
    legend = [("< 15°C", (80,140,230)), ("15–28°C", (60,180,100)),
              ("28–38°C", (230,130,40)), ("> 38°C", (220,60,60))]
    lx = W - PAD_R - 4
    ly = PAD_T + 4
    for lbl, lcol in reversed(legend):
        draw.text((lx, ly), f"■ {lbl}", font=f_small, fill=lcol, anchor="rm")
        ly += 16

    buf = BytesIO()
    img.save(buf, format="PNG", optimize=True)
    buf.seek(0)
    return buf, None


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC: get_streak_history
# Returns recent insight_history rows for streak context calculation.
# Called from bot.py alongside generate_insights_split.
# ─────────────────────────────────────────────────────────────────────────────

async def get_streak_history(user_id: int, area: str) -> list:
    """
    Returns the last 7 insight_history rows for this user+area, sorted DESC.
    Today's row is not included — the caller has today's active tiers fresh.
    Returns [] if no history yet (new user / first week).
    """
    today = date.today()
    try:
        conn = await asyncpg.connect(DATABASE_URL, ssl="require")
        try:
            rows = await conn.fetch(
                """
                SELECT insight_date, tiers_json
                FROM insight_history
                WHERE user_id = $1 AND area = $2
                  AND insight_date < $3
                ORDER BY insight_date DESC
                LIMIT 7
                """,
                user_id, area, today
            )
            return [dict(r) for r in rows]
        finally:
            await conn.close()
    except Exception as e:
        print(f"[DB ERROR - get_streak_history] {e}")
        return []