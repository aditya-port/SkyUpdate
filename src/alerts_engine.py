"""
alerts_engine.py
────────────────
Pure alert scoring and formatting layer for SkyUpdate.
No DB calls. Receives lists of hourly dicts (same shape as insights_engine.py),
scores every possible alert by severity, picks the top 3, and returns a
formatted 3-bullet Telegram message string.

Called from insights.py → generate_alert_message()
which handles all DB fetching and passes clean data here.

Severity scale (higher = more important):
    10  — life-safety (extreme heat, dangerous air, severe storm)
    7   — significant disruption (heavy rain, strong wind, very high UV)
    5   — advisory (moderate rain, elevated AQI, wind chill)
    3   — informational / positive (clear skies, good air)
"""

from datetime import datetime
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# SHARED HELPERS (duplicated intentionally — this file has zero imports from
# insights_engine.py so it stays completely independent and testable alone)
# ─────────────────────────────────────────────────────────────────────────────

def _is_outdoor_hour(ts) -> bool:
    """Returns True if the timestamp falls in the allowed outdoor window.
    Morning: 05:00–09:00  |  Evening: 17:00–19:00
    """
    if ts is None:
        return False
    h = ts.hour if hasattr(ts, "hour") else int(str(ts)[11:13])
    return (5 <= h < 9) or (17 <= h < 19)


def _outdoor_hours(hours: list) -> list:
    """Filter a list of hourly dicts to only the allowed outdoor windows."""
    return [h for h in hours if _is_outdoor_hour(h.get("timestamp"))]


def _fmt(dt) -> str:
    """Format a datetime to 12-hour clock string e.g. '3:00 PM'."""
    if dt is None:
        return "N/A"
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt)
        except Exception:
            return dt
    return dt.strftime("%I:%M %p").lstrip("0")

def _fmt_range(start, end) -> str:
    """Format time range — single-hour groups show 'around X' not 'X–X'."""
    if start == end:
        return f"around {_fmt(start)}"
    return f"{_fmt(start)}–{_fmt(end)}"


def _group(hours: list, check) -> list:
    """
    Returns [(start_dt, end_dt, rows)] for consecutive hours where check(row)
    is True. End time is the start of the hour after the last matching row.
    """
    groups, current = [], []
    for row in hours:
        if check(row):
            current.append(row)
        else:
            if current:
                s = current[0]["timestamp"]
                e = current[-1]["timestamp"]
                e = e.replace(hour=e.hour + 1) if e.hour < 23 else e.replace(hour=23, minute=59)
                groups.append((s, e, current))
                current = []
    if current:
        s = current[0]["timestamp"]
        e = current[-1]["timestamp"]
        e = e.replace(hour=e.hour + 1) if e.hour < 23 else e.replace(hour=23, minute=59)
        groups.append((s, e, current))
    return groups


def _heat_index(temp_c: float, humidity: float) -> float:
    """NWS Heat Index formula. Only meaningful above 27°C."""
    if temp_c < 27:
        return temp_c
    T, H = temp_c, humidity
    return round(
        -8.78469475556 + 1.61139411 * T + 2.3385170 * H
        - 0.14611605 * T * H - 0.01230809 * T ** 2
        - 0.01642482 * H ** 2 + 0.00221173 * T ** 2 * H
        + 0.00072546 * T * H ** 2 - 0.00000358 * T ** 2 * H ** 2,
        1
    )


# ─────────────────────────────────────────────────────────────────────────────
# ALERT SCORERS — each returns (severity: int, message: str) or None
# ─────────────────────────────────────────────────────────────────────────────

def _alert_rain(hours: list):
    valid = [h for h in hours if h.get("precipitation_probability") is not None]
    if not valid:
        return None
    max_prob = max(h["precipitation_probability"] for h in valid)
    if max_prob < 40:
        return None
    groups = _group(valid, lambda h: h.get("precipitation_probability", 0) >= 60)
    if groups:
        start, end, rows = max(groups, key=lambda g: sum(r.get("rain", 0) for r in g[2]))
        total_mm = round(sum(r.get("rain", 0) for r in rows), 1)
        severity = 10 if total_mm >= 15 else 7
        return (severity, f"🌧️ Rain expected {_fmt_range(start, end)} (~{total_mm}mm). Carry an umbrella.")
    peak = max(valid, key=lambda h: h["precipitation_probability"])
    return (5, f"🌦️ Rain possible around {_fmt(peak['timestamp'])} ({round(peak['precipitation_probability'])}% chance). Keep an umbrella handy.")


def _alert_heat(hours: list):
    valid = [h for h in hours if h.get("apparent_temperature") is not None]
    if not valid:
        return None
    peak     = max(valid, key=lambda h: h["apparent_temperature"])
    val      = round(peak["apparent_temperature"], 1)
    peak_t   = _fmt(peak["timestamp"])

    # Find the dangerous window (hours ≥ threshold)
    if val >= 35:
        hot_hours = [h for h in valid if h.get("apparent_temperature", 0) >= 33]
        if len(hot_hours) >= 2:
            window = f"{_fmt(hot_hours[0]['timestamp'])}–{_fmt(hot_hours[-1]['timestamp'])}"
        else:
            window = peak_t
        msg = (
            f"🔥 Dangerous heat — feels like {val}°C (peak around {peak_t})\n"
            f"⛔ Stay indoors {window} — outdoor exposure is dangerous\n"
            f"💧 Drink water every 20 min if you must go out\n"
            f"🧴 SPF 50+ + hat + light loose clothing essential\n"
            f"🌬️ Keep fans/AC running — overnight low may stay above 28°C"
        )
        return (10, msg)

    if val >= 32:
        msg = (
            f"🌡️ Intense heat — feels like up to {val}°C around {peak_t}\n"
            f"💧 Stay hydrated — drink at least 3L water today\n"
            f"🧴 Apply SPF 50+ before going outside\n"
            f"🏠 Limit outdoor time between 11AM–4PM"
        )
        return (7, msg)

    if val >= 29:
        msg = (
            f"☀️ Warm afternoon — feels like {val}°C around {peak_t}\n"
            f"💧 Drink water regularly and apply SPF 30+\n"
            f"👕 Wear light breathable clothing"
        )
        return (5, msg)

    return None


def _alert_heat_stress(hours: list):
    valid = [h for h in hours if h.get("temperature_2m") is not None and h.get("relative_humidity_2m") is not None]
    if not valid:
        return None
    hi_rows = [(h, _heat_index(h["temperature_2m"], h["relative_humidity_2m"])) for h in valid]
    hi_rows = [(h, hi) for h, hi in hi_rows if hi >= 32]
    if not hi_rows:
        return None
    peak_row, peak_hi = max(hi_rows, key=lambda x: x[1])
    if peak_hi >= 45:
        return (10, f"🚨 Dangerous heat stress (heat index {peak_hi}°C) around {_fmt(peak_row['timestamp'])}. Do not go outside.")
    if peak_hi >= 38:
        return (7, f"⚠️ High heat stress (heat index {peak_hi}°C). Rest in shade and drink water every 20 minutes.")
    return (5, f"💧 Mild heat stress around {_fmt(peak_row['timestamp'])} (heat index {peak_hi}°C). Stay hydrated.")


def _alert_uv(hours: list):
    valid = [h for h in hours if h.get("uv_index") is not None and h.get("is_day", 0) == 1]
    if not valid:
        return None
    peak     = max(valid, key=lambda h: h["uv_index"])
    val      = round(peak["uv_index"], 1)
    peak_t   = _fmt(peak["timestamp"])
    # Find UV peak window
    high_uv  = [h for h in valid if h.get("uv_index", 0) >= max(val * 0.8, 6)]
    if len(high_uv) >= 2:
        uv_window = f"{_fmt(high_uv[0]['timestamp'])}–{_fmt(high_uv[-1]['timestamp'])}"
    else:
        uv_window = peak_t

    if val < 5:
        return None
    if val >= 11:
        return (10, (
            f"🚨 Extreme UV index {val} — peak around {peak_t}\n"
            f"⛔ Avoid being outside {uv_window} entirely\n"
            f"🧴 SPF 50+ mandatory — reapply every 2 hours\n"
            f"🕶️ UV-protective sunglasses + full-sleeve clothing"
        ))
    if val >= 8:
        return (7, (
            f"🕶️ Very high UV {val} — peak {uv_window}\n"
            f"🧴 Wear SPF 50+, reapply after sweating\n"
            f"🧢 Hat and seek shade between 10AM–3PM"
        ))
    return (5, (
        f"☀️ High UV {val} around {peak_t}\n"
        f"🧴 Apply SPF 30+ before going out"
    ))


def _alert_wind(hours: list):
    valid = [h for h in hours if h.get("wind_gusts_10m") is not None]
    if not valid:
        return None
    peak = max(valid, key=lambda h: h["wind_gusts_10m"])
    val = round(peak["wind_gusts_10m"], 1)
    if val < 40:
        return None
    if val >= 65:
        return (10, f"🌀 Severe wind gusts ({val} km/h) around {_fmt(peak['timestamp'])}. Avoid outdoor activity.")
    if val >= 50:
        return (7, f"💨 Strong gusts up to {val} km/h around {_fmt(peak['timestamp'])}. Secure loose items outdoors.")
    return (5, f"🌬️ Moderate wind gusts ({val} km/h) expected. Hair and light items may be affected.")


def _alert_aqi(aqi_hours: list, current: dict):
    """
    Uses weather.com scraped AQI (scraped_aqi_value) as the display number.
    Falls back to Open-Meteo us_aqi only if weather.com value is unavailable,
    and labels it with '(OM)' so the user knows the source.
    us_aqi from hourly_aqi is NOT used for the display number — only for
    internal threshold checks when the scraped value is also missing.
    """
    scraped_val = current.get("scraped_aqi_value")
    scraped_cat = current.get("scraped_aqi_category")
    source_suffix = ""

    if scraped_val is not None:
        try:
            val = round(float(scraped_val))
        except (ValueError, TypeError):
            val = None
    else:
        # Fallback: use Open-Meteo current us_aqi
        om = current.get("us_aqi")
        if om is None:
            valid = [h for h in aqi_hours if h.get("us_aqi") is not None]
            om = valid[0]["us_aqi"] if valid else None
        if om is None:
            return None
        val = round(om)
        source_suffix = " (OM)"
        if val <= 50:   scraped_cat = "Good"
        elif val <= 100: scraped_cat = "Moderate"
        elif val <= 150: scraped_cat = "Unhealthy for Sensitive Groups"
        elif val <= 200: scraped_cat = "Unhealthy"
        elif val <= 300: scraped_cat = "Very Unhealthy"
        else:            scraped_cat = "Hazardous"

    if val is None:
        return None

    cat_str = f" — {scraped_cat}" if scraped_cat else ""
    display  = f"{val}{source_suffix}{cat_str}"

    if val <= 50:
        return (3, f"🌿 Clean air today (AQI {display}). Great conditions for outdoor exercise.")
    if val > 300:
        return (10, f"☠️ Hazardous air quality (AQI {display}). Do not go outside.")
    if val > 200:
        return (10, f"🚨 Very poor air quality (AQI {display}). Stay indoors, keep windows closed.")
    if val > 150:
        return (7, f"⚠️ Unhealthy air (AQI {display}). Everyone should reduce outdoor exertion.")
    if val > 100:
        return (5, f"😷 Moderate air quality (AQI {display}). Sensitive groups limit outdoor activity.")
    return None


def _alert_pm25(aqi_hours: list):
    valid = [h for h in aqi_hours if h.get("pm2_5") is not None]
    if not valid:
        return None
    peak = max(valid, key=lambda h: h["pm2_5"])
    val = round(peak["pm2_5"], 1)
    if val < 55:
        return None
    if val >= 150:
        return (10, f"🚨 Dangerous PM2.5 ({val} µg/m³). Stay indoors and keep windows shut.")
    if val >= 55:
        return (7, f"⚠️ High PM2.5 ({val} µg/m³) around {_fmt(peak['timestamp'])}. Avoid prolonged outdoor exposure.")
    return None


def _alert_visibility(hours: list):
    valid = [h for h in hours if h.get("visibility") is not None]
    if not valid:
        return None
    low = min(valid, key=lambda h: h["visibility"])
    km = round(low["visibility"] / 1000, 1)
    if low["visibility"] > 2000:
        return None
    if low["visibility"] <= 500:
        return (10, f"🚨 Very poor visibility ({km} km) around {_fmt(low['timestamp'])}. Avoid driving if possible.")
    return (7, f"🌫️ Reduced visibility ({km} km) around {_fmt(low['timestamp'])}. Drive with caution.")


def _alert_pressure_drop(hours: list):
    valid = [h for h in hours if h.get("pressure_msl") is not None]
    if len(valid) < 3:
        return None
    change = round(valid[-1]["pressure_msl"] - valid[0]["pressure_msl"], 1)
    if change >= -3:
        return None
    if change < -8:
        return (10, f"⚠️ Rapid pressure drop ({abs(change)} hPa). Expect worsening weather — storm possible.")
    return (5, f"📉 Pressure falling ({abs(change)} hPa). Weather may deteriorate later today.")


def _alert_snow(hours: list):
    valid = [h for h in hours if h.get("snowfall") is not None]
    total = sum(h["snowfall"] for h in valid)
    if total <= 0:
        return None
    groups = _group(valid, lambda h: h.get("snowfall", 0) > 0)
    if not groups:
        return None
    start, end, rows = max(groups, key=lambda g: sum(r.get("snowfall", 0) for r in g[2]))
    mm = round(sum(r.get("snowfall", 0) for r in rows), 1)
    if mm >= 10:
        return (10, f"🚨 Heavy snowfall ({mm}mm) between {_fmt_range(start, end)}. Avoid travel.")
    if mm >= 5:
        return (7, f"❄️ Moderate snow expected between {_fmt_range(start, end)} (~{mm}mm). Allow extra travel time.")
    return (5, f"🌨️ Light snowfall possible around {_fmt(start)} (~{mm}mm). Roads may get slippery.")


def _alert_clear_skies(hours: list):
    """Positive alert — fires when the day looks great overall."""
    daytime = [h for h in hours if h.get("is_day", 0) == 1]
    if not daytime:
        return None
    avg_cloud = sum(h.get("cloud_cover", 100) for h in daytime) / len(daytime)
    max_prob = max((h.get("precipitation_probability", 0) for h in daytime), default=0)
    temps = [h.get("apparent_temperature", 0) for h in daytime if h.get("apparent_temperature")]
    avg_temp = sum(temps) / len(temps) if temps else 0
    # Only fire if genuinely pleasant: clear, low rain risk, comfortable temp
    if avg_cloud < 30 and max_prob < 15 and 18 <= avg_temp <= 33:
        return (3, f"🌟 Beautiful day ahead — clear skies, comfortable temperature ({round(avg_temp)}°C). Great time to be outside.")
    return None


def _alert_best_run_time(hours: list, aqi_hours: list):
    """Recommends the single best hour to exercise outside.
    Only considers the safe outdoor windows: 5–9 AM and 5–7 PM.
    """
    if not hours:
        return None
    aqi_by_hour = {}
    for row in aqi_hours:
        ts = row["timestamp"]
        aqi_by_hour[(ts.date(), ts.hour)] = row

    daytime = _outdoor_hours(hours)
    if not daytime:
        return None

    best_score, best_row = -1, None
    for h in daytime:
        uv = h.get("uv_index", 10)
        prob = h.get("precipitation_probability", 100)
        temp = h.get("apparent_temperature", 0)
        ts = h["timestamp"]
        aqi_row = aqi_by_hour.get((ts.date(), ts.hour), {})
        aqi_val = aqi_row.get("us_aqi", 200)

        # Score: lower UV + lower rain + comfortable temp + good AQI = better
        score = 0
        if uv < 4:
            score += 2
        elif uv < 6:
            score += 1
        if prob < 15:
            score += 2
        elif prob < 30:
            score += 1
        if 18 <= temp <= 28:
            score += 2
        elif 15 <= temp <= 33:
            score += 1
        if aqi_val < 50:
            score += 2
        elif aqi_val < 100:
            score += 1

        if score > best_score:
            best_score = score
            best_row = h

    if best_row and best_score >= 5:
        return (3, f"🏃 Best time to exercise outside today: {_fmt(best_row['timestamp'])} (low UV, good air, comfortable temp).")
    return None


def _alert_laundry(hours: list):
    """Checks if today is a good day to dry clothes outdoors."""
    daytime = [h for h in hours if h.get("is_day", 0) == 1]
    if not daytime:
        return None
    max_prob = max((h.get("precipitation_probability", 100) for h in daytime), default=100)
    avg_cloud = sum(h.get("cloud_cover", 100) for h in daytime) / len(daytime)
    avg_humidity = sum(h.get("relative_humidity_2m", 100) for h in daytime) / len(daytime)
    avg_wind = sum(h.get("wind_speed_10m", 0) for h in daytime) / len(daytime)

    if max_prob < 15 and avg_cloud < 40 and avg_humidity < 65 and 5 <= avg_wind <= 25:
        return (3, f"🧺 Great day to dry clothes outside — sunny, low humidity ({round(avg_humidity)}%), light breeze.")
    return None


def _alert_golden_hour(daily: dict):
    """Checks if today has a clear sunset worth mentioning."""
    sunset = daily.get("sunset")
    if not sunset:
        return None
    if isinstance(sunset, str):
        try:
            sunset = datetime.fromisoformat(sunset)
        except Exception:
            return None
    now = datetime.now()
    if sunset <= now:
        return None
    # Only mention if sunset is within the rest of the day and sky looks clear
    # We don't have cloud cover at sunset time, so use daily weather_code as proxy
    wmo = daily.get("weather_code_max", 99)
    if wmo in (0, 1, 2):  # Clear or mainly clear
        return (3, f"🌅 Beautiful sunset likely today at {_fmt(sunset)}. Great time for photography or an evening walk.")
    return None


def _alert_anomaly(hours: list, historical_avg_temp: float):
    """
    Fires if today's average temperature is significantly different
    from the historical average for this month in this area.
    historical_avg_temp is passed in from insights.py (queried from daily_weather).
    """
    if historical_avg_temp is None:
        return None
    temps = [h.get("temperature_2m") for h in hours if h.get("temperature_2m") is not None]
    if not temps:
        return None
    avg_today = sum(temps) / len(temps)
    diff = round(avg_today - historical_avg_temp, 1)
    if abs(diff) < 4:
        return None
    if diff >= 6:
        return (7, f"🌡️ Unusually hot today — about {diff}°C above normal for this time of year.")
    if diff <= -6:
        return (7, f"🥶 Unusually cold today — about {abs(diff)}°C below normal for this time of year.")
    if diff > 0:
        return (3, f"🌡️ Slightly warmer than usual ({diff}°C above average for this month).")
    return (3, f"🧥 Slightly cooler than usual ({abs(diff)}°C below average for this month).")


# ─────────────────────────────────────────────────────────────────────────────
# SCORING ENGINE — picks top 3 by severity
# ─────────────────────────────────────────────────────────────────────────────

def _run_all_scorers(hours: list, aqi_hours: list, daily: dict,
                     current: dict = None, historical_avg_temp: float = None) -> list:
    """
    Runs every alert scorer, collects non-None results,
    and returns them sorted by severity descending.
    `current` is passed to _alert_aqi so it can read the weather.com
    scraped_aqi_value instead of the raw Open-Meteo us_aqi.
    """
    if current is None:
        current = {}
    candidates = [
        _alert_rain(hours),
        _alert_heat(hours),
        _alert_heat_stress(hours),
        _alert_uv(hours),
        _alert_wind(hours),
        _alert_aqi(aqi_hours, current),   # needs current for weather.com AQI
        _alert_pm25(aqi_hours),
        _alert_visibility(hours),
        _alert_pressure_drop(hours),
        _alert_snow(hours),
        _alert_clear_skies(hours),
        _alert_best_run_time(hours, aqi_hours),
        _alert_laundry(hours),
        _alert_golden_hour(daily),
        _alert_anomaly(hours, historical_avg_temp),
    ]
    # Filter None, sort highest severity first
    scored = sorted(
        [c for c in candidates if c is not None],
        key=lambda x: x[0],
        reverse=True
    )
    return scored


def pick_top_3(hours: list, aqi_hours: list, daily: dict,
               current: dict = None, historical_avg_temp: float = None) -> list[str]:
    """
    Public function — returns a list of up to 3 alert message strings,
    ordered by severity. If nothing triggers, returns one friendly fallback.
    `current` carries the weather.com scraped AQI for display.
    """
    scored = _run_all_scorers(hours, aqi_hours, daily, current, historical_avg_temp)
    top = scored[:3]
    if not top:
        return ["✅ All clear today — no significant weather alerts for your area."]
    return [msg for _, msg in top]


def format_alert_message(area: str, bullets: list[str]) -> str:
    """
    Wraps the bullet list into the final Telegram-ready morning alert string.
    No parse_mode — dynamic dashes and symbols break Telegram Markdown.
    """
    header = f"☀️ Good morning! Today's heads-up for {area}:\n\n"
    body = "\n\n".join(bullets)
    return header + body