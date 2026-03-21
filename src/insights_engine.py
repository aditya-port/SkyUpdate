"""
insights_engine.py
──────────────────
Pure calculation layer for SkyUpdate insights.
Takes lists of hourly dicts (weather + aqi), a daily dict, and a current dict.
Returns a list of insight strings — only the ones that are actually triggered.
No DB calls here — all data is passed in. DB fetching happens in insights.py.

Design rules (enforced throughout):
  - Every insight answers ONE question and produces ONE action or ONE fact.
  - Maximum 1–2 sentences per insight. No padding.
  - No function duplicates what another already said.
  - AQI is resolved ONCE at the top of generate_insights_split and passed down.
  - insight_temperature and insight_wind_chill are retired — insight_clothing
    absorbs both via apparent_temperature.
  - insight_sunshine is retired — insight_cloud_trend replaces it.
  - insight_pressure is retired — misleading and already covered by rain/wind.
  - insight_aqi + insight_mask + insight_respiratory + insight_aqi_trend are
    all retired and replaced by the single composite insight_air().
"""

from datetime import datetime, timedelta
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# OUTDOOR WINDOW FILTER
# Exercise / outdoor activity is only suggested during safe hours:
#   Morning: 05:00–09:00  |  Evening: 17:00–19:00
# ─────────────────────────────────────────────────────────────────────────────

def _is_outdoor_hour(ts) -> bool:
    if ts is None:
        return False
    h = ts.hour if hasattr(ts, "hour") else int(str(ts)[11:13])
    return (5 <= h < 9) or (17 <= h < 19)

def _outdoor_hours(hours: list) -> list:
    return [h for h in hours if _is_outdoor_hour(h.get("timestamp"))]


# ─────────────────────────────────────────────────────────────────────────────
# SHARED HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def fmt_time(dt) -> str:
    if dt is None:
        return "N/A"
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt)
        except Exception:
            return dt
    return dt.strftime("%I:%M %p").lstrip("0")

def fmt_time_range(start, end) -> str:
    if start == end or (hasattr(start, "hour") and hasattr(end, "hour") and start.hour == end.hour):
        return f"around {fmt_time(start)}"
    return f"{fmt_time(start)}–{fmt_time(end)}"

def group_consecutive_hours(hours: list, key_check) -> list:
    groups = []
    current_group = []
    for row in hours:
        if key_check(row):
            current_group.append(row)
        else:
            if current_group:
                groups.append((current_group[0]["timestamp"], current_group[-1]["timestamp"], current_group))
                current_group = []
    if current_group:
        groups.append((current_group[0]["timestamp"], current_group[-1]["timestamp"], current_group))
    return groups

def heat_index(temp_c: float, humidity: float) -> float:
    if temp_c < 27:
        return temp_c
    T, H = temp_c, humidity
    hi = (-8.78469475556
          + 1.61139411 * T + 2.3385170 * H
          - 0.14611605 * T * H - 0.01230809 * T ** 2
          - 0.01642482 * H ** 2 + 0.00221173 * T ** 2 * H
          + 0.00072546 * T * H ** 2 - 0.00000358 * T ** 2 * H ** 2)
    return round(hi, 1)


# ─────────────────────────────────────────────────────────────────────────────
# AQI RESOLUTION — single source of truth, called once per generate pass
# Priority: weather.com scraped value → Open-Meteo current → first hourly value
# Returns (numeric_aqi_or_None, category_str)
# ─────────────────────────────────────────────────────────────────────────────

def resolve_aqi(current: dict, aqi_hours: list) -> tuple:
    """
    Returns (aqi_int, category_str) from the best available source.
    Every insight function that needs an AQI value receives this resolved tuple —
    none of them do their own lookup. Eliminates the three-source contradiction.
    """
    scraped_val = current.get("scraped_aqi_value")
    scraped_cat = current.get("scraped_aqi_category") or ""

    if scraped_val is not None:
        try:
            val = round(float(scraped_val))
            if 0 <= val <= 500:
                return val, scraped_cat
        except (ValueError, TypeError):
            pass

    # Fallback: Open-Meteo current snapshot
    om = current.get("us_aqi")
    if om is not None:
        return round(om), _epa_cat(om)

    # Last resort: first available hourly value
    for h in aqi_hours:
        if h.get("us_aqi") is not None:
            return round(h["us_aqi"]), _epa_cat(h["us_aqi"])

    return None, ""


def _epa_cat(val) -> str:
    if val is None:
        return ""
    if val <= 50:   return "Good"
    if val <= 100:  return "Moderate"
    if val <= 150:  return "Unhealthy for Sensitive Groups"
    if val <= 200:  return "Unhealthy"
    if val <= 300:  return "Very Unhealthy"
    return "Hazardous"


# ─────────────────────────────────────────────────────────────────────────────
# TIER 1 — DANGEROUS CONDITIONS
# ─────────────────────────────────────────────────────────────────────────────

def insight_heat_stroke(hours: list) -> Optional[str]:
    """
    Fires when heat index ≥ 41°C AND UV ≥ 8 simultaneously during daytime.
    Most specific heat danger — takes priority over heat_stress.
    """
    valid = [
        h for h in hours
        if h.get("temperature_2m") is not None
        and h.get("relative_humidity_2m") is not None
        and h.get("uv_index") is not None
        and h.get("is_day", 0) == 1
    ]
    if not valid:
        return None

    risk = [(h, heat_index(h["temperature_2m"], h["relative_humidity_2m"]), h["uv_index"])
            for h in valid]
    risk = [(h, hi, uv) for h, hi, uv in risk if hi >= 41 and uv >= 8]
    if not risk:
        return None

    peak = max(risk, key=lambda x: x[1] + x[2])
    peak_h, peak_hi, peak_uv = peak
    groups = group_consecutive_hours([x[0] for x in risk], lambda _: True)
    if groups:
        start, end, _ = groups[0]
        return (f"🚨 Heat stroke risk {fmt_time_range(start, end)} "
                f"(heat index {peak_hi}°C, UV {round(peak_uv)}) — stay indoors, drink water every 15 min.")
    return (f"🚨 Heat stroke risk around {fmt_time(peak_h['timestamp'])} "
            f"(heat index {peak_hi}°C, UV {round(peak_uv)}) — stay indoors.")


def insight_heat_stress(hours: list) -> Optional[str]:
    """
    Fires on heat index alone (no UV check). Suppressed when heat_stroke fires
    (heat_stroke already covers the worst case and is more specific).
    """
    valid = [h for h in hours
             if h.get("temperature_2m") is not None and h.get("relative_humidity_2m") is not None]
    if not valid:
        return None

    hi_rows = [(h, heat_index(h["temperature_2m"], h["relative_humidity_2m"])) for h in valid]
    hi_rows = [(h, hi) for h, hi in hi_rows if hi >= 32]
    if not hi_rows:
        return None

    peak_row, peak_hi = max(hi_rows, key=lambda x: x[1])

    if peak_hi < 32:
        return None

    if peak_hi < 38:
        groups = group_consecutive_hours(
            valid, lambda h: heat_index(h["temperature_2m"], h.get("relative_humidity_2m", 0)) >= 32
        )
        if groups:
            start, end, _ = groups[0]
            return f"🌡️ Moderate heat stress {fmt_time_range(start, end)} (heat index {peak_hi}°C) — stay hydrated."
        return f"🌡️ Moderate heat stress today (heat index {peak_hi}°C) — stay hydrated."

    if peak_hi < 45:
        groups = group_consecutive_hours(
            valid, lambda h: heat_index(h["temperature_2m"], h.get("relative_humidity_2m", 0)) >= 38
        )
        if groups:
            start, end, _ = groups[0]
            return f"⚠️ High heat stress {fmt_time_range(start, end)} (heat index {peak_hi}°C) — avoid outdoor exertion."
        return f"⚠️ High heat stress today (heat index {peak_hi}°C) — avoid outdoor exertion."

    groups = group_consecutive_hours(
        valid, lambda h: heat_index(h["temperature_2m"], h.get("relative_humidity_2m", 0)) >= 45
    )
    if groups:
        start, end, _ = groups[0]
        return f"🚨 Dangerous heat stress {fmt_time_range(start, end)} (heat index {peak_hi}°C) — do not go outside."
    return f"🚨 Dangerous heat stress (heat index {peak_hi}°C) — stay indoors."


def insight_frost(hours: list) -> Optional[str]:
    valid = [h for h in hours if h.get("freezing_level_height") is not None]
    if not valid:
        return None
    min_row = min(valid, key=lambda h: h["freezing_level_height"])
    h = round(min_row["freezing_level_height"])
    if h > 3000:
        return None
    if h > 2000:
        return f"❄️ Freezing level dropping to {h}m — cold night ahead, protect plants and pipes."
    if h > 1000:
        return f"🥶 Frost risk tonight (freezing level {h}m) — cover plants, check outdoor pipes."
    if h > 500:
        groups = group_consecutive_hours(valid, lambda r: r.get("freezing_level_height", 9999) <= 1000)
        if groups:
            start, end, _ = groups[0]
            return f"⚠️ Significant frost {fmt_time_range(start, end)} (freezing level {h}m) — secure vehicles and water lines."
        return f"⚠️ Significant frost risk (freezing level {h}m) — secure vehicles and water lines."
    return f"🚨 Severe frost conditions (freezing level {h}m) — pipes and plants at serious risk."


def insight_snow(hours: list) -> Optional[str]:
    valid = [h for h in hours if h.get("snowfall") is not None]
    total = sum(h["snowfall"] for h in valid)
    if total <= 0:
        return None
    groups = group_consecutive_hours(valid, lambda h: h.get("snowfall", 0) > 0)
    if not groups:
        return None
    biggest = max(groups, key=lambda g: sum(r.get("snowfall", 0) for r in g[2]))
    start, end, rows = biggest
    mm = round(sum(r.get("snowfall", 0) for r in rows), 1)
    if mm < 2:
        return f"🌨️ Light snowfall possible around {fmt_time(start)} (~{mm}mm) — roads may get slippery."
    if mm < 5:
        return f"❄️ Moderate snow {fmt_time_range(start, end)} (~{mm}mm) — allow extra travel time."
    if mm < 10:
        return f"⚠️ Heavy snow {fmt_time_range(start, end)} (~{mm}mm) — avoid travel if possible."
    return f"🚨 Severe snowfall {fmt_time_range(start, end)} (~{mm}mm total) — stay indoors."


# ─────────────────────────────────────────────────────────────────────────────
# TIER 2 — RAIN
# ─────────────────────────────────────────────────────────────────────────────

def insight_rain(hours: list) -> Optional[str]:
    valid = [h for h in hours if h.get("precipitation_probability") is not None]
    if not valid:
        return None

    max_prob = max(h["precipitation_probability"] for h in valid)

    # Tier 1: truly negligible
    if max_prob < 10:
        high_count = sum(1 for h in valid if h.get("precipitation_probability", 0) >= 6)
        if high_count >= 4:
            return f"🌤️ Rain unlikely but possible — {high_count} hours with ~{round(max_prob)}% chance."
        return None  # Suppressed — no rain insight when probability is trivial

    # Tier 2: low (10–29%)
    if max_prob < 30:
        low_groups = group_consecutive_hours(
            valid, lambda h: h.get("precipitation_probability", 0) >= 15
        )
        if low_groups:
            start, end, _ = low_groups[0]
            return f"🌤️ Low rain chance ({round(max_prob)}% peak) around {fmt_time_range(start, end)} — probably fine without an umbrella."
        return f"🌤️ Very low rain chance (max {round(max_prob)}%) — no action needed."

    # Tier 3: moderate (30–59%)
    if max_prob < 60:
        rain_groups = group_consecutive_hours(
            valid, lambda h: h.get("precipitation_probability", 0) >= 30
        )
        if not rain_groups:
            peak_row = max(valid, key=lambda h: h["precipitation_probability"])
            return f"🌦️ Some rain possible around {fmt_time(peak_row['timestamp'])} ({round(peak_row['precipitation_probability'])}%) — keep an umbrella handy."
        rain_groups.sort(key=lambda g: g[0])
        total_mm = round(sum(r.get("rain", 0) for g in rain_groups for r in g[2]), 1)
        if len(rain_groups) == 1:
            start, end, rows = rain_groups[0]
            peak = round(max(r.get("precipitation_probability", 0) for r in rows))
            mm_str = f" ~{total_mm}mm" if total_mm >= 1 else ""
            return f"🌦️ Rain possible {fmt_time_range(start, end)} ({peak}%{mm_str}) — take an umbrella if going out then."
        windows = ", ".join(f"{fmt_time(g[0])}–{fmt_time(g[1])}" for g in rain_groups)
        return f"🌦️ Scattered rain in {len(rain_groups)} windows: {windows} — keep an umbrella handy."

    # Tier 4: likely (≥60%) — intensity + timing
    rain_groups = group_consecutive_hours(
        valid, lambda h: h.get("precipitation_probability", 0) >= 60
    )
    if not rain_groups:
        peak_row = max(valid, key=lambda h: h["precipitation_probability"])
        return (f"🌧️ Rain likely around {fmt_time(peak_row['timestamp'])} "
                f"({round(peak_row['precipitation_probability'])}%) — carry an umbrella.")

    rain_groups.sort(key=lambda g: g[0])
    now = datetime.now()
    first_start = rain_groups[0][0]
    mins_until = int((first_start - now).total_seconds() / 60) if first_start > now else 0

    if mins_until > 90:
        timing = f" Leave before {fmt_time(first_start)}."
    elif mins_until > 30:
        timing = f" Leave soon — starts {fmt_time(first_start)}."
    elif mins_until > 0:
        timing = f" Rain in {mins_until} min — take an umbrella now."
    else:
        timing = ""

    if len(rain_groups) == 1:
        start, end, rows = rain_groups[0]
        total_mm = round(sum(r.get("rain", 0) for r in rows), 1)
        max_prob_w = round(max(r.get("precipitation_probability", 0) for r in rows))
        duration = len(rows)
        mm_per_hr = total_mm / duration if duration > 0 else 0
        if mm_per_hr >= 7.5 or max_prob_w >= 85:
            intensity, emoji = "heavy", "⛈️"
        elif mm_per_hr >= 2.5:
            intensity, emoji = "moderate", "🌧️"
        else:
            intensity, emoji = "light", "🌦️"
        mm_str = f" ~{total_mm}mm" if total_mm >= 0.5 else ""
        return f"{emoji} {intensity.capitalize()} rain {fmt_time_range(start, end)} ({max_prob_w}%{mm_str}).{timing}"

    # Multiple windows
    parts = []
    total_all = 0
    for g_start, g_end, g_rows in rain_groups:
        g_mm = round(sum(r.get("rain", 0) for r in g_rows), 1)
        g_prob = round(max(r.get("precipitation_probability", 0) for r in g_rows))
        total_all += g_mm
        mm_part = f" ~{g_mm}mm" if g_mm >= 0.5 else ""
        parts.append(f"{fmt_time_range(g_start, g_end)} ({g_prob}%{mm_part})")
    windows_str = " and ".join(parts) if len(parts) == 2 else ", ".join(parts[:-1]) + f" and {parts[-1]}"
    total_str = f" (~{round(total_all, 1)}mm total)" if total_all >= 0.5 else ""
    return f"🌧️ Rain in {len(rain_groups)} windows: {windows_str}{total_str}.{timing}"


# ─────────────────────────────────────────────────────────────────────────────
# TIER 3 — AIR QUALITY (composite: replaces insight_aqi + insight_mask +
#           insight_respiratory + insight_aqi_trend)
# ─────────────────────────────────────────────────────────────────────────────

def insight_air(aqi_hours: list, resolved_aqi: int, aqi_cat: str) -> Optional[str]:
    """
    Single composite air quality insight.
    Uses the pre-resolved AQI (from resolve_aqi()) for both display and thresholds.
    Internally checks PM2.5, ozone, and AQI trend to produce one complete line.
    Replaces: insight_aqi, insight_mask, insight_respiratory, insight_aqi_trend.
    """
    if resolved_aqi is None:
        return None

    # AQI trend — check direction (used to append a clause, not a separate bullet)
    trend_suffix = ""
    valid_trend = [h for h in aqi_hours if h.get("us_aqi") is not None]
    if len(valid_trend) >= 6:
        first_mean = sum(h["us_aqi"] for h in valid_trend[:3]) / 3
        last_mean  = sum(h["us_aqi"] for h in valid_trend[-3:]) / 3
        change = last_mean - first_mean
        if change >= 25:
            trend_suffix = " Worsening through the day."
        elif change <= -25:
            trend_suffix = " Improving through the day."

    # PM2.5 peak — used to determine mask recommendation
    pm25_vals = [h.get("pm2_5") for h in aqi_hours if h.get("pm2_5") is not None]
    peak_pm25 = max(pm25_vals) if pm25_vals else 0

    cat_str = f" — {aqi_cat}" if aqi_cat else ""

    # Hazardous / Very Unhealthy
    if resolved_aqi > 200:
        return f"☠️ Hazardous air (AQI {resolved_aqi}{cat_str}) — stay indoors, N99 mask if you must go out.{trend_suffix}"

    if resolved_aqi > 150:
        mask = "N95 mask outdoors" if peak_pm25 >= 55 else "mask recommended outdoors"
        return f"😷 Unhealthy air (AQI {resolved_aqi}{cat_str}) — {mask}.{trend_suffix}"

    if resolved_aqi > 100:
        if peak_pm25 >= 35:
            return f"😷 Air quality poor for sensitive groups (AQI {resolved_aqi}) — N95 if you have respiratory conditions.{trend_suffix}"
        return f"⚠️ Moderate air quality (AQI {resolved_aqi}{cat_str}) — sensitive groups should limit outdoor time.{trend_suffix}"

    # Good / acceptable — only fire if something useful to add (trend or clean air note)
    if resolved_aqi <= 50:
        if trend_suffix:
            return f"🌿 Clean air right now (AQI {resolved_aqi}).{trend_suffix}"
        return f"🌿 Clean air today (AQI {resolved_aqi}) — good time to be outside."

    # AQI 51–100: acceptable — only mention if worsening
    if trend_suffix:
        return f"😐 Acceptable air (AQI {resolved_aqi}).{trend_suffix}"

    return None  # Suppress "Acceptable" with no trend — no action needed


def insight_pollen_combined(aqi_hours: list) -> Optional[str]:
    """
    Combined pollen insight. Kept separate from air quality — it's different information
    (allergens, not pollution). One tight line with the active types and action.
    """
    keys = [("grass_pollen", "Grass"), ("alder_pollen", "Alder"), ("birch_pollen", "Birch")]
    peaks = {}
    for key, name in keys:
        vals = [h.get(key) for h in aqi_hours if h.get(key) is not None]
        if vals:
            peaks[name] = round(max(vals), 1)

    if not peaks:
        return None

    dominant_val = max(peaks.values())
    if dominant_val < 10:
        return None

    active = [f"{n} {v}" for n, v in peaks.items() if v >= 10]
    active_str = ", ".join(active)

    if dominant_val >= 120:
        return f"🤧 Very high pollen — {active_str}. Stay indoors and take antihistamines."
    if dominant_val >= 60:
        return f"🌸 High pollen — {active_str}. Take antihistamines before going out."
    if dominant_val >= 30:
        return f"🌸 Moderate pollen — {active_str}. Antihistamines recommended if sensitive."
    return None  # Low pollen — suppress, not actionable


# ─────────────────────────────────────────────────────────────────────────────
# TIER 4 — UV, CLOTHING, COMMUTE
# ─────────────────────────────────────────────────────────────────────────────

def insight_uv(hours: list) -> Optional[str]:
    """
    UV index insight — daytime only. Suppressed below index 5 (low UV is not actionable).
    """
    valid = [h for h in hours if h.get("uv_index") is not None and h.get("is_day", 0) == 1]
    if not valid:
        return None

    peak_row = max(valid, key=lambda h: h["uv_index"])
    peak_val = round(peak_row["uv_index"], 1)

    if peak_val < 5:
        return None  # Low / moderate UV — not worth a dedicated insight

    if peak_val < 8:
        groups = group_consecutive_hours(valid, lambda h: h.get("uv_index", 0) >= 5)
        if groups:
            start, end, _ = groups[0]
            return f"☀️ High UV ({peak_val}) {fmt_time_range(start, end)} — apply SPF 30+ before going out."
        return f"☀️ High UV ({peak_val}) around {fmt_time(peak_row['timestamp'])} — apply SPF 30+."

    if peak_val < 11:
        groups = group_consecutive_hours(valid, lambda h: h.get("uv_index", 0) >= 8)
        if groups:
            start, end, _ = groups[0]
            return f"🕶️ Very high UV ({peak_val}) {fmt_time_range(start, end)} — sunscreen + hat essential, limit sun exposure."
        return f"🕶️ Very high UV ({peak_val}) — sunscreen and hat essential."

    groups = group_consecutive_hours(valid, lambda h: h.get("uv_index", 0) >= 11)
    if groups:
        start, end, _ = groups[0]
        return f"🚨 Extreme UV ({peak_val}) {fmt_time_range(start, end)} — avoid being outside entirely."
    return f"🚨 Extreme UV ({peak_val}) — stay indoors during peak hours."


def insight_clothing(hours: list) -> Optional[str]:
    """
    Single 'what to wear' insight. Reads apparent_temperature (which already
    includes wind chill effect) and rain probability. Replaces insight_temperature
    and insight_wind_chill — both of those are absorbed here via apparent_temp.
    Priority: extreme danger > rain gear > cold > hot > comfortable.
    """
    valid = [h for h in hours if h.get("apparent_temperature") is not None]
    if not valid:
        return None

    peak_feels = max(h["apparent_temperature"] for h in valid)
    min_feels  = min(h["apparent_temperature"] for h in valid)
    max_rain   = max((h.get("precipitation_probability", 0) or 0 for h in valid), default=0)
    max_gusts  = max((h.get("wind_gusts_10m", 0) or 0 for h in valid), default=0)

    # Extreme heat — clothing advice overlaps with heat_stress but is more specific action
    if peak_feels >= 42:
        return f"👕 Extreme heat (feels like {round(peak_feels)}°C) — light loose clothing, cover skin, avoid peak sun."

    # Rain + cold combo
    if max_rain >= 60 and min_feels < 18:
        return f"🌧️🧥 Wet and cool (feels like {round(min_feels)}°C) — waterproof jacket, layer underneath."

    # Rain gear
    if max_rain >= 60:
        return f"☂️ Rain likely — umbrella or rain jacket essential."

    # Cold
    if min_feels < 10:
        return f"🧥 Cold today (feels like {round(min_feels)}°C at coldest) — heavy jacket needed."
    if min_feels < 18:
        if max_gusts >= 40:
            return f"🧥 Cool and gusty (feels like {round(min_feels)}°C, gusts {round(max_gusts)} km/h) — windproof jacket."
        return f"🧥 Cool day (feels like {round(min_feels)}°C) — light jacket recommended."

    # Warm but not dangerous
    if peak_feels >= 35:
        return f"👕 Warm day (feels like {round(peak_feels)}°C peak) — light breathable clothing."

    # Comfortable with light rain chance
    if max_rain >= 30:
        return f"👔 Comfortable ({round(min_feels)}–{round(peak_feels)}°C) but keep a light rain jacket handy."

    # Fully comfortable — suppress, weather card already shows this
    return None


def insight_commute(hours: list, aqi_hours: list, resolved_aqi: int) -> Optional[str]:
    """
    Checks if heavy rain, dangerous heat, poor AQI or severe wind falls during
    commute windows (7–9 AM or 5–7 PM). Uses resolved AQI, not raw us_aqi.
    """
    def _commute_hour(ts) -> bool:
        if ts is None:
            return False
        h = ts.hour if hasattr(ts, "hour") else int(str(ts)[11:13])
        return (7 <= h < 9) or (17 <= h < 19)

    commute_h = [h for h in hours if _commute_hour(h.get("timestamp"))]
    if not commute_h:
        return None

    issues = []

    max_rain = max((h.get("precipitation_probability", 0) or 0 for h in commute_h), default=0)
    if max_rain >= 70:
        peak = max(commute_h, key=lambda h: h.get("precipitation_probability", 0))
        slot = "morning commute" if peak["timestamp"].hour < 12 else "evening commute"
        issues.append(f"heavy rain during {slot} ({round(max_rain)}%)")

    max_feels = max((h.get("apparent_temperature", 0) or 0 for h in commute_h), default=0)
    if max_feels >= 40:
        issues.append(f"dangerous heat (feels like {round(max_feels)}°C)")

    # Use resolved_aqi for the threshold — consistent with what insight_air shows
    if resolved_aqi is not None and resolved_aqi >= 150:
        issues.append(f"unhealthy air (AQI {resolved_aqi})")

    max_gusts = max((h.get("wind_gusts_10m", 0) or 0 for h in commute_h), default=0)
    if max_gusts >= 55:
        issues.append(f"strong gusts ({round(max_gusts)} km/h)")

    if not issues:
        return None

    if len(issues) == 1:
        return f"🚗 Commute alert — {issues[0].capitalize()}. Plan accordingly."
    return f"🚗 Commute alert — {' and '.join(issues).capitalize()}. Adjust travel time."


# ─────────────────────────────────────────────────────────────────────────────
# TIER 5 — WIND, HYDRATION
# ─────────────────────────────────────────────────────────────────────────────

def insight_wind(hours: list) -> Optional[str]:
    """
    Daytime wind gusts. Light winds suppressed — not actionable.
    insight_clothing already handles gusts via apparent_temperature + gusts check,
    so this only fires for notable gusts that need explicit warning.
    """
    valid = [h for h in hours if h.get("wind_gusts_10m") is not None and h.get("is_day", 0) == 1]
    if not valid:
        valid = [h for h in hours if h.get("wind_gusts_10m") is not None]
    if not valid:
        return None

    peak = max(valid, key=lambda h: h["wind_gusts_10m"])
    gust = round(peak["wind_gusts_10m"], 1)

    if gust < 40:
        return None  # Not noteworthy

    if gust < 50:
        return f"🌬️ Moderate gusts today ({gust} km/h) — light items may be affected."

    if gust < 65:
        groups = group_consecutive_hours(valid, lambda h: h.get("wind_gusts_10m", 0) >= 50)
        if groups:
            start, end, _ = groups[0]
            return f"⚠️ Strong gusts up to {gust} km/h after {fmt_time(start)} — secure loose items outdoors."
        return f"⚠️ Strong gusts up to {gust} km/h — secure loose items."

    groups = group_consecutive_hours(valid, lambda h: h.get("wind_gusts_10m", 0) >= 65)
    if groups:
        start, end, _ = groups[0]
        return f"🌀 Severe gusts ({gust} km/h) {fmt_time_range(start, end)} — avoid outdoor activity."
    return f"🌀 Severe wind gusts ({gust} km/h) — stay indoors."


def insight_hydration(hours: list) -> Optional[str]:
    """
    Fires when heat index is high AND temperature is warm enough to matter.
    Both the heat index path AND humidity path now have a temperature floor (≥25°C).
    """
    valid = [h for h in hours
             if h.get("temperature_2m") is not None and h.get("relative_humidity_2m") is not None]
    if not valid:
        return None

    hi_rows = [(h, heat_index(h["temperature_2m"], h["relative_humidity_2m"])) for h in valid]
    peak_row, peak_hi = max(hi_rows, key=lambda x: x[1])
    max_temp = max(h.get("temperature_2m", 0) for h in valid)
    avg_hum  = sum(h.get("relative_humidity_2m", 0) for h in valid) / len(valid)

    # Temperature floor — hydration only meaningful when warm enough to sweat
    if max_temp < 25 and peak_hi < 32:
        return None

    if peak_hi >= 45:
        return f"💧 Drink 3.5L+ today (heat index {peak_hi}°C) — sip every 15 min when outside."
    if peak_hi >= 38:
        return f"💧 Drink 3L+ today (heat index {peak_hi}°C) — avoid caffeine and alcohol."
    if peak_hi >= 32:
        return f"💧 Stay well hydrated — heat index reaching {peak_hi}°C. Aim for 2.5L+."
    # Humidity-only path — also needs temperature floor (fixed bug)
    if avg_hum >= 80 and max_temp >= 25:
        return f"💧 High humidity ({round(avg_hum)}%) increases sweat loss — drink water throughout the day."
    return None


# ─────────────────────────────────────────────────────────────────────────────
# TIER 6 — LIFESTYLE AND PLANNING
# ─────────────────────────────────────────────────────────────────────────────

def insight_sleep(hours: list, aqi_hours: list, resolved_aqi: int) -> Optional[str]:
    """
    Overnight comfort for sleep (10PM–6AM). Uses resolved AQI for consistency.
    Only fires when something is notable — comfortable nights are suppressed unless
    it's genuinely perfect (to avoid noise).
    """
    night_h = [h for h in hours
               if h.get("timestamp") and
               (h["timestamp"].hour >= 22 or h["timestamp"].hour < 6)]
    if not night_h:
        return None

    temps = [h.get("apparent_temperature") for h in night_h if h.get("apparent_temperature") is not None]
    humidity = [h.get("relative_humidity_2m") for h in night_h if h.get("relative_humidity_2m") is not None]
    if not temps:
        return None

    avg_temp = sum(temps) / len(temps)
    avg_hum  = sum(humidity) / len(humidity) if humidity else None

    issues = []
    if avg_temp > 28:
        issues.append(f"hot ({round(avg_temp)}°C) — fan or AC recommended")
    elif avg_temp > 24:
        issues.append(f"warm overnight ({round(avg_temp)}°C) — keep windows open")
    elif avg_temp < 14:
        issues.append(f"cold overnight ({round(avg_temp)}°C) — extra blanket needed")

    if avg_hum and avg_hum > 80:
        issues.append(f"high humidity ({round(avg_hum)}%) — may feel sticky")

    # Use resolved_aqi for overnight air quality (same source as insight_air)
    if resolved_aqi is not None and resolved_aqi > 150:
        issues.append("poor air quality — keep windows closed")

    if not issues:
        if 18 <= avg_temp <= 22:
            return f"😴 Good sleep conditions tonight — comfortable {round(avg_temp)}°C overnight."
        return None  # Suppress "fine" nights that aren't notable

    return f"😴 Sleep forecast — {', '.join(issues).capitalize()}."


def insight_cloud_trend(hours: list) -> Optional[str]:
    """
    Detects meaningful directional cloud cover change (≥30% shift).
    Replaces insight_sunshine — more useful because it shows direction of change.
    Suppressed when no meaningful change exists (consistently clear or cloudy days
    are already obvious from the weather card condition label).
    """
    daytime = [h for h in hours if h.get("is_day", 0) == 1 and h.get("cloud_cover") is not None]
    if len(daytime) < 4:
        return None

    mid = len(daytime) // 2
    avg_first  = sum(h["cloud_cover"] for h in daytime[:mid]) / mid
    avg_second = sum(h["cloud_cover"] for h in daytime[mid:]) / (len(daytime) - mid)
    change = avg_second - avg_first

    if abs(change) < 30:
        return None

    threshold = (avg_first + avg_second) / 2
    turning = None
    if change < 0:
        for h in daytime:
            if h["cloud_cover"] <= threshold:
                turning = h["timestamp"]
                break
    else:
        for h in daytime:
            if h["cloud_cover"] >= threshold:
                turning = h["timestamp"]
                break

    tp = f" from {fmt_time(turning)}" if turning else ""

    if change <= -40:
        return f"🌤️ Skies clearing{tp} — cloud cover drops from {round(avg_first)}% to {round(avg_second)}%. Plan outdoor activity for later."
    if change <= -30:
        return f"⛅ Gradual clearing{tp} — morning cloudier ({round(avg_first)}%) but afternoon opens up ({round(avg_second)}%)."
    if change >= 40:
        return f"🌥️ Skies closing in{tp} — cloud cover rises from {round(avg_first)}% to {round(avg_second)}%. Do outdoor plans earlier."
    if change >= 30:
        return f"⛅ Increasing cloud cover{tp} — morning clearer ({round(avg_first)}%) than afternoon ({round(avg_second)}%)."
    return None


def insight_visibility(hours: list) -> Optional[str]:
    """
    Low visibility only — good visibility is suppressed (weather card covers it).
    """
    valid = [h for h in hours if h.get("visibility") is not None]
    if not valid:
        return None

    min_row = min(valid, key=lambda h: h["visibility"])
    vis_m  = min_row["visibility"]
    vis_km = round(vis_m / 1000, 1)

    if vis_m > 5000:
        return None  # Good or better — suppressed

    if vis_m > 2000:
        return f"🌫️ Reduced visibility ({vis_km} km) around {fmt_time(min_row['timestamp'])} — drive carefully."

    if vis_m > 1000:
        groups = group_consecutive_hours(valid, lambda h: h.get("visibility", 99999) <= 2000)
        if groups:
            start, end, _ = groups[0]
            return f"⚠️ Poor visibility ({vis_km} km) {fmt_time_range(start, end)} — use headlights, reduce speed."
        return f"⚠️ Poor visibility ({vis_km} km) around {fmt_time(min_row['timestamp'])} — drive with caution."

    return f"🚨 Very poor visibility ({vis_km} km) — avoid driving if possible."


def insight_best_outdoor_window(hours: list, aqi_hours: list, resolved_aqi: int) -> Optional[str]:
    """
    Best consecutive 2-hour block: low UV + no rain + comfortable temp + good AQI.
    Uses resolved_aqi — no per-hour AQI lookup needed.
    Only considers outdoor-safe hours (5–9AM, 5–7PM).
    """
    if not hours:
        return None

    daytime = _outdoor_hours(hours)
    if len(daytime) < 2:
        return None

    best_score = -1
    best_start = None
    best_end = None
    best_issues = []

    for i in range(len(daytime) - 1):
        window = daytime[i:i+2]
        issues = []
        score = 4

        for row in window:
            uv = row.get("uv_index", 0)
            prob = row.get("precipitation_probability", 0)
            app_temp = row.get("apparent_temperature", 25)
            # Use resolved_aqi as a proxy for the window — consistent with displayed AQI
            aqi_val = resolved_aqi if resolved_aqi is not None else 50

            if uv >= 6:
                score -= 1
                issues.append("high UV")
            if prob >= 30:
                score -= 1
                issues.append("rain risk")
            if not (18 <= app_temp <= 34):
                score -= 1
                issues.append("uncomfortable temp")
            if aqi_val >= 100:
                score -= 1
                issues.append("poor air")

        if score > best_score:
            best_score = score
            best_start = window[0]["timestamp"]
            best_end   = window[-1]["timestamp"]
            best_issues = list(set(issues))

    if best_score == 4:
        return f"🌟 Best time outside: {fmt_time(best_start)}–{fmt_time(best_end)} — low UV, clean air, comfortable temperature."
    if best_score >= 2:
        issue_str = " and ".join(best_issues) if best_issues else "some conditions"
        return f"🌤️ Best outdoor window: {fmt_time(best_start)}–{fmt_time(best_end)} — though {issue_str} may be a factor."
    return None  # No good window — suppress, insight_air / insight_rain will have already warned


def insight_daylight(hours: list, daily: dict) -> Optional[str]:
    """
    Remaining daylight. Only fires when < 2 hours left — otherwise it's noise.
    The >4h and >2h tiers were suppressed because users don't need to be told
    there are still 5 hours of daylight. Only the urgent case matters.
    """
    sunset = daily.get("sunset")
    if not sunset:
        return None
    if isinstance(sunset, str):
        try:
            sunset = datetime.fromisoformat(sunset)
        except Exception:
            return None

    now = datetime.now()
    if now > sunset:
        return None

    remaining_hours = (sunset - now).total_seconds() / 3600

    if remaining_hours > 2:
        return None  # Plenty of day left — suppressed

    return f"🌙 Less than 2 hours of daylight remaining — sunset at {fmt_time(sunset)}."


# ─────────────────────────────────────────────────────────────────────────────
# RAIN STREAK — called from insights.py with 7-day data
# ─────────────────────────────────────────────────────────────────────────────

def insight_rain_streak(daily_rows: list) -> Optional[str]:
    """
    Checks if it has rained (precipitation_sum > 1mm) for 3+ consecutive days
    including today. daily_rows is the full 7-day list passed from insights.py.
    """
    if not daily_rows:
        return None

    from datetime import date as _date
    sorted_rows = sorted(daily_rows, key=lambda r: r.get("date") or _date.min)

    streak = 0
    for row in reversed(sorted_rows):
        precip = row.get("precipitation_sum") or 0
        try:
            precip = float(precip)
        except (TypeError, ValueError):
            precip = 0
        if precip >= 1.0:
            streak += 1
        else:
            break

    if streak < 3:
        return None

    if streak >= 7:
        return (f"🌧️ {streak}-day rain streak — ground is heavily saturated. "
                f"Expect standing water. Waterproof footwear essential.")
    if streak >= 5:
        return f"🌧️ {streak} consecutive rainy days — ground saturated, puddles and mud likely. Waterproof footwear strongly recommended."
    return f"🌧️ Rain for {streak} days in a row — ground saturated. Waterproof footwear recommended."


# ─────────────────────────────────────────────────────────────────────────────
# BONUS INSIGHTS — lifestyle, called separately from generate_bonus_insights
# ─────────────────────────────────────────────────────────────────────────────

def get_best_run_time(hours: list, aqi_hours: list) -> Optional[str]:
    """
    Best hour to exercise outside — scored on UV, rain, apparent temp, AQI.
    Only considers outdoor-safe windows (5–9AM, 5–7PM).
    Returns None if no hour scores ≥5/8 — caller shows get_exercise_air_score instead.
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

    best_score, best_row, best_aqi = -1, None, 200

    for h in daytime:
        uv   = h.get("uv_index", 10)
        prob = h.get("precipitation_probability", 100)
        temp = h.get("apparent_temperature", 0)
        ts   = h["timestamp"]
        aqi_val = aqi_by_hour.get((ts.date(), ts.hour), {}).get("us_aqi", 200)

        score = 0
        if uv < 4:            score += 2
        elif uv < 6:          score += 1
        if prob < 15:         score += 2
        elif prob < 30:       score += 1
        if 18 <= temp <= 28:  score += 2
        elif 15 <= temp <= 33: score += 1
        if aqi_val < 50:      score += 2
        elif aqi_val < 100:   score += 1

        if score > best_score:
            best_score = score
            best_row   = h
            best_aqi   = aqi_val

    if best_row is None or best_score < 5:
        return None

    best_ts = best_row["timestamp"]
    nearby_rain = any(
        h.get("precipitation_probability", 0) >= 50
        for h in hours
        if h.get("timestamp") and abs((h["timestamp"] - best_ts).total_seconds()) <= 7200
    )

    temp_str = f"{round(best_row.get('apparent_temperature', 0))}°C"
    aqi_note = " with clean air" if best_aqi < 100 else ""
    if nearby_rain:
        return (f"🏃 Best time to exercise: {fmt_time(best_ts)} — "
                f"comfortable at {temp_str}{aqi_note}, though rain is nearby. Check first.")
    return f"🏃 Best time to exercise: {fmt_time(best_ts)} — {temp_str}{aqi_note}, low UV."


def get_exercise_air_score(hours: list, aqi_hours: list) -> Optional[str]:
    """
    Air quality verdict for exercise. Only fires when get_best_run_time returns None.
    Mutual-exclusion is built in — re-runs the scorer to confirm.
    """
    daytime_aqi = [h for h in _outdoor_hours(aqi_hours) if h.get("us_aqi") is not None]
    if not daytime_aqi:
        daytime_aqi = [h for h in aqi_hours if h.get("us_aqi") is not None]
    if not daytime_aqi:
        return None

    # Check if best_run_time would fire — suppress if so
    if hours:
        aqi_by_hour = {}
        for row in aqi_hours:
            ts = row["timestamp"]
            aqi_by_hour[(ts.date(), ts.hour)] = row
        for h in _outdoor_hours(hours):
            uv   = h.get("uv_index", 10)
            prob = h.get("precipitation_probability", 100)
            temp = h.get("apparent_temperature", 0)
            ts   = h["timestamp"]
            aqi_val = aqi_by_hour.get((ts.date(), ts.hour), {}).get("us_aqi", 200)
            score = 0
            if uv < 4:            score += 2
            elif uv < 6:          score += 1
            if prob < 15:         score += 2
            elif prob < 30:       score += 1
            if 18 <= temp <= 28:  score += 2
            elif 15 <= temp <= 33: score += 1
            if aqi_val < 50:      score += 2
            elif aqi_val < 100:   score += 1
            if score >= 5:
                return None  # best_run_time fires — suppress this

    aqi_vals  = [h["us_aqi"] for h in daytime_aqi]
    pm25_vals = [h.get("pm2_5") for h in daytime_aqi if h.get("pm2_5") is not None]
    ozone_vals = [h.get("ozone") for h in daytime_aqi if h.get("ozone") is not None]

    avg_aqi   = sum(aqi_vals) / len(aqi_vals)
    avg_pm25  = sum(pm25_vals) / len(pm25_vals) if pm25_vals else None
    avg_ozone = sum(ozone_vals) / len(ozone_vals) if ozone_vals else None

    pm25_bad  = avg_pm25 is not None and avg_pm25 > 55
    ozone_bad = avg_ozone is not None and avg_ozone > 160

    if avg_aqi < 100 and not pm25_bad and not ozone_bad:
        return "🧘 Air quality is suitable for outdoor exercise today."
    if avg_aqi > 150 or pm25_bad:
        return "🚫 Air not safe for outdoor exercise today — work out indoors."
    return "⚠️ Exercise outdoors with caution — air quality is moderate."


def get_laundry_score(hours: list) -> Optional[str]:
    daytime = [h for h in hours if h.get("is_day", 0) == 1]
    if not daytime:
        return None
    max_prob    = max((h.get("precipitation_probability", 100) for h in daytime), default=100)
    avg_cloud   = sum(h.get("cloud_cover", 100) for h in daytime) / len(daytime)
    avg_humidity = sum(h.get("relative_humidity_2m", 100) for h in daytime) / len(daytime)
    avg_wind    = sum(h.get("wind_speed_10m", 0) for h in daytime) / len(daytime)

    if max_prob < 15 and avg_cloud < 40 and avg_humidity < 65 and 5 <= avg_wind <= 25:
        return f"🧺 Great day to dry clothes outside — sunny, low humidity ({round(avg_humidity)}%), light breeze."
    if max_prob >= 50:
        return "🚫 Not ideal for drying clothes outside today — significant rain risk."
    return None


def get_golden_hour(daily: dict) -> Optional[str]:
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

    wmo = daily.get("weather_code_max", 99)
    if wmo not in (0, 1, 2):
        return None

    golden_start = sunset - timedelta(minutes=30)
    blue_start   = sunset + timedelta(minutes=5)
    blue_end     = sunset + timedelta(minutes=35)
    sky_note = "Partly cloudy — clouds may enhance the colours." if wmo == 2 else "Clear skies — clean warm light."

    return (f"📷 Golden hour {fmt_time(golden_start)}–{fmt_time(sunset)} · "
            f"Blue hour {fmt_time(blue_start)}–{fmt_time(blue_end)}. {sky_note}")


def get_tomorrow_summary(tomorrow_hours: list, tomorrow_aqi: list) -> str:
    if not tomorrow_hours:
        return "📅 No forecast data available for tomorrow yet. Check back later."

    temps = [h.get("apparent_temperature") for h in tomorrow_hours if h.get("apparent_temperature") is not None]
    temp_high = round(max(temps), 1) if temps else None
    temp_low  = round(min(temps), 1) if temps else None

    max_rain_prob = max((h.get("precipitation_probability", 0) for h in tomorrow_hours), default=0)
    total_rain    = round(sum(h.get("rain", 0) for h in tomorrow_hours), 1)

    uv_vals = [h.get("uv_index", 0) for h in tomorrow_hours if h.get("is_day", 0) == 1]
    peak_uv = round(max(uv_vals), 1) if uv_vals else 0

    # Tomorrow AQI — use Open-Meteo us_aqi internally, display only EPA category word
    # Cap at 300 to avoid showing artefact values (Open-Meteo forecast can spike unrealistically)
    aqi_vals = [min(h.get("us_aqi") or 0, 300) for h in tomorrow_aqi if h.get("us_aqi") is not None]
    avg_aqi  = round(sum(aqi_vals) / len(aqi_vals)) if aqi_vals else None

    lines = []
    if temp_high is not None and temp_low is not None:
        lines.append(f"🌡️ Feels like {temp_low}–{temp_high}°C")

    if max_rain_prob >= 60:
        lines.append(f"🌧️ Rain likely (~{total_rain}mm, {round(max_rain_prob)}% peak)")
    elif max_rain_prob >= 30:
        lines.append(f"🌦️ Some rain possible ({round(max_rain_prob)}% chance)")
    else:
        lines.append("☀️ Mostly dry")

    if peak_uv >= 8:
        lines.append(f"🕶️ Very high UV ({peak_uv}) — sunscreen essential")
    elif peak_uv >= 5:
        lines.append(f"☀️ Moderate–high UV ({peak_uv}) — apply sunscreen")

    if avg_aqi is not None and avg_aqi > 100:
        cat = _epa_cat(avg_aqi)
        lines.append(f"😷 Air quality forecast: {cat} — limit outdoor activity")
    elif avg_aqi is not None and avg_aqi <= 50:
        lines.append("🌿 Clean air expected tomorrow")

    return "📅 Tomorrow's forecast:\n\n" + "\n".join(lines)


def detect_anomaly(hours: list, historical_avg_temp: float) -> Optional[str]:
    if historical_avg_temp is None:
        return None
    temps = [h.get("temperature_2m") for h in hours if h.get("temperature_2m") is not None]
    if not temps:
        return None
    avg_today = sum(temps) / len(temps)
    diff = round(avg_today - historical_avg_temp, 1)
    if abs(diff) < 4:
        return None
    if diff >= 8:
        return f"🌡️ Unusually hot today — about {diff}°C above normal for this time of year."
    if diff >= 4:
        return f"🌡️ Warmer than usual ({diff}°C above average for this month)."
    if diff <= -8:
        return f"🥶 Unusually cold today — about {abs(diff)}°C below normal for this time of year."
    return f"🧥 Cooler than usual ({abs(diff)}°C below average for this month)."


# ─────────────────────────────────────────────────────────────────────────────
# MASTER ENGINE — generates all insights, returns (visible_text, hidden_text)
# ─────────────────────────────────────────────────────────────────────────────

def generate_insights_split(
    hours: list,
    aqi_hours: list,
    daily: dict,
    current: dict,
    daily_rows: list = None,   # 7-day rows for rain streak — passed from insights.py
) -> tuple:
    """
    Main entry point. Returns (visible_text, hidden_text).
    visible_text: header + tier 1 & 2 (dangerous + rain) — always shown.
    hidden_text:  tier 3–6 — shown when user taps "Show more".
    AQI is resolved once here and passed to all functions that need it.
    """
    # ── Single AQI resolution — used by every function below ─────────────
    resolved_aqi, aqi_cat = resolve_aqi(current, aqi_hours)

    # ── Run all insight functions ─────────────────────────────────────────
    # Each tuple: (tier, insight_text_or_None)
    # Tier 1 = dangerous, Tier 2 = rain, Tier 3–6 = expanded

    heat_stroke_result = insight_heat_stroke(hours)
    heat_stress_result = insight_heat_stress(hours)
    # heat_stress suppressed when heat_stroke fires (heat_stroke is more specific)
    if heat_stroke_result is not None:
        heat_stress_result = None

    checks = [
        (1, heat_stroke_result),
        (1, heat_stress_result),
        (1, insight_frost(hours)),
        (1, insight_snow(hours)),
        (2, insight_rain(hours)),
        (2, insight_rain_streak(daily_rows) if daily_rows else None),
        (3, insight_air(aqi_hours, resolved_aqi, aqi_cat)),
        (3, insight_pollen_combined(aqi_hours)),
        (4, insight_uv(hours)),
        (4, insight_clothing(hours)),
        (4, insight_commute(hours, aqi_hours, resolved_aqi)),
        (5, insight_wind(hours)),
        (5, insight_hydration(hours)),
        (6, insight_sleep(hours, aqi_hours, resolved_aqi)),
        (6, insight_cloud_trend(hours)),
        (6, insight_visibility(hours)),
        (6, insight_best_outdoor_window(hours, aqi_hours, resolved_aqi)),
        (6, insight_daylight(hours, daily)),
    ]

    triggered = [(tier, text) for tier, text in checks if text is not None]
    triggered.sort(key=lambda x: x[0])

    if not triggered:
        return "✅ All conditions are normal for the rest of today. No alerts or advisories.", ""

    # ── Suppress positive lifestyle insights when tier-1 danger exists ────
    has_tier1 = any(tier == 1 for tier, _ in triggered)
    _positive_keywords = (
        "best time to", "best outdoor", "best time outside",
        "great day to dry", "comfortable", "good sleep conditions",
        "skies clearing", "gradual clearing", "plan outdoor activity for later",
        "golden hour", "photography windows",
    )
    if has_tier1:
        triggered = [
            (tier, text) for tier, text in triggered
            if not (tier >= 5 and any(k in text.lower() for k in _positive_keywords))
        ]

    # ── Clothing + rain dedup: if rain insight already mentions umbrella/jacket
    #    and clothing insight would say the same, clothing wins (more specific).
    #    Both can fire — they cover different angles — but if clothing says
    #    "rain jacket essential" and rain also fires at tier 4, they complement
    #    rather than duplicate since rain gives timing and clothing gives gear.
    #    No dedup needed — they're genuinely different.

    # ── Split into visible (tier 1–2) and hidden (tier 3+) ───────────────
    visible_items = [text for tier, text in triggered if tier <= 2]
    hidden_items  = [text for tier, text in triggered if tier > 2]

    # ── Severity-weighted header ──────────────────────────────────────────
    alert_count = sum(1 for tier, _ in triggered if tier <= 2)
    tip_count   = sum(1 for tier, _ in triggered if tier > 2)

    if alert_count > 0 and tip_count > 0:
        header = f"💡 {alert_count} alert{'s' if alert_count > 1 else ''} · {tip_count} tip{'s' if tip_count > 1 else ''}\n\n"
    elif alert_count > 0:
        header = f"⚠️ {alert_count} alert{'s' if alert_count > 1 else ''} today\n\n"
    else:
        header = f"💡 {tip_count} tip{'s' if tip_count > 1 else ''} for today\n\n"

    # If nothing in visible tier (all were tier 3+), promote first 2 items
    if not visible_items and triggered:
        visible_items = [text for _, text in triggered[:2]]
        hidden_items  = [text for _, text in triggered[2:]]

    visible_text = header + "\n\n".join(visible_items)
    hidden_text  = "\n\n".join(hidden_items) if hidden_items else ""

    return visible_text, hidden_text


def generate_insights_from_data(hours: list, aqi_hours: list, daily: dict, current: dict) -> str:
    """
    Backward-compatible entry point. Delegates to generate_insights_split.
    """
    visible, hidden = generate_insights_split(hours, aqi_hours, daily, current)
    if hidden:
        return visible + "\n\n" + hidden
    return visible


# ─────────────────────────────────────────────────────────────────────────────
# DIFFERENTIAL INSIGHTS — "3rd consecutive day of X"
# Called from insights.py alongside the main engine. Takes insight_history rows
# (what tiers fired each day) and produces streak context strings.
# These fire only when the streak is ≥ 3 days and is genuinely informative.
# ─────────────────────────────────────────────────────────────────────────────

_STREAK_LABELS = {
    1: ("dangerous conditions", "🚨"),
    2: ("rainy days",           "🌧️"),
    3: ("poor air quality",     "😷"),
    4: ("high UV",              "🕶️"),
    5: ("strong winds",         "💨"),
}

def build_streak_context(history_rows: list, active_tiers: list) -> str:
    """
    Given recent insight_history rows (each with tiers_json) and today's active
    tiers, returns a single streak-context line for the most severe active tier
    that has been firing for 3+ consecutive days.

    history_rows: list of dicts with keys {insight_date, tiers_json (list of ints)}
                  sorted DESC by date (most recent first). Today's row NOT included
                  — active_tiers covers today.
    active_tiers: list of tier ints that fired today.

    Returns "" if no notable streak exists.
    """
    import json as _json

    if not history_rows or not active_tiers:
        return ""

    best_tier   = min(active_tiers)  # lowest = most severe
    label, icon = _STREAK_LABELS.get(best_tier, ("these conditions", "📊"))

    # Count consecutive days including today
    streak = 1  # today already counts
    for row in history_rows:
        tiers = row.get("tiers_json") or []
        if isinstance(tiers, str):
            try: tiers = _json.loads(tiers)
            except: tiers = []
        if best_tier in tiers:
            streak += 1
        else:
            break

    if streak < 3:
        return ""

    if streak >= 7:
        return f"\n\n📊 _{icon} {streak} days in a row of {label} — one of the longer stretches this month._"
    return f"\n\n📊 _{icon} {label.capitalize()} for {streak} days in a row._"