"""
insights_engine.py
──────────────────
Pure calculation layer for SkyUpdate insights.
Takes lists of hourly dicts (weather + aqi), a daily dict, and a current dict.
Returns a list of insight strings — only the ones that are actually triggered.
No DB calls here — all data is passed in. DB fetching happens in insights.py.

Each insight function:
  - Receives the relevant slice of data
  - Performs a real calculation (argmax, grouping, trend, formula)
  - Returns a formatted string or None if suppressed
"""

from datetime import datetime, timedelta
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# OUTDOOR ACTIVITY WINDOW
# Exercise / outdoor activity is only suggested during safe hours:
#   Morning  : 05:00–09:00
#   Evening  : 17:00–19:00
# Any function recommending outdoor activity filters to these windows only.
# ─────────────────────────────────────────────────────────────────────────────

def _is_outdoor_hour(ts) -> bool:
    """Returns True if the timestamp falls in the allowed outdoor window."""
    if ts is None:
        return False
    h = ts.hour if hasattr(ts, "hour") else int(str(ts)[11:13])
    return (5 <= h < 9) or (17 <= h < 19)

def _outdoor_hours(hours: list) -> list:
    """Filter a list of hourly dicts to only the allowed outdoor windows."""
    return [h for h in hours if _is_outdoor_hour(h.get("timestamp"))]



# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def fmt_time(dt) -> str:
    """Format a datetime to 12-hour clock string e.g. '3:00 PM'"""
    if dt is None:
        return "N/A"
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt)
        except Exception:
            return dt
    return dt.strftime("%I:%M %p").lstrip("0")

def fmt_time_range(start, end) -> str:
    """
    Format a time range. If start == end (single-hour group), returns
    'around 3:00 PM' instead of '3:00 PM–3:00 PM'.
    """
    if start == end or (hasattr(start, 'hour') and hasattr(end, 'hour') and start.hour == end.hour):
        return f"around {fmt_time(start)}"
    return f"{fmt_time(start)}–{fmt_time(end)}"

def group_consecutive_hours(hours: list, key_check) -> list:
    """
    Given a list of hourly dicts and a condition function,
    returns list of (start_dt, end_dt, list_of_matching_rows) groups
    for consecutive hours where key_check(row) is True.
    e.g. rain hours 3pm,4pm,5pm → one group (3pm, 5pm, [rows])
    End time is the timestamp of the LAST matching hour (not +1h),
    so single-hour groups have start == end and fmt_time_range handles display.
    """
    groups = []
    current_group = []

    for row in hours:
        if key_check(row):
            current_group.append(row)
        else:
            if current_group:
                start = current_group[0]["timestamp"]
                end   = current_group[-1]["timestamp"]
                groups.append((start, end, current_group))
                current_group = []

    if current_group:
        start = current_group[0]["timestamp"]
        end   = current_group[-1]["timestamp"]
        groups.append((start, end, current_group))

    return groups


def heat_index(temp_c: float, humidity: float) -> float:
    """
    US NWS Heat Index formula.
    Input: temperature in Celsius, relative humidity as percentage (0-100).
    Output: heat index in Celsius.
    Only meaningful above 27°C — below that, returns temp_c unchanged.
    """
    if temp_c < 27:
        return temp_c
    T = temp_c
    H = humidity
    hi = (-8.78469475556
          + 1.61139411 * T
          + 2.3385170 * H
          - 0.14611605 * T * H
          - 0.01230809 * T ** 2
          - 0.01642482 * H ** 2
          + 0.00221173 * T ** 2 * H
          + 0.00072546 * T * H ** 2
          - 0.00000358 * T ** 2 * H ** 2)
    return round(hi, 1)


# ─────────────────────────────────────────────────────────────────────────────
# INSIGHT 1 — APPARENT TEMPERATURE (Feels Like)
# ─────────────────────────────────────────────────────────────────────────────

def insight_temperature(hours: list) -> Optional[str]:
    valid = [h for h in hours if h.get("apparent_temperature") is not None]
    if not valid:
        return None

    peak_row = max(valid, key=lambda h: h["apparent_temperature"])
    peak_val = round(peak_row["apparent_temperature"], 1)
    peak_time = fmt_time(peak_row["timestamp"])

    # Find cold window if evening drops significantly
    cold_rows = [h for h in valid if h.get("apparent_temperature", 99) < 20]

    if peak_val < 10:
        return f"🥶 Very cold today, feels like {peak_val}°C. Stay indoors or layer up heavily."
    elif peak_val < 20:
        return f"🧥 Cool day, peaks at {peak_val}°C around {peak_time}. A jacket will be comfortable."
    elif peak_val < 30:
        return f"😊 Comfortable temperature, peaking at {peak_val}°C around {peak_time}. Great day to go out."
    elif peak_val < 38:
        msg = f"🌡️ Warm afternoon — feels like {peak_val}°C around {peak_time}. Stay hydrated."
        if cold_rows:
            cold_val = round(min(h["apparent_temperature"] for h in cold_rows), 1)
            cold_time = fmt_time(cold_rows[0]["timestamp"])
            msg += f" Cools to {cold_val}°C from {cold_time}."
        return msg
    else:
        # Find the dangerous window (above 38°C)
        hot_groups = group_consecutive_hours(valid, lambda h: h.get("apparent_temperature", 0) >= 38)
        if hot_groups:
            start, end, group_rows = hot_groups[0]
            return (f"🔥 Dangerous heat — feels like up to {peak_val}°C between "
                    f"{fmt_time_range(start, end)}. Avoid going out during this window.")
        return f"🔥 Dangerous heat — feels like {peak_val}°C around {peak_time}. Avoid outdoor activity."


# ─────────────────────────────────────────────────────────────────────────────
# INSIGHT 2 — RAIN
# ─────────────────────────────────────────────────────────────────────────────

def insight_rain(hours: list) -> Optional[str]:
    """
    Improved rain insight with:
    - Cumulative risk check for borderline "no rain" cases
    - Window grouping for medium-probability clusters
    - mm/hr intensity classification (light/moderate/heavy)
    - "Leave before rain" framing based on when rain starts
    """
    valid = [h for h in hours if h.get("precipitation_probability") is not None]
    if not valid:
        return None

    max_prob = max(h["precipitation_probability"] for h in valid)

    # ── Tier 1: Truly negligible ──────────────────────────────────────────
    if max_prob < 10:
        # Check cumulative risk: if many hours are all near the threshold,
        # the actual chance of staying dry all day is lower than it looks
        high_count = sum(1 for h in valid if h.get("precipitation_probability", 0) >= 6)
        if high_count >= 4:
            return (f"🌤️ Rain unlikely but not impossible — {high_count} hours with ~{round(max_prob)}% chance. "
                    f"Probably fine without an umbrella.")
        return "☀️ No rain expected for the rest of today. Leave the umbrella at home."

    # ── Tier 2: Low but non-trivial (10–29%) ─────────────────────────────
    elif max_prob < 30:
        # Group to see if it's a brief spike or sustained low-level risk
        low_groups = group_consecutive_hours(
            valid, lambda h: h.get("precipitation_probability", 0) >= 15
        )
        if low_groups:
            # Report the longest sustained window
            longest = max(low_groups, key=lambda g: (g[1] - g[0]).seconds)
            start, end, _ = longest
            return (f"🌤️ Low rain chance ({round(max_prob)}% peak) — "
                    f"mainly around {fmt_time_range(start, end)}. "
                    f"Unlikely to affect your plans but keep an eye out.")
        return f"🌤️ Very low rain chance (max {round(max_prob)}%). Unlikely to affect your plans."

    # ── Tier 3: Moderate risk (30–59%) — group clusters ──────────────────
    elif max_prob < 60:
        rain_groups = group_consecutive_hours(
            valid, lambda h: h.get("precipitation_probability", 0) >= 30
        )
        if not rain_groups:
            peak_row = max(valid, key=lambda h: h["precipitation_probability"])
            return (f"🌦️ Some rain possible around {fmt_time(peak_row['timestamp'])} "
                    f"({round(peak_row['precipitation_probability'])}% chance). Keep an umbrella handy.")

        # Report all clusters, sorted by start time
        rain_groups.sort(key=lambda g: g[0])
        total_mm = round(sum(r.get("rain", 0) for g in rain_groups for r in g[2]), 1)

        if len(rain_groups) == 1:
            start, end, rows = rain_groups[0]
            peak = round(max(r.get("precipitation_probability", 0) for r in rows))
            mm_str = f" (~{total_mm}mm)" if total_mm >= 1 else ""
            return (f"🌦️ Rain possible {fmt_time_range(start, end)}{mm_str} "
                    f"({peak}% peak chance). Take an umbrella if going out then.")
        else:
            windows = ", ".join(
                f"{fmt_time(g[0])}–{fmt_time(g[1])}" for g in rain_groups
            )
            return (f"🌦️ Scattered rain possible in {len(rain_groups)} windows: {windows}. "
                    f"Keep an umbrella handy throughout the day.")

    # ── Tier 4: Likely rain (≥60%) — intensity + "leave before" framing ──
    else:
        rain_groups = group_consecutive_hours(
            valid,
            lambda h: h.get("precipitation_probability", 0) >= 60
        )

        if not rain_groups:
            # High prob but grouper found nothing — use peak hour
            peak_row = max(valid, key=lambda h: h["precipitation_probability"])
            return (f"🌧️ Rain likely around {fmt_time(peak_row['timestamp'])} "
                    f"({round(peak_row['precipitation_probability'])}%). Carry an umbrella.")

        # Sort all windows by start time for chronological display
        rain_groups.sort(key=lambda g: g[0])

        # "Leave before" framing based on the FIRST upcoming window
        now = __import__("datetime").datetime.now()
        first_start = rain_groups[0][0]
        mins_until = int((first_start - now).total_seconds() / 60) if first_start > now else 0

        if mins_until > 90:
            timing_prefix = f"Rain starts around {fmt_time(first_start)} — you have time to leave before it hits."
        elif mins_until > 30:
            timing_prefix = f"Rain starts around {fmt_time(first_start)} — leave soon if you need to go out."
        elif mins_until > 0:
            timing_prefix = f"Rain starting very soon ({mins_until} min) — take an umbrella now."
        else:
            timing_prefix = ""

        if len(rain_groups) == 1:
            start, end, group_rows = rain_groups[0]
            total_mm        = round(sum(r.get("rain", 0) for r in group_rows), 1)
            max_prob_window = round(max(r.get("precipitation_probability", 0) for r in group_rows))
            duration_hrs    = len(group_rows)
            mm_per_hr = total_mm / duration_hrs if duration_hrs > 0 else 0
            if mm_per_hr >= 7.5 or max_prob_window >= 85:
                intensity, emoji = "heavy", "⛈️"
            elif mm_per_hr >= 2.5:
                intensity, emoji = "moderate", "🌧️"
            else:
                intensity, emoji = "light", "🌦️"
            mm_str = f" (~{total_mm}mm)" if total_mm >= 0.5 else ""
            timing = f" {timing_prefix}" if timing_prefix else ""
            return (f"{emoji} {intensity.capitalize()} rain {fmt_time_range(start, end)}{mm_str} "
                    f"({max_prob_window}% chance).{timing}")
        else:
            # Multiple windows — list all of them clearly
            window_parts = []
            total_mm_all = 0
            for g_start, g_end, g_rows in rain_groups:
                g_mm = round(sum(r.get("rain", 0) for r in g_rows), 1)
                g_prob = round(max(r.get("precipitation_probability", 0) for r in g_rows))
                total_mm_all += g_mm
                mm_part = f" ~{g_mm}mm" if g_mm >= 0.5 else ""
                window_parts.append(f"{fmt_time_range(g_start, g_end)} ({g_prob}%{mm_part})")
            windows_str = " and ".join(window_parts) if len(window_parts) == 2 else ", ".join(window_parts[:-1]) + f" and {window_parts[-1]}"
            total_str = f" (~{round(total_mm_all, 1)}mm total)" if total_mm_all >= 0.5 else ""
            timing = f" {timing_prefix}" if timing_prefix else ""
            return (f"🌧️ Rain expected in {len(rain_groups)} windows today: {windows_str}{total_str}. "
                    f"Carry an umbrella all day.{timing}")


# ─────────────────────────────────────────────────────────────────────────────
# INSIGHT 3 — UV INDEX
# ─────────────────────────────────────────────────────────────────────────────

def insight_uv(hours: list) -> Optional[str]:
    # Only check daytime hours
    valid = [h for h in hours if h.get("uv_index") is not None and h.get("is_day", 0) == 1]
    if not valid:
        return None

    peak_row = max(valid, key=lambda h: h["uv_index"])
    peak_val = round(peak_row["uv_index"], 1)

    if peak_val < 3:
        return None  # Suppressed — UV is low, no action needed

    elif peak_val < 6:
        return (f"🌤️ Moderate UV today (peaks at {peak_val} around {fmt_time(peak_row['timestamp'])}). "
                f"Sunscreen recommended if you're outside for extended periods.")

    elif peak_val < 8:
        uv_groups = group_consecutive_hours(valid, lambda h: h.get("uv_index", 0) >= 6)
        if uv_groups:
            start, end, _ = uv_groups[0]
            return (f"☀️ High UV ({peak_val}) between {fmt_time_range(start, end)}. "
                    f"Apply SPF 30+ and seek shade.")
        return f"☀️ High UV ({peak_val}) around {fmt_time(peak_row['timestamp'])}. Apply SPF 30+."

    elif peak_val < 11:
        uv_groups = group_consecutive_hours(valid, lambda h: h.get("uv_index", 0) >= 8)
        if uv_groups:
            start, end, _ = uv_groups[0]
            return (f"🕶️ Very high UV ({peak_val}) between {fmt_time_range(start, end)}. "
                    f"Limit sun exposure, wear sunscreen and a hat.")
        return f"🕶️ Very high UV ({peak_val}) around {fmt_time(peak_row['timestamp'])}. Limit sun exposure."

    else:
        uv_groups = group_consecutive_hours(valid, lambda h: h.get("uv_index", 0) >= 11)
        if uv_groups:
            start, end, _ = uv_groups[0]
            return (f"🚨 Extreme UV ({peak_val}) between {fmt_time_range(start, end)}. "
                    f"Avoid being outside during this window entirely.")
        return f"🚨 Extreme UV ({peak_val}) around {fmt_time(peak_row['timestamp'])}. Stay indoors."


# ─────────────────────────────────────────────────────────────────────────────
# INSIGHT 4 — WIND
# ─────────────────────────────────────────────────────────────────────────────

def insight_wind(hours: list) -> Optional[str]:
    # Only consider remaining daytime hours — a 2 AM gust is irrelevant to the user
    valid = [
        h for h in hours
        if h.get("wind_gusts_10m") is not None and h.get("is_day", 0) == 1
    ]
    # If no daytime hours left, fall back to all remaining hours
    if not valid:
        valid = [h for h in hours if h.get("wind_gusts_10m") is not None]
    if not valid:
        return None

    peak_gust_row = max(valid, key=lambda h: h["wind_gusts_10m"])
    peak_gust = round(peak_gust_row["wind_gusts_10m"], 1)
    peak_speed = round(peak_gust_row.get("wind_speed_10m", 0), 1)

    if peak_gust < 15:
        return None  # Suppressed — calm winds

    elif peak_gust < 30:
        return f"🍃 Light winds today ({peak_speed} km/h, gusts to {peak_gust} km/h). Pleasant conditions."

    elif peak_gust < 50:
        return (f"🌬️ Moderate wind around {fmt_time(peak_gust_row['timestamp'])} "
                f"(gusts to {peak_gust} km/h). Hair and light items may be affected.")

    elif peak_gust < 65:
        wind_groups = group_consecutive_hours(valid, lambda h: h.get("wind_gusts_10m", 0) >= 50)
        if wind_groups:
            start, end, _ = wind_groups[0]
            return (f"⚠️ Strong gusts up to {peak_gust} km/h after {fmt_time(start)}. "
                    f"Secure loose items outdoors.")
        return f"⚠️ Strong gusts up to {peak_gust} km/h around {fmt_time(peak_gust_row['timestamp'])}. Secure loose items."

    else:
        wind_groups = group_consecutive_hours(valid, lambda h: h.get("wind_gusts_10m", 0) >= 65)
        if wind_groups:
            start, end, _ = wind_groups[0]
            return (f"🌀 Severe wind gusts ({peak_gust} km/h) between {fmt_time_range(start, end)}. "
                    f"Avoid outdoor activity during this window.")
        return f"🌀 Severe wind gusts ({peak_gust} km/h) around {fmt_time(peak_gust_row['timestamp'])}. Stay indoors."


# ─────────────────────────────────────────────────────────────────────────────
# INSIGHT 5 — WIND CHILL
# ─────────────────────────────────────────────────────────────────────────────

def insight_wind_chill(hours: list) -> Optional[str]:
    valid = [h for h in hours
             if h.get("temperature_2m") is not None and h.get("apparent_temperature") is not None]
    if not valid:
        return None

    # Wind chill = how much colder apparent feels vs actual temperature
    # Positive value means apparent is COLDER than actual
    chills = [(h, round(h["temperature_2m"] - h["apparent_temperature"], 1))
              for h in valid if h["temperature_2m"] - h["apparent_temperature"] > 0]

    if not chills:
        return None

    peak_row, peak_diff = max(chills, key=lambda x: x[1])

    if peak_diff < 2:
        return None  # Suppressed

    elif peak_diff < 4:
        return (f"🌬️ Wind makes it feel {peak_diff}°C cooler than actual temperature "
                f"around {fmt_time(peak_row['timestamp'])}.")

    elif peak_diff < 6:
        return (f"🌬️ Noticeable wind chill — feels {peak_diff}°C colder than actual "
                f"around {fmt_time(peak_row['timestamp'])}. Layer up.")

    elif peak_diff < 8:
        chill_groups = group_consecutive_hours(
            valid, lambda h: (h["temperature_2m"] - h.get("apparent_temperature", h["temperature_2m"])) >= 4
        )
        if chill_groups:
            start, end, _ = chill_groups[0]
            return f"🥶 Strong wind chill between {fmt_time_range(start, end)} — feels {peak_diff}°C colder than actual."
        return f"🥶 Strong wind chill — feels {peak_diff}°C colder than actual around {fmt_time(peak_row['timestamp'])}."

    else:
        return (f"🥶 Severe wind chill — feels {peak_diff}°C colder than actual temperature "
                f"around {fmt_time(peak_row['timestamp'])}. Dress warmly.")


# ─────────────────────────────────────────────────────────────────────────────
# INSIGHT 6 — CLOUD COVER AND SUNSHINE
# ─────────────────────────────────────────────────────────────────────────────

def insight_sunshine(hours: list) -> Optional[str]:
    daytime = [h for h in hours if h.get("is_day", 0) == 1 and h.get("cloud_cover") is not None]
    if not daytime:
        return None

    avg_cloud = sum(h["cloud_cover"] for h in daytime) / len(daytime)

    # Calculate actual sunshine hours from sunshine_duration (seconds per hour)
    sunshine_hours = sum(
        h.get("sunshine_duration", 0) / 3600
        for h in daytime
        if h.get("sunshine_duration") is not None
    )
    sunshine_hours = round(sunshine_hours, 1)

    if avg_cloud < 20:
        return f"☀️ Clear skies for the rest of today — {sunshine_hours} hours of sunshine expected. Perfect for outdoor plans."
    elif avg_cloud < 40:
        return f"🌤️ Mostly sunny with {sunshine_hours} hours of clear sky. Good day to be outside."
    elif avg_cloud < 60:
        # Find clearest window
        clear_groups = group_consecutive_hours(daytime, lambda h: h.get("cloud_cover", 100) < 40)
        if clear_groups:
            start, end, _ = clear_groups[0]
            return f"⛅ Partly cloudy — clearest window between {fmt_time_range(start, end)} ({sunshine_hours} sunshine hrs total)."
        return f"⛅ Partly cloudy today with around {sunshine_hours} hours of sunshine."
    elif avg_cloud < 80:
        return f"🌥️ Mostly cloudy today ({sunshine_hours} hrs sunshine). UV still possible through clouds."
    else:
        return "☁️ Overcast for the rest of today. Little to no direct sunshine expected."


# ─────────────────────────────────────────────────────────────────────────────
# INSIGHT 7 — AQI
# Display: weather.com scraped AQI (scraped_aqi_value / scraped_aqi_category).
# Fallback: Open-Meteo us_aqi — shown with " (OM)" suffix so user knows source.
# us_aqi from hourly_aqi is stored in DB and used for internal scoring only —
# it is never shown raw to the user as the headline AQI number.
# ─────────────────────────────────────────────────────────────────────────────



# ─────────────────────────────────────────────────────────────────────────────
# INSIGHT NEW — CLOUD COVER TREND
# Detects if skies are clearing or worsening, not just the average.
# Only fires when there is a meaningful directional change (≥30% shift).
# ─────────────────────────────────────────────────────────────────────────────

def insight_cloud_trend(hours: list) -> Optional[str]:
    """
    Detects direction of cloud cover change across the remaining day.
    Splits daytime into first half and second half and compares averages.
    Only fires when there is a meaningful shift — not for consistently cloudy
    or consistently clear days (insight_sunshine handles those).
    """
    daytime = [h for h in hours if h.get("is_day", 0) == 1 and h.get("cloud_cover") is not None]
    if len(daytime) < 4:
        return None

    mid = len(daytime) // 2
    first_half  = daytime[:mid]
    second_half = daytime[mid:]

    avg_first  = sum(h["cloud_cover"] for h in first_half)  / len(first_half)
    avg_second = sum(h["cloud_cover"] for h in second_half) / len(second_half)
    change = avg_second - avg_first  # positive = more cloud later

    if abs(change) < 30:
        return None  # Not enough directional shift to be worth mentioning

    # Find the transition point (hour where cloud cover crosses midpoint)
    threshold = (avg_first + avg_second) / 2
    turning_point = None
    if change < 0:  # clearing — find first hour where cloud drops below threshold
        for h in daytime:
            if h["cloud_cover"] <= threshold:
                turning_point = h["timestamp"]
                break
    else:  # worsening — find first hour where cloud exceeds threshold
        for h in daytime:
            if h["cloud_cover"] >= threshold:
                turning_point = h["timestamp"]
                break

    if change <= -40:
        tp_str = f" from around {fmt_time(turning_point)}" if turning_point else ""
        return (f"🌤️ Skies clearing{tp_str} — cloud cover drops from {round(avg_first)}% to "
                f"{round(avg_second)}% by afternoon. Plan outdoor activity for later.")

    if change <= -30:
        tp_str = f" after {fmt_time(turning_point)}" if turning_point else ""
        return (f"⛅ Gradual clearing expected{tp_str}. Morning is cloudier "
                f"({round(avg_first)}%) but afternoon opens up ({round(avg_second)}%).")

    if change >= 40:
        tp_str = f" from around {fmt_time(turning_point)}" if turning_point else ""
        return (f"🌥️ Skies closing in{tp_str} — cloud cover rises from {round(avg_first)}% to "
                f"{round(avg_second)}% through the day. Get outdoor plans done earlier.")

    if change >= 30:
        tp_str = f" after {fmt_time(turning_point)}" if turning_point else ""
        return (f"⛅ Increasing cloud cover expected{tp_str} — morning is clearer "
                f"({round(avg_first)}%) than the afternoon ({round(avg_second)}%).")

    return None

def insight_aqi(aqi_hours: list, current: dict) -> Optional[str]:
    """
    Shows the user-facing AQI insight.
    Priority:
      1. scraped_aqi_value from weather_scraped (weather.com current reading)
      2. If None, fall back to current us_aqi from Open-Meteo with "(OM)" suffix
    Category thresholds use the scraped_aqi_category string if available,
    or derive it from the numeric value using EPA breakpoints.
    """
    # ── Determine display value and category ──────────────────────────────
    scraped_val = current.get("scraped_aqi_value")   # e.g. "219" or None
    scraped_cat = current.get("scraped_aqi_category") # e.g. "Unhealthy" or None

    if scraped_val is not None:
        try:
            display_num = round(float(scraped_val))
        except (ValueError, TypeError):
            display_num = None
        display_str = str(display_num) if display_num is not None else scraped_val
        source_suffix = ""
    else:
        # Fallback: use Open-Meteo current us_aqi
        om_val = current.get("us_aqi")
        if om_val is None:
            # Last resort: take the first available hourly value
            valid = [h for h in aqi_hours if h.get("us_aqi") is not None]
            om_val = valid[0]["us_aqi"] if valid else None
        if om_val is None:
            return None
        display_num   = round(om_val)
        display_str   = str(display_num)
        source_suffix = " (OM)"  # label so user knows this is Open-Meteo, not weather.com

        # Derive category from EPA breakpoints since weather.com category is unavailable
        if om_val <= 50:   scraped_cat = "Good"
        elif om_val <= 100: scraped_cat = "Moderate"
        elif om_val <= 150: scraped_cat = "Unhealthy for Sensitive Groups"
        elif om_val <= 200: scraped_cat = "Unhealthy"
        elif om_val <= 300: scraped_cat = "Very Unhealthy"
        else:               scraped_cat = "Hazardous"

    if display_num is None:
        return None

    # ── Generate insight text using display_num for thresholds ───────────
    cat_str = f" — {scraped_cat}" if scraped_cat else ""

    if display_num <= 50:
        return f"🌿 Excellent air quality today (AQI {display_str}{source_suffix}{cat_str}). Good time to be outside."

    elif display_num <= 100:
        return f"😐 Acceptable air quality (AQI {display_str}{source_suffix}{cat_str}). Most people can go about their day normally."

    elif display_num <= 150:
        return (f"😷 Moderate air quality (AQI {display_str}{source_suffix}{cat_str}). "
                f"Sensitive groups should limit prolonged outdoor activity.")

    elif display_num <= 200:
        return (f"⚠️ Unhealthy air quality (AQI {display_str}{source_suffix}{cat_str}). "
                f"Everyone should reduce outdoor exertion, especially children and elderly.")

    elif display_num <= 300:
        return (f"🚨 Very unhealthy air quality (AQI {display_str}{source_suffix}{cat_str}). "
                f"Stay indoors if possible and keep windows closed.")

    else:
        return (f"☠️ Hazardous air quality (AQI {display_str}{source_suffix}{cat_str}). "
                f"Do not go outside. Keep all windows shut.")


# ─────────────────────────────────────────────────────────────────────────────
# INSIGHT 8 — MASK RECOMMENDATION
# Combines PM2.5, PM10 and dust to give a single actionable mask advice.
# Mask types: none → surgical → N95 → N99
# ─────────────────────────────────────────────────────────────────────────────

def insight_mask(aqi_hours: list) -> Optional[str]:
    """
    Recommends the right mask based on combined PM2.5, PM10 and dust levels.
    N95 filters ≥95% of particles ≥0.3µm — recommended above PM2.5 > 55.
    N99 for truly hazardous conditions PM2.5 > 150.
    Surgical mask recommended for mild PM10 or dust elevation.
    No mask needed when all values are safe.
    """
    pm25_vals  = [h.get("pm2_5") for h in aqi_hours if h.get("pm2_5") is not None]
    pm10_vals  = [h.get("pm10")  for h in aqi_hours if h.get("pm10")  is not None]
    dust_vals  = [h.get("dust")  for h in aqi_hours if h.get("dust")  is not None]

    peak_pm25 = round(max(pm25_vals), 1) if pm25_vals else 0
    peak_pm10 = round(max(pm10_vals), 1) if pm10_vals else 0
    peak_dust = round(max(dust_vals), 1) if dust_vals else 0

    # N99 — extremely hazardous
    if peak_pm25 >= 150:
        return (f"🚨 Hazardous air — N99 mask essential outdoors "
                f"(PM2.5 {peak_pm25} µg/m³). Stay indoors if possible.")

    # N95 — unhealthy for all
    if peak_pm25 >= 55:
        return (f"😷 N95 mask recommended outdoors "
                f"(PM2.5 {peak_pm25} µg/m³). Filters 95% of fine particles.")

    # N95 for sensitive groups
    if peak_pm25 >= 35:
        return (f"😷 N95 mask advised for sensitive groups "
                f"(PM2.5 {peak_pm25} µg/m³). Healthy adults can use surgical mask.")

    # Surgical mask for elevated PM10 or dust
    if peak_pm10 >= 150 or peak_dust >= 100:
        detail = f"PM10 {peak_pm10} µg/m³" if peak_pm10 >= 150 else f"dust {peak_dust} µg/m³"
        return (f"🌫️ Surgical mask sufficient for outdoor activity "
                f"({detail}). No N95 required.")

    # Clean air — no mask
    if peak_pm25 < 12 and peak_pm10 < 50 and peak_dust < 25:
        return "🌿 Air quality is clean — no mask needed today."

    return None


# ─────────────────────────────────────────────────────────────────────────────
# INSIGHT 9 — OZONE
# ─────────────────────────────────────────────────────────────────────────────

def insight_ozone(aqi_hours: list) -> Optional[str]:
    valid = [h for h in aqi_hours if h.get("ozone") is not None]
    if not valid:
        return None

    peak_row = max(valid, key=lambda h: h["ozone"])
    peak_val = round(peak_row["ozone"], 1)

    if peak_val < 100:
        return None  # Suppressed

    elif peak_val < 140:
        return None  # Normal range — suppressed

    elif peak_val < 180:
        ozone_groups = group_consecutive_hours(valid, lambda h: h.get("ozone", 0) >= 140)
        if ozone_groups:
            start, end, _ = ozone_groups[0]
            return (f"😷 Moderate ozone ({peak_val} µg/m³) between {fmt_time_range(start, end)}. "
                    f"Avoid heavy exercise outdoors during this window.")
        return f"😷 Moderate ozone levels ({peak_val} µg/m³). Limit outdoor exertion."

    else:
        ozone_groups = group_consecutive_hours(valid, lambda h: h.get("ozone", 0) >= 180)
        if ozone_groups:
            start, end, _ = ozone_groups[0]
            return (f"🚨 High ozone ({peak_val} µg/m³) between {fmt_time_range(start, end)}. "
                    f"Stay indoors during this window.")
        return f"🚨 High ozone levels ({peak_val} µg/m³). Limit outdoor activity."





# ─────────────────────────────────────────────────────────────────────────────
# INSIGHT 11/12/13 — POLLEN (Grass, Alder, Birch)
# ─────────────────────────────────────────────────────────────────────────────



# ─────────────────────────────────────────────────────────────────────────────
# INSIGHT NEW — RESPIRATORY ADVISORY
# Combines ozone + PM2.5 + pollen into one respiratory health score.
# Replaces insight_ozone as the single respiratory risk signal.
# Specifically useful for asthma, COPD, allergy sufferers.
# ─────────────────────────────────────────────────────────────────────────────

def insight_respiratory(aqi_hours: list) -> Optional[str]:
    """
    Calculates a combined respiratory stress score from:
    - Ozone (µg/m³): above 140 = moderate stress, above 180 = high
    - PM2.5 (µg/m³): above 35 = moderate, above 55 = high
    - Pollen (grass + alder + birch, grains/m³): above 30 any = moderate, above 60 = high

    Fires only when at least 2 of the 3 factors are elevated, or 1 is severely elevated.
    Gives one unified message covering all active respiratory risks.
    """
    pm25_vals  = [h.get("pm2_5")  for h in aqi_hours if h.get("pm2_5")  is not None]
    ozone_vals = [h.get("ozone")  for h in aqi_hours if h.get("ozone")  is not None]

    # Pollen — take the highest of all three types
    pollen_peaks = []
    for key in ("grass_pollen", "alder_pollen", "birch_pollen"):
        vals = [h.get(key) for h in aqi_hours if h.get(key) is not None]
        if vals:
            pollen_peaks.append(max(vals))
    peak_pollen = max(pollen_peaks) if pollen_peaks else 0

    peak_pm25  = max(pm25_vals)  if pm25_vals  else 0
    peak_ozone = max(ozone_vals) if ozone_vals else 0

    # Score each factor: 0 = fine, 1 = moderate, 2 = high
    pm25_score  = 2 if peak_pm25 >= 55  else (1 if peak_pm25 >= 35  else 0)
    ozone_score = 2 if peak_ozone >= 180 else (1 if peak_ozone >= 140 else 0)
    pollen_score= 2 if peak_pollen >= 60 else (1 if peak_pollen >= 30 else 0)

    total_score = pm25_score + ozone_score + pollen_score

    if total_score == 0:
        return None  # All fine — no respiratory advisory needed

    # Build active factor list for message
    factors = []
    if pm25_score >= 1:
        factors.append(f"PM2.5 {round(peak_pm25, 1)} µg/m³")
    if ozone_score >= 1:
        factors.append(f"ozone {round(peak_ozone)} µg/m³")
    if pollen_score >= 1:
        pollen_name = "grass" if peak_pollen == max(pollen_peaks) and pollen_peaks else "pollen"
        factors.append(f"{pollen_name} pollen active")

    factor_str = " + ".join(factors)

    if total_score >= 4:
        return (f"😮‍💨 High respiratory stress today — {factor_str}. "
                f"People with asthma or COPD should stay indoors and carry inhalers. "
                f"N95 mask essential if going outside.")

    if total_score >= 2:
        action = "carry inhalers if you have respiratory conditions" if pm25_score + ozone_score >= 2 else "take antihistamines before going out"
        return (f"😮‍💨 Moderate respiratory stress — {factor_str}. "
                f"Sensitive individuals should {action}.")

    # Score of 1 — only one factor mildly elevated
    if pm25_score == 1 or ozone_score == 1:
        return (f"😮‍💨 Mild respiratory irritants today — {factor_str}. "
                f"Generally fine for healthy adults, but those with respiratory conditions should take precautions.")

    return None

def insight_pollen(aqi_hours: list, pollen_key: str, name: str, emoji: str) -> Optional[str]:
    valid = [h for h in aqi_hours if h.get(pollen_key) is not None]
    if not valid:
        return None

    peak_row = max(valid, key=lambda h: h[pollen_key])
    peak_val = round(peak_row[pollen_key], 1)

    if peak_val < 10:
        return None  # Suppressed

    elif peak_val < 30:
        return f"{emoji} Low {name} pollen today ({peak_val}). Allergy sufferers generally unaffected."

    elif peak_val < 60:
        return (f"🤧 Moderate {name} pollen ({peak_val}) around {fmt_time(peak_row['timestamp'])}. "
                f"Consider antihistamines before going out.")

    elif peak_val < 120:
        pollen_groups = group_consecutive_hours(valid, lambda h: h.get(pollen_key, 0) >= 60)
        if pollen_groups:
            start, end, _ = pollen_groups[0]
            return (f"⚠️ High {name} pollen ({peak_val}) between {fmt_time_range(start, end)}. "
                    f"Allergy sufferers stay indoors if possible.")
        return f"⚠️ High {name} pollen ({peak_val}). Take antihistamines and limit outdoor time."

    else:
        return (f"🚨 Very high {name} pollen ({peak_val}) between "
                f"{fmt_time(peak_row['timestamp'])}. Avoid outdoor activity if allergic.")


# ─────────────────────────────────────────────────────────────────────────────
# INSIGHT 14 — HEAT STRESS (Heat Index formula — NWS)
# ─────────────────────────────────────────────────────────────────────────────



# ─────────────────────────────────────────────────────────────────────────────
# INSIGHT NEW — HEAT STROKE RISK WINDOW
# Fires only when temp + humidity + UV all align simultaneously in daytime.
# More specific than heat_stress — that fires on heat index alone.
# This fires when all three risk factors peak together.
# ─────────────────────────────────────────────────────────────────────────────

def insight_heat_stroke(hours: list) -> Optional[str]:
    """
    Heat stroke risk requires all three conditions simultaneously:
    - Heat index ≥ 41°C (danger zone)
    - UV index ≥ 8 (very high — direct sun amplifies heat stroke risk)
    - Daytime only (is_day == 1)
    Separate from heat_stress which fires on heat index alone.
    Returns a specific time window when risk is highest.
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

    risk_hours = []
    for h in valid:
        hi = heat_index(h["temperature_2m"], h["relative_humidity_2m"])
        uv = h.get("uv_index", 0)
        if hi >= 41 and uv >= 8:
            risk_hours.append((h, hi, uv))

    if not risk_hours:
        return None

    # Find peak risk hour
    peak = max(risk_hours, key=lambda x: x[1] + x[2])
    peak_h, peak_hi, peak_uv = peak

    # Group consecutive risk hours for time window
    risk_only = [x[0] for x in risk_hours]
    groups = group_consecutive_hours(risk_only, lambda h: True)

    if groups:
        start, end, _ = groups[0]
        window_str = fmt_time_range(start, end)
        return (f"🚨 Heat stroke risk window {window_str} — heat index {peak_hi}°C with UV {round(peak_uv)}. "
                f"Do not exercise outdoors. Stay in shade or indoors and drink water every 15 minutes.")

    return (f"🚨 Heat stroke risk around {fmt_time(peak_h['timestamp'])} — "
            f"heat index {peak_hi}°C with UV {round(peak_uv)}. Stay indoors during this window.")

def insight_heat_stress(hours: list) -> Optional[str]:
    valid = [h for h in hours
             if h.get("temperature_2m") is not None and h.get("relative_humidity_2m") is not None]
    if not valid:
        return None

    # Calculate real heat index for each hour
    hi_rows = [(h, heat_index(h["temperature_2m"], h["relative_humidity_2m"])) for h in valid]
    hi_rows = [(h, hi) for h, hi in hi_rows if hi >= 27]  # Only meaningful above 27°C

    if not hi_rows:
        return None

    peak_row, peak_hi = max(hi_rows, key=lambda x: x[1])

    if peak_hi < 32:
        return f"💧 Mild heat stress around {fmt_time(peak_row['timestamp'])} (heat index {peak_hi}°C). Drink water regularly."

    elif peak_hi < 38:
        stress_groups = group_consecutive_hours(
            valid, lambda h: heat_index(h["temperature_2m"], h.get("relative_humidity_2m", 0)) >= 32
        )
        if stress_groups:
            start, end, _ = stress_groups[0]
            return (f"🌡️ Moderate heat stress between {fmt_time_range(start, end)} "
                    f"(heat index up to {peak_hi}°C). Avoid heavy exertion, stay hydrated.")
        return f"🌡️ Moderate heat stress (heat index {peak_hi}°C). Stay hydrated and rest in shade."

    elif peak_hi < 45:
        stress_groups = group_consecutive_hours(
            valid, lambda h: heat_index(h["temperature_2m"], h.get("relative_humidity_2m", 0)) >= 38
        )
        if stress_groups:
            start, end, _ = stress_groups[0]
            return (f"⚠️ High heat stress between {fmt_time_range(start, end)} "
                    f"(heat index {peak_hi}°C). Rest in shade, drink water every 20 minutes.")
        return f"⚠️ High heat stress (heat index {peak_hi}°C). Avoid outdoor activity during peak hours."

    else:
        stress_groups = group_consecutive_hours(
            valid, lambda h: heat_index(h["temperature_2m"], h.get("relative_humidity_2m", 0)) >= 45
        )
        if stress_groups:
            start, end, _ = stress_groups[0]
            return (f"🚨 Dangerous heat stress between {fmt_time_range(start, end)} "
                    f"(heat index {peak_hi}°C). Do not go outside during this window.")
        return f"🚨 Dangerous heat stress (heat index {peak_hi}°C). Stay indoors."


# ─────────────────────────────────────────────────────────────────────────────
# INSIGHT 15 — FROST RISK (freezing_level_height)
# ─────────────────────────────────────────────────────────────────────────────

def insight_frost(hours: list) -> Optional[str]:
    valid = [h for h in hours if h.get("freezing_level_height") is not None]
    if not valid:
        return None

    min_row = min(valid, key=lambda h: h["freezing_level_height"])
    min_height = round(min_row["freezing_level_height"])

    if min_height > 3000:
        return None  # Suppressed — no frost risk at ground level

    elif min_height > 2000:
        return f"❄️ Freezing level dropping to {min_height}m around {fmt_time(min_row['timestamp'])}. Cold night ahead — protect plants and outdoor pipes."

    elif min_height > 1000:
        return f"🥶 Frost risk tonight — freezing level at {min_height}m. Cover plants and check pipes before sleeping."

    elif min_height > 500:
        frost_groups = group_consecutive_hours(valid, lambda h: h.get("freezing_level_height", 9999) <= 1000)
        if frost_groups:
            start, end, _ = frost_groups[0]
            return (f"⚠️ Significant frost risk between {fmt_time_range(start, end)} "
                    f"(freezing level {min_height}m). Secure vehicles and water lines.")
        return f"⚠️ Significant frost risk (freezing level {min_height}m). Secure vehicles and water lines."

    else:
        return (f"🚨 Severe frost conditions (freezing level {min_height}m) around {fmt_time(min_row['timestamp'])}. "
                f"Pipes and outdoor plants at serious risk.")


# ─────────────────────────────────────────────────────────────────────────────
# INSIGHT 16 — VISIBILITY
# ─────────────────────────────────────────────────────────────────────────────

def insight_visibility(hours: list) -> Optional[str]:
    valid = [h for h in hours if h.get("visibility") is not None]
    if not valid:
        return None

    min_row = min(valid, key=lambda h: h["visibility"])
    min_vis_m = min_row["visibility"]
    min_vis_km = round(min_vis_m / 1000, 1)

    if min_vis_m > 10000:
        return None  # Suppressed — excellent visibility

    elif min_vis_m > 5000:
        return f"👁️ Good visibility throughout the day ({min_vis_km} km at lowest)."

    elif min_vis_m > 2000:
        return (f"🌫️ Reduced visibility ({min_vis_km} km) around {fmt_time(min_row['timestamp'])}. "
                f"Drive carefully.")

    elif min_vis_m > 1000:
        vis_groups = group_consecutive_hours(valid, lambda h: h.get("visibility", 99999) <= 2000)
        if vis_groups:
            start, end, _ = vis_groups[0]
            return (f"⚠️ Poor visibility ({min_vis_km} km) between {fmt_time_range(start, end)}. "
                    f"Use headlights, reduce speed.")
        return f"⚠️ Poor visibility ({min_vis_km} km) around {fmt_time(min_row['timestamp'])}."

    else:
        return (f"🚨 Very poor visibility ({min_vis_km} km) between "
                f"{fmt_time(min_row['timestamp'])}. Avoid driving if possible.")


# ─────────────────────────────────────────────────────────────────────────────
# INSIGHT 17 — SNOWFALL
# ─────────────────────────────────────────────────────────────────────────────

def insight_snow(hours: list) -> Optional[str]:
    valid = [h for h in hours if h.get("snowfall") is not None]
    total_snow = sum(h["snowfall"] for h in valid)

    if total_snow <= 0:
        return None  # Suppressed — no snow

    snow_groups = group_consecutive_hours(valid, lambda h: h.get("snowfall", 0) > 0)
    if not snow_groups:
        return None

    biggest = max(snow_groups, key=lambda g: sum(r.get("snowfall", 0) for r in g[2]))
    start, end, group_rows = biggest
    total_mm = round(sum(r.get("snowfall", 0) for r in group_rows), 1)

    if total_mm < 2:
        return f"🌨️ Light snowfall possible around {fmt_time(start)} (~{total_mm}mm). Roads may get slippery."
    elif total_mm < 5:
        return (f"❄️ Moderate snow expected between {fmt_time_range(start, end)} "
                f"(~{total_mm}mm). Allow extra travel time.")
    elif total_mm < 10:
        return (f"⚠️ Heavy snow {fmt_time_range(start, end)} (~{total_mm}mm). "
                f"Avoid travel if possible.")
    else:
        return (f"🚨 Severe snowfall {fmt_time_range(start, end)} "
                f"(~{total_mm}mm total). Stay indoors.")


# ─────────────────────────────────────────────────────────────────────────────
# INSIGHT 18 — PRESSURE TREND
# ─────────────────────────────────────────────────────────────────────────────

def insight_pressure(hours: list) -> Optional[str]:
    valid = [h for h in hours if h.get("pressure_msl") is not None]
    if len(valid) < 3:
        return None

    first_pressure = valid[0]["pressure_msl"]
    last_pressure = valid[-1]["pressure_msl"]
    change = round(last_pressure - first_pressure, 1)  # negative = falling
    hours_span = len(valid)

    # Rate in hPa per hour
    rate = round(change / hours_span, 2)

    if abs(change) < 2:
        return None  # Stable — suppressed

    elif change < -5:
        # Find the time when it really starts dropping
        return (f"⚠️ Pressure dropping sharply ({abs(change)} hPa over remaining day). "
                f"Expect worsening weather conditions by {fmt_time(valid[-1]['timestamp'])}.")

    elif change < -2:
        return f"📉 Pressure slowly falling ({abs(change)} hPa). Weather may deteriorate later today."

    elif change > 5:
        return (f"✅ Pressure rising strongly ({change} hPa). "
                f"Weather clearing up — conditions should improve by {fmt_time(valid[-1]['timestamp'])}.")

    else:
        return f"📈 Pressure gradually rising ({change} hPa). Conditions likely improving through the day."


# ─────────────────────────────────────────────────────────────────────────────
# INSIGHT 19 — BEST OUTDOOR WINDOW (composite cross-table)
# ─────────────────────────────────────────────────────────────────────────────

def insight_best_outdoor_window(hours: list, aqi_hours: list) -> Optional[str]:
    """
    Finds the best consecutive 2-hour block where:
    - UV index < 6
    - precipitation_probability < 30
    - apparent_temperature between 18 and 34°C
    - us_aqi < 100
    - is_day == 1
    Scores each 2-hour window and returns the best one.
    """
    if not hours or not aqi_hours:
        return None

    # Build an aqi lookup by timestamp hour for fast joining
    aqi_by_hour = {}
    for row in aqi_hours:
        ts = row["timestamp"]
        key = (ts.date(), ts.hour)
        aqi_by_hour[key] = row

    # Only consider allowed outdoor windows (morning 5-9am, evening 5-7pm)
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
        score = 4  # Start perfect, deduct for each failed condition

        for row in window:
            uv = row.get("uv_index", 0)
            prob = row.get("precipitation_probability", 0)
            app_temp = row.get("apparent_temperature", 25)
            ts = row["timestamp"]
            aqi_row = aqi_by_hour.get((ts.date(), ts.hour), {})
            aqi_val = aqi_row.get("us_aqi", 50)

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
            best_end = window[-1]["timestamp"]
            best_issues = list(set(issues))

    if best_score == 4:
        return (f"🌟 Best time to go outside today: {fmt_time(best_start)}–{fmt_time(best_end)}. "
                f"Low UV, comfortable temperature, no rain, clean air.")
    elif best_score >= 2:
        issue_str = " and ".join(best_issues) if best_issues else "some conditions"
        return (f"🌤️ Best outdoor window: {fmt_time(best_start)}–{fmt_time(best_end)}, "
                f"though {issue_str} may be a factor.")
    else:
        return "⚠️ No ideal outdoor window today — conditions are challenging throughout the rest of the day."


# ─────────────────────────────────────────────────────────────────────────────
# INSIGHT 20 — AQI TREND
# ─────────────────────────────────────────────────────────────────────────────

def insight_aqi_trend(aqi_hours: list) -> Optional[str]:
    """
    Detects whether air quality is improving or worsening across the day.
    Uses us_aqi internally for trend direction only — never displays the
    raw Open-Meteo us_aqi number to the user in the output string.
    """
    valid = [h for h in aqi_hours if h.get("us_aqi") is not None]
    if len(valid) < 6:
        return None

    first_mean = sum(h["us_aqi"] for h in valid[:3]) / 3
    last_mean  = sum(h["us_aqi"] for h in valid[-3:]) / 3
    change     = round(last_mean - first_mean, 1)

    if abs(change) < 20:
        return None  # Stable — suppressed

    end_time = fmt_time(valid[-1]["timestamp"])

    if change < -20:
        return (f"📉 Air quality improving throughout the day. "
                f"Conditions should be better by {end_time} — good window for outdoor activity later.")
    else:
        return (f"📈 Air quality worsening throughout the day. "
                f"Plan outdoor activity now if needed — conditions deteriorate by {end_time}.")


# ─────────────────────────────────────────────────────────────────────────────
# INSIGHT 21 — DAYLIGHT REMAINING
# ─────────────────────────────────────────────────────────────────────────────

def insight_daylight(hours: list, daily: dict) -> Optional[str]:
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
        return None  # Sun already set — suppressed

    remaining_minutes = (sunset - now).total_seconds() / 60
    remaining_hours = remaining_minutes / 60

    if remaining_hours > 8:
        return None  # Plenty of day left — suppressed

    elif remaining_hours > 4:
        return f"🌅 {round(remaining_hours, 1)} hours of daylight remaining. Sunset at {fmt_time(sunset)}."

    elif remaining_hours > 2:
        return f"🌇 Only {round(remaining_hours, 1)} hours of daylight left. Sunset at {fmt_time(sunset)} — plan accordingly."

    else:
        return f"🌙 Less than 2 hours of daylight remaining. Sunset at {fmt_time(sunset)}."


# ─────────────────────────────────────────────────────────────────────────────
# MASTER ENGINE — runs all 21 insights, returns only triggered ones
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# PRIORITY TIERS
# Every insight is assigned a tier so the most critical ones always surface first.
#   1 = 🔴 Dangerous  — heat stress, storm, frost, heavy rain, snow
#   2 = 🟠 High       — rain (likely), UV high, wind, AQI bad, PM2.5
#   3 = 🟡 Medium     — wind chill, pressure drop, visibility, AQI trend
#   4 = 🟢 Low        — sunshine, pollen, daylight, ozone, dust, outdoor window
# Lower number = shown first. Within the same tier, order is preserved.
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# INSIGHT NEW A — CLOTHING RECOMMENDATION
# Combines feels-like, wind chill and rain into one "what to wear" insight.
# ─────────────────────────────────────────────────────────────────────────────

def insight_clothing(hours: list) -> Optional[str]:
    """
    Recommends clothing based on the combined effect of feels-like temperature,
    wind chill and rain probability. Only one clothing insight fires per day.
    Priority: rain gear > cold layers > heat clothing > comfortable.
    """
    valid = [h for h in hours if h.get("apparent_temperature") is not None]
    if not valid:
        return None

    peak_feels  = max(h["apparent_temperature"] for h in valid)
    min_feels   = min(h["apparent_temperature"] for h in valid)
    max_rain    = max((h.get("precipitation_probability", 0) for h in valid), default=0)
    max_gusts   = max((h.get("wind_gusts_10m", 0) or 0 for h in valid), default=0)

    # Rain gear takes priority
    if max_rain >= 60:
        if min_feels < 18:
            return f"🧥🌧️ Wear a waterproof jacket and layer underneath — wet and cool (feels like {round(min_feels)}°C at coldest)."
        return f"☂️ Carry an umbrella or wear a rain jacket — high chance of rain today."

    # Cold conditions
    if min_feels < 10:
        return f"🧥 Bundle up — feels like {round(min_feels)}°C at its coldest. Heavy jacket, gloves if needed."
    if min_feels < 18:
        if max_gusts >= 40:
            return f"🧥💨 Light jacket plus a windproof layer — cool and gusty (feels like {round(min_feels)}°C, gusts {round(max_gusts)} km/h)."
        return f"🧥 A light jacket is recommended — feels like {round(min_feels)}°C at its coolest."

    # Hot conditions
    if peak_feels >= 42:
        return f"👕 Wear light, loose, light-coloured clothing — dangerous heat (feels like {round(peak_feels)}°C peak). Cover skin from direct sun."
    if peak_feels >= 35:
        return f"👕 Light breathable clothing recommended — warm day peaking at {round(peak_feels)}°C feels-like."

    # Comfortable range — light rain possible
    if max_rain >= 30:
        return f"👔 Comfortable day ({round(min_feels)}–{round(peak_feels)}°C feels-like) but keep a light rain jacket handy."

    return f"👕 Comfortable clothing day — feels like {round(min_feels)}–{round(peak_feels)}°C. No special gear needed."


# ─────────────────────────────────────────────────────────────────────────────
# INSIGHT NEW B — COMMUTE WINDOW ALERT
# Flags if worst weather falls during typical commute hours 8–9 AM or 6–7 PM.
# ─────────────────────────────────────────────────────────────────────────────

def insight_commute(hours: list, aqi_hours: list) -> Optional[str]:
    """
    Checks if heavy rain, dangerous heat, poor AQI or severe wind falls
    during commute windows: morning (7–9 AM) or evening (5–7 PM).
    Returns a specific warning with timing so user can plan accordingly.
    """
    def _commute_hour(ts) -> bool:
        if ts is None: return False
        h = ts.hour if hasattr(ts, "hour") else int(str(ts)[11:13])
        return (7 <= h < 9) or (17 <= h < 19)

    commute_h = [h for h in hours if _commute_hour(h.get("timestamp"))]
    if not commute_h:
        return None

    issues = []

    # Heavy rain during commute
    max_rain = max((h.get("precipitation_probability", 0) for h in commute_h), default=0)
    if max_rain >= 70:
        peak = max(commute_h, key=lambda h: h.get("precipitation_probability", 0))
        slot = "morning commute" if peak["timestamp"].hour < 12 else "evening commute"
        issues.append(f"heavy rain during {slot} ({round(max_rain)}%)")

    # Dangerous heat during commute
    max_feels = max((h.get("apparent_temperature", 0) or 0 for h in commute_h), default=0)
    if max_feels >= 40:
        issues.append(f"dangerous heat during commute (feels like {round(max_feels)}°C)")

    # Poor AQI during commute
    aqi_by_hour = {}
    for row in aqi_hours:
        ts = row.get("timestamp")
        if ts: aqi_by_hour[(ts.date(), ts.hour)] = row
    commute_aqi = [aqi_by_hour.get((h["timestamp"].date(), h["timestamp"].hour), {})
                   for h in commute_h]
    max_aqi = max((r.get("us_aqi", 0) or 0 for r in commute_aqi), default=0)
    if max_aqi >= 150:
        issues.append(f"unhealthy air during commute (AQI {round(max_aqi)})")

    # Strong gusts during commute
    max_gusts = max((h.get("wind_gusts_10m", 0) or 0 for h in commute_h), default=0)
    if max_gusts >= 55:
        issues.append(f"strong wind gusts during commute ({round(max_gusts)} km/h)")

    if not issues:
        return None

    if len(issues) == 1:
        return f"🚗 Commute alert — {issues[0].capitalize()}. Plan accordingly."
    return f"🚗 Commute alert — {' and '.join(issues).capitalize()}. Consider adjusting travel time."


# ─────────────────────────────────────────────────────────────────────────────
# INSIGHT NEW C — SLEEP QUALITY FORECAST
# Night-time temperature, humidity and AQI combined into overnight comfort score.
# ─────────────────────────────────────────────────────────────────────────────

def insight_sleep(hours: list, aqi_hours: list) -> Optional[str]:
    """
    Evaluates overnight comfort for sleep based on temperature (18–22°C ideal),
    humidity (<60% ideal) and AQI (<100 ideal).
    Only looks at hours between 10 PM and 6 AM.
    """
    night_h = [h for h in hours
               if h.get("timestamp") and
               (h["timestamp"].hour >= 22 or h["timestamp"].hour < 6)]
    if not night_h:
        return None

    temps    = [h.get("apparent_temperature") for h in night_h if h.get("apparent_temperature") is not None]
    humidity = [h.get("relative_humidity_2m") for h in night_h if h.get("relative_humidity_2m") is not None]

    if not temps:
        return None

    avg_temp = sum(temps) / len(temps)
    avg_hum  = sum(humidity) / len(humidity) if humidity else None

    aqi_by_hour = {}
    for row in aqi_hours:
        ts = row.get("timestamp")
        if ts: aqi_by_hour[(ts.date(), ts.hour)] = row
    night_aqi = [aqi_by_hour.get((h["timestamp"].date(), h["timestamp"].hour), {})
                 for h in night_h]
    avg_aqi = sum(r.get("us_aqi", 0) or 0 for r in night_aqi) / len(night_aqi) if night_aqi else 0

    issues = []
    if avg_temp > 28:
        issues.append(f"hot ({round(avg_temp)}°C overnight — fan or AC recommended)")
    elif avg_temp > 24:
        issues.append(f"warm overnight ({round(avg_temp)}°C — keep windows open if possible)")
    elif avg_temp < 14:
        issues.append(f"cold overnight ({round(avg_temp)}°C — extra blanket recommended)")

    if avg_hum and avg_hum > 80:
        issues.append(f"high humidity ({round(avg_hum)}% — may feel sticky and uncomfortable)")

    if avg_aqi > 150:
        issues.append("poor overnight air quality — keep windows closed")

    if not issues:
        if 18 <= avg_temp <= 22:
            return f"😴 Great night for sleep — comfortable {round(avg_temp)}°C overnight with good air quality."
        return None

    return f"😴 Sleep forecast — {', '.join(issues).capitalize()}."


# ─────────────────────────────────────────────────────────────────────────────
# INSIGHT NEW D — COMBINED POLLEN LOAD
# Merges grass/alder/birch pollen into one score instead of three bullets.
# ─────────────────────────────────────────────────────────────────────────────

def insight_pollen_combined(aqi_hours: list) -> Optional[str]:
    """
    Combines grass, alder and birch pollen into a single overall pollen load
    insight. Reports which type is dominant and overall severity.
    Replaces three separate pollen insights with one actionable summary.
    """
    keys = [("grass_pollen", "Grass"), ("alder_pollen", "Alder"), ("birch_pollen", "Birch")]
    peaks = {}
    for key, name in keys:
        vals = [h.get(key) for h in aqi_hours if h.get(key) is not None]
        if vals:
            peaks[name] = round(max(vals), 1)

    if not peaks:
        return None

    total = sum(peaks.values())
    dominant = max(peaks, key=peaks.get)
    dominant_val = peaks[dominant]

    if dominant_val < 10:
        return None  # All low — suppressed

    active = [f"{n} ({v})" for n, v in peaks.items() if v >= 10]
    active_str = ", ".join(active)

    if dominant_val >= 120:
        return (f"🤧 Very high pollen load today — {active_str}. "
                f"Allergy sufferers stay indoors and take antihistamines before going out.")
    if dominant_val >= 60:
        return (f"🌸 High pollen today — {active_str}. "
                f"Take antihistamines before going outside. Sunglasses help with eye irritation.")
    if dominant_val >= 30:
        return (f"🌸 Moderate pollen — {active_str}. "
                f"Allergy sufferers may want to take antihistamines.")
    return (f"🌿 Low pollen levels — {active_str}. Generally not a concern for most people.")


# ─────────────────────────────────────────────────────────────────────────────
# INSIGHT NEW E — HYDRATION REMINDER
# Fires when heat index > 35°C or humidity > 80% to remind users to drink water.
# ─────────────────────────────────────────────────────────────────────────────

def insight_hydration(hours: list) -> Optional[str]:
    """
    Calculates estimated extra water intake needed based on heat index and humidity.
    WHO recommends 2L/day baseline — adds 500ml per heat tier above comfort zone.
    Only fires when conditions meaningfully increase dehydration risk.
    """
    valid = [h for h in hours
             if h.get("temperature_2m") is not None and h.get("relative_humidity_2m") is not None]
    if not valid:
        return None

    hi_rows = [(h, heat_index(h["temperature_2m"], h["relative_humidity_2m"])) for h in valid]
    peak_row, peak_hi = max(hi_rows, key=lambda x: x[1])
    avg_hum = sum(h.get("relative_humidity_2m", 0) for h in valid) / len(valid)

    # Temperature floor — hydration only meaningful when warm enough to sweat
    max_temp = max(h.get("temperature_2m", 0) for h in valid)
    if max_temp < 25 and peak_hi < 32:
        return None  # Too cool for meaningful dehydration risk

    if peak_hi < 32 and avg_hum < 75:
        return None  # Normal conditions — no extra hydration needed

    if peak_hi >= 45:
        return (f"💧 Drink at least 3.5L of water today — dangerous heat stress "
                f"(heat index {peak_hi}°C). Sip water every 15–20 minutes when outside.")
    if peak_hi >= 38:
        return (f"💧 Drink at least 3L of water today — high heat stress "
                f"(heat index {peak_hi}°C). Avoid caffeine and alcohol which increase dehydration.")
    if peak_hi >= 32:
        return (f"💧 Stay well hydrated — heat index reaching {peak_hi}°C. "
                f"Aim for 2.5L+ of water and avoid prolonged sun exposure.")
    if avg_hum >= 80:
        return (f"💧 High humidity ({round(avg_hum)}%) increases sweat loss even in shade. "
                f"Drink water regularly throughout the day.")
    return None


def generate_insights_split(hours: list, aqi_hours: list, daily: dict, current: dict) -> tuple[str, str]:
    """
    Same as generate_insights_from_data but returns (visible_text, hidden_text).
    visible_text: header + tier 1 & 2 insights (dangerous + rain) — always shown
    hidden_text:  tier 3–6 insights — shown only when user taps "Show more"
    If there are no hidden insights, hidden_text is an empty string.
    """
    checks = [
        (1, insight_heat_stress(hours)),
        (1, insight_frost(hours)),
        (1, insight_snow(hours)),
        (1, insight_temperature(hours)),
        (2, insight_rain(hours)),
        (1, insight_heat_stroke(hours)),
        (3, insight_aqi(aqi_hours, current)),
        (3, insight_mask(aqi_hours)),
        (3, insight_respiratory(aqi_hours)),
        (3, insight_pollen_combined(aqi_hours)),
        (3, insight_aqi_trend(aqi_hours)),
        (4, insight_uv(hours)),
        (4, insight_clothing(hours)),
        (4, insight_commute(hours, aqi_hours)),
        (5, insight_wind(hours)),
        (5, insight_wind_chill(hours)),
        (5, insight_hydration(hours)),
        (6, insight_sleep(hours, aqi_hours)),
        (6, insight_sunshine(hours)),
        (6, insight_cloud_trend(hours)),
        (6, insight_visibility(hours)),
        (6, insight_best_outdoor_window(hours, aqi_hours)),
        (6, insight_daylight(hours, daily)),
    ]

    triggered = [(tier, text) for tier, text in checks if text is not None]
    triggered.sort(key=lambda x: x[0])

    if not triggered:
        return "✅ All conditions are normal for the rest of today. No alerts or advisories.", ""

    # ── Conflicting insight suppression ───────────────────────────────────────
    # If tier-1 dangers are active, remove positive lifestyle recommendations
    # (outdoor window, best run time, laundry, golden hour) to avoid contradictions
    # like "Best time to exercise: 2PM" alongside "Dangerous heat all day".
    has_tier1 = any(tier == 1 for tier, _ in triggered)
    _positive_keywords = ("best time to", "best outdoor", "great day to dry",
                          "clear skies expected at sunset", "air quality is suitable",
                          "great conditions for", "comfortable clothing day",
                          "great night for sleep", "no mask needed",
                          "photography windows", "golden hour", "skies clearing",
                          "gradual clearing")
    if has_tier1:
        triggered = [
            (tier, text) for tier, text in triggered
            if not (tier >= 5 and any(k in text.lower() for k in _positive_keywords))
        ]

    # ── Heat stress + dangerous temp deduplication ────────────────────────────
    # If both "dangerous heat" (feels like) and "heat stress" fire at tier 1,
    # keep only the heat stress one (more medically precise, contains HI value).
    has_heat_stress   = any("heat stress" in t.lower() for _, t in triggered)
    has_danger_heat   = any("dangerous heat" in t.lower() for _, t in triggered)
    if has_heat_stress and has_danger_heat:
        triggered = [(tier, text) for tier, text in triggered
                     if "dangerous heat" not in text.lower() or "heat stress" not in text.lower()]

    # ── Clothing + temperature deduplication ──────────────────────────────────
    # insight_clothing is more specific and actionable — if it fires, suppress
    # insight_temperature which says the same thing at a higher level.
    has_clothing    = any(tier == 4 and ("jacket" in t.lower() or "wear" in t.lower() or "clothing" in t.lower()) for tier, t in triggered)
    has_temperature = any(tier == 1 and ("jacket" in t.lower() or "cool day" in t.lower() or "warm day" in t.lower() or "cold day" in t.lower()) for tier, t in triggered)
    if has_clothing and has_temperature:
        triggered = [(tier, text) for tier, text in triggered
                     if not (tier == 1 and ("jacket" in text.lower() or "cool day" in text.lower() or "warm day" in text.lower() or "cold day" in text.lower()))]

    # ── AQI + mask deduplication ───────────────────────────────────────────────
    # If both AQI insight and mask insight fire and both indicate bad air,
    # merge them into one combined bullet. AQI insight wins — mask detail appended.
    aqi_items  = [(tier, text) for tier, text in triggered if tier == 3 and ("air quality" in text.lower() or "aqi" in text.lower()) and "mask" not in text.lower()]
    mask_items = [(tier, text) for tier, text in triggered if "n95" in text.lower() or "n99" in text.lower() or "surgical mask" in text.lower()]
    if aqi_items and mask_items:
        # Remove both from triggered, add merged version
        aqi_tier, aqi_text  = aqi_items[0]
        _,         mask_text = mask_items[0]
        # Extract mask recommendation (everything after the dash)
        mask_action = mask_text.split("—")[1].strip() if "—" in mask_text else mask_text
        merged = f"{aqi_text.rstrip('.')} — {mask_action}"
        triggered = [(tier, text) for tier, text in triggered
                     if text not in (aqi_text, mask_items[0][1])]
        triggered.append((aqi_tier, merged))
        triggered.sort(key=lambda x: x[0])

    visible_items = [text for tier, text in triggered if tier <= 2]
    hidden_items  = [text for tier, text in triggered if tier > 2]

    # Severity-weighted header — shows alerts separately from tips
    alert_count = sum(1 for tier, _ in triggered if tier <= 2)
    tip_count   = sum(1 for tier, _ in triggered if tier > 2)
    if alert_count > 0 and tip_count > 0:
        header = f"💡 {alert_count} alert{'s' if alert_count > 1 else ''} · {tip_count} tip{'s' if tip_count > 1 else ''}\n\n"
    elif alert_count > 0:
        header = f"⚠️ {alert_count} alert{'s' if alert_count > 1 else ''} today\n\n"
    else:
        header = f"💡 {tip_count} tip{'s' if tip_count > 1 else ''} for today\n\n"

    if not visible_items:
        visible_items = [text for _, text in triggered[:2]]
        hidden_items  = [text for _, text in triggered[2:]]

    visible_text = header + "\n\n".join(visible_items)
    hidden_text  = "\n\n".join(hidden_items) if hidden_items else ""

    return visible_text, hidden_text


def generate_insights_from_data(hours: list, aqi_hours: list, daily: dict, current: dict) -> str:
    """
    Main entry point for the insights engine.
    Runs all 21 insight functions, sorts by priority tier, returns formatted string.
    Kept for backward-compatibility — internally delegates to generate_insights_split.
    """
    visible, hidden = generate_insights_split(hours, aqi_hours, daily, current)
    if hidden:
        return visible + "\n\n" + hidden
    return visible

# ─────────────────────────────────────────────────────────────────────────────
# NEW FUNCTION A — BEST RUN/WALK TIME
# Returns the single best hour to exercise outside today.
# Scores each daytime hour on UV, rain probability, temperature, and AQI.
# ─────────────────────────────────────────────────────────────────────────────

def get_best_run_time(hours: list, aqi_hours: list) -> Optional[str]:
    """
    Finds the single best hour today to exercise outside.
    Scoring: low UV + low rain chance + comfortable apparent temp + good AQI.
    Threshold raised to ≥5/8 to avoid recommending mediocre conditions.
    Only daytime hours considered for both weather and AQI (fair comparison window).
    Returns None if no good window exists — caller should then show exercise_air_score.
    """
    if not hours:
        return None

    # Build AQI lookup keyed by (date, hour) — daytime only for fair comparison
    aqi_by_hour = {}
    for row in aqi_hours:
        ts = row["timestamp"]
        aqi_by_hour[(ts.date(), ts.hour)] = row

    # Only consider allowed outdoor windows (morning 5-9am, evening 5-7pm)
    daytime = _outdoor_hours(hours)
    if not daytime:
        return None

    best_score, best_row, best_aqi = -1, None, 200

    for h in daytime:
        uv      = h.get("uv_index", 10)
        prob    = h.get("precipitation_probability", 100)
        temp    = h.get("apparent_temperature", 0)
        ts      = h["timestamp"]
        aqi_row = aqi_by_hour.get((ts.date(), ts.hour), {})
        aqi_val = aqi_row.get("us_aqi", 200)

        score = 0
        if uv < 4:              score += 2
        elif uv < 6:            score += 1
        if prob < 15:           score += 2
        elif prob < 30:         score += 1
        if 18 <= temp <= 28:    score += 2
        elif 15 <= temp <= 33:  score += 1
        if aqi_val < 50:        score += 2
        elif aqi_val < 100:     score += 1

        if score > best_score:
            best_score = score
            best_row   = h
            best_aqi   = aqi_val

    # Require score ≥ 5/8 — below this conditions are too mixed to recommend
    if best_row is None or best_score < 5:
        return None

    # Rain mutual exclusion — check if rain is likely within 2 hours of best window
    best_ts = best_row["timestamp"]
    nearby_rain = any(
        h.get("precipitation_probability", 0) >= 50
        for h in hours
        if h.get("timestamp") and abs((h["timestamp"] - best_ts).total_seconds()) <= 7200
    )

    temp_str = f"{round(best_row.get('apparent_temperature', 0))}°C"
    aqi_note = " with clean air" if best_aqi < 100 else ""
    if nearby_rain:
        return (
            f"🏃 Best time to exercise outside: {fmt_time(best_ts)} — "
            f"comfortable at {temp_str}{aqi_note}, though rain is possible nearby. Check conditions first."
        )
    return (
        f"🏃 Best time to exercise outside today: {fmt_time(best_ts)}. "
        f"Comfortable at {temp_str}{aqi_note} and low UV."
    )


def get_exercise_air_score(hours: list, aqi_hours: list) -> Optional[str]:
    """
    Returns a simple exercise recommendation based on daytime AQI + PM2.5 + ozone.
    Only fires when get_best_run_time returns None (i.e. no good window exists),
    so the two functions never contradict each other in the same message.
    Uses daytime AQI only — same window as get_best_run_time for consistency.
    """
    # Only use outdoor-window AQI hours — same window as get_best_run_time
    daytime_aqi = [h for h in _outdoor_hours(aqi_hours) if h.get("us_aqi") is not None]
    if not daytime_aqi:
        daytime_aqi = [h for h in aqi_hours if h.get("us_aqi") is not None]  # fallback
    if not daytime_aqi:
        return None

    # Check whether get_best_run_time would fire — if so, suppress this function
    # to avoid contradicting it. We re-run the scorer with a low threshold to check.
    if hours:
        aqi_by_hour = {}
        for row in aqi_hours:
            ts = row["timestamp"]
            aqi_by_hour[(ts.date(), ts.hour)] = row
        daytime = _outdoor_hours(hours)
        for h in daytime:
            uv   = h.get("uv_index", 10)
            prob = h.get("precipitation_probability", 100)
            temp = h.get("apparent_temperature", 0)
            ts   = h["timestamp"]
            aqi_val = aqi_by_hour.get((ts.date(), ts.hour), {}).get("us_aqi", 200)
            score = 0
            if uv < 4:             score += 2
            elif uv < 6:           score += 1
            if prob < 15:          score += 2
            elif prob < 30:        score += 1
            if 18 <= temp <= 28:   score += 2
            elif 15 <= temp <= 33: score += 1
            if aqi_val < 50:       score += 2
            elif aqi_val < 100:    score += 1
            if score >= 5:
                return None  # get_best_run_time will fire — suppress this

    aqi_vals   = [h["us_aqi"] for h in daytime_aqi]
    pm25_vals  = [h.get("pm2_5") for h in daytime_aqi if h.get("pm2_5") is not None]
    ozone_vals = [h.get("ozone") for h in daytime_aqi if h.get("ozone") is not None]

    avg_aqi   = sum(aqi_vals) / len(aqi_vals)
    avg_pm25  = sum(pm25_vals) / len(pm25_vals) if pm25_vals else None
    avg_ozone = sum(ozone_vals) / len(ozone_vals) if ozone_vals else None

    pm25_bad  = avg_pm25 is not None and avg_pm25 > 55
    ozone_bad = avg_ozone is not None and avg_ozone > 160

    if avg_aqi < 100 and not pm25_bad and not ozone_bad:
        return "🧘 Air quality is suitable for outdoor exercise today."
    if avg_aqi > 150 or pm25_bad:
        return "🚫 Air quality not safe for outdoor exercise today. Work out indoors."
    return "⚠️ Exercise outdoors with caution today — air quality is moderate."


# ─────────────────────────────────────────────────────────────────────────────
# NEW FUNCTION B — LAUNDRY SCORE
# Checks whether today is a good day to dry clothes outdoors.
# ─────────────────────────────────────────────────────────────────────────────

def get_laundry_score(hours: list) -> Optional[str]:
    """
    Returns a laundry recommendation string if conditions are favourable
    (low rain probability, low cloud cover, moderate humidity, light breeze).
    Returns None if conditions are poor or data is missing.
    """
    daytime = [h for h in hours if h.get("is_day", 0) == 1]
    if not daytime:
        return None

    max_prob    = max((h.get("precipitation_probability", 100) for h in daytime), default=100)
    avg_cloud   = sum(h.get("cloud_cover", 100) for h in daytime) / len(daytime)
    avg_humidity = sum(h.get("relative_humidity_2m", 100) for h in daytime) / len(daytime)
    avg_wind    = sum(h.get("wind_speed_10m", 0) for h in daytime) / len(daytime)

    if max_prob < 15 and avg_cloud < 40 and avg_humidity < 65 and 5 <= avg_wind <= 25:
        return (
            f"🧺 Great day to dry clothes outside — sunny with low humidity "
            f"({round(avg_humidity)}%) and a light breeze ({round(avg_wind)} km/h)."
        )
    if max_prob >= 50:
        return "🚫 Not ideal for drying clothes outside today — significant rain risk."
    return None


# ─────────────────────────────────────────────────────────────────────────────
# NEW FUNCTION C — GOLDEN HOUR / SUNSET ALERT
# Fires when today's sunset window looks clear and beautiful.
# ─────────────────────────────────────────────────────────────────────────────

def get_golden_hour(daily: dict) -> Optional[str]:
    """
    Returns a golden hour + blue hour alert when conditions are suitable for photography.
    Golden hour: ~30 minutes before sunset (warm directional light).
    Blue hour:   ~20–40 minutes after sunset (cool twilight, even illumination).

    Clear sky WMO codes: 0 = clear sky, 1 = mainly clear, 2 = partly cloudy.
    Partly cloudy (2) is actually ideal for golden hour — some cloud diffuses light.
    WMO codes 3+ (overcast, rain, fog) suppress the alert.

    Only fires if:
    - Sunset hasn't passed yet
    - WMO code indicates clear/partly cloudy conditions
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
    if sunset <= now:
        return None  # Sun already set

    # WMO codes 0, 1, 2 = clear / mainly clear / partly cloudy — suitable for golden hour
    wmo = daily.get("weather_code_max", 99)
    if wmo not in (0, 1, 2):
        return None  # Overcast, rain or fog — no worthwhile golden hour

    # Calculate golden hour start (~30 min before sunset) and blue hour (~25 min after)
    golden_start = sunset - timedelta(minutes=30)
    blue_start   = sunset + timedelta(minutes=5)
    blue_end     = sunset + timedelta(minutes=35)

    golden_str = fmt_time(golden_start)
    sunset_str = fmt_time(sunset)
    blue_str   = f"{fmt_time(blue_start)}–{fmt_time(blue_end)}"

    if wmo == 2:
        sky_note = "Partly cloudy — clouds may enhance the colours."
    else:
        sky_note = "Clear skies — clean light with warm tones."

    return (
        f"📷 Photography windows today: Golden hour {golden_str}–{sunset_str} · "
        f"Blue hour {blue_str}. {sky_note}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# NEW FUNCTION D — TOMORROW SUMMARY
# Summarises tomorrow's forecast from the hourly rows for the next 24–48h window.
# insights.py fetches the full 48h window; this function filters to tomorrow only.
# ─────────────────────────────────────────────────────────────────────────────

def get_tomorrow_summary(tomorrow_hours: list, tomorrow_aqi: list) -> str:
    """
    Generates a concise tomorrow-forecast string from tomorrow's hourly data.
    `tomorrow_hours` and `tomorrow_aqi` are pre-filtered to DATE = tomorrow
    by insights.py before being passed here.
    Returns a formatted string (never None — always falls back gracefully).
    """
    if not tomorrow_hours:
        return "📅 No forecast data available for tomorrow yet. Check back later."

    # Temperature range
    temps = [h.get("apparent_temperature") for h in tomorrow_hours if h.get("apparent_temperature") is not None]
    temp_high = round(max(temps), 1) if temps else None
    temp_low  = round(min(temps), 1) if temps else None

    # Rain
    max_rain_prob = max((h.get("precipitation_probability", 0) for h in tomorrow_hours), default=0)
    total_rain    = round(sum(h.get("rain", 0) for h in tomorrow_hours), 1)

    # UV
    uv_vals  = [h.get("uv_index", 0) for h in tomorrow_hours if h.get("is_day", 0) == 1]
    peak_uv  = round(max(uv_vals), 1) if uv_vals else 0

    # AQI — tomorrow has no weather.com reading (weather.com is current-only).
    # Use Open-Meteo us_aqi internally for threshold logic but display only the
    # EPA category word, never the raw number. This avoids showing inflated
    # Open-Meteo forecast values (which can be 700+ for future hours) to the user.
    aqi_vals  = [h.get("us_aqi") for h in tomorrow_aqi if h.get("us_aqi") is not None]
    avg_aqi   = round(sum(aqi_vals) / len(aqi_vals)) if aqi_vals else None

    # Derive EPA category from average us_aqi — used as a word, not a number
    def _epa_cat(val):
        if val is None: return None
        if val <= 50:   return "Good"
        if val <= 100:  return "Moderate"
        if val <= 150:  return "Unhealthy for Sensitive Groups"
        if val <= 200:  return "Unhealthy"
        if val <= 300:  return "Very Unhealthy"
        return "Hazardous"

    lines = []

    # Temp line
    if temp_high is not None and temp_low is not None:
        lines.append(f"🌡️ Feels like {temp_low}–{temp_high}°C")

    # Rain line
    if max_rain_prob >= 60:
        lines.append(f"🌧️ Rain likely (~{total_rain}mm, {round(max_rain_prob)}% peak chance)")
    elif max_rain_prob >= 30:
        lines.append(f"🌦️ Some rain possible ({round(max_rain_prob)}% chance)")
    else:
        lines.append("☀️ Mostly dry")

    # UV line
    if peak_uv >= 8:
        lines.append(f"🕶️ Very high UV ({peak_uv}) — sunscreen essential")
    elif peak_uv >= 5:
        lines.append(f"☀️ Moderate–high UV ({peak_uv}) — apply sunscreen")

    # AQI line — category word only, no raw number
    if avg_aqi is not None and avg_aqi > 100:
        cat = _epa_cat(avg_aqi)
        lines.append(f"😷 Air quality expected to be {cat} — limit outdoor activity")
    elif avg_aqi is not None and avg_aqi <= 50:
        lines.append("🌿 Clean air forecast tomorrow")

    header = "📅 Tomorrow's forecast:\n\n"
    return header + "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# NEW FUNCTION E — ANOMALY DETECTION
# Compares today's average temperature against a historical monthly baseline.
# The baseline (historical_avg_temp) is pre-computed by insights.py from
# past daily_weather rows for the same month.
# ─────────────────────────────────────────────────────────────────────────────



# ─────────────────────────────────────────────────────────────────────────────
# INSIGHT NEW — RAIN STREAK
# Detects 3+ consecutive rainy days from the daily_weather rows.
# daily_rows is the full 7-day list passed from insights.py.
# ─────────────────────────────────────────────────────────────────────────────

def insight_rain_streak(daily_rows: list) -> Optional[str]:
    """
    Checks if it has rained (precipitation_sum > 1mm) for 3+ consecutive days
    including today. Uses the daily_weather rows sorted by date ascending.
    Fires a ground saturation warning — useful for waterproof footwear advice,
    outdoor activity planning, and flood risk context.

    daily_rows: list of dicts from daily_weather, each with keys:
        date, precipitation_sum
    """
    if not daily_rows:
        return None

    # Sort ascending by date so we can check consecutive days
    from datetime import date as _date
    sorted_rows = sorted(daily_rows, key=lambda r: r.get("date") or _date.min)

    # Count consecutive rainy days ending on the most recent (today or yesterday)
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
            break  # streak broken

    if streak < 3:
        return None

    if streak >= 7:
        return (f"🌧️ {streak}-day rain streak — ground is heavily saturated. "
                f"Expect standing water and slippery surfaces. Waterproof footwear essential. "
                f"Avoid low-lying areas prone to flooding.")

    if streak >= 5:
        return (f"🌧️ {streak} consecutive rainy days — ground is saturated. "
                f"Puddles and muddy paths likely. Waterproof footwear strongly recommended.")

    return (f"🌧️ Rain for {streak} days in a row — ground is saturated, "
            f"puddles likely on roads and paths. Waterproof footwear recommended today.")

def detect_anomaly(hours: list, historical_avg_temp: float) -> Optional[str]:
    """
    Returns an anomaly insight if today is significantly hotter or colder
    than the historical average for this month in this area.

    `historical_avg_temp` is the mean of temperature_2m_max from the past
    daily_weather rows for the current month, passed in from insights.py.
    Returns None if the difference is within the normal ±4°C band.
    """
    if historical_avg_temp is None:
        return None

    temps = [h.get("temperature_2m") for h in hours if h.get("temperature_2m") is not None]
    if not temps:
        return None

    avg_today = sum(temps) / len(temps)
    diff      = round(avg_today - historical_avg_temp, 1)

    if abs(diff) < 4:
        return None  # Within normal range — suppressed

    if diff >= 8:
        return f"🌡️ Unusually hot today — about {diff}°C above normal for this time of year."
    if diff >= 4:
        return f"🌡️ Warmer than usual today ({diff}°C above average for this month)."
    if diff <= -8:
        return f"🥶 Unusually cold today — about {abs(diff)}°C below normal for this time of year."
    # diff <= -4
    return f"🧥 Cooler than usual today ({abs(diff)}°C below average for this month)."


# NOTE: get_exercise_air_score is defined above (around line 1117) with full
# mutual-exclusion logic against get_best_run_time. The second definition
# that previously appeared here has been removed — it was a duplicate without
# the mutual-exclusion guard and would override the correct implementation.