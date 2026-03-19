def run_scraper(lat: float, lon: float, url: str, user_id: int, area: str):
    import requests
    import psycopg2
    import os
    import urllib.parse as up
    from datetime import datetime
    from bs4 import BeautifulSoup
    from dotenv import load_dotenv

    load_dotenv()  # loads .env from project root (src/.env)

    DB_URL = os.getenv("DATABASE_URL")
    TIMEZONE = "Asia/Kolkata"

    r = up.urlparse(DB_URL)
    password = up.unquote(r.password)  # handles special chars like @ in password

    # ── WMO Weather Code → Human Readable Condition ────────────────────────
    # Open-Meteo returns a numeric WMO weather code (e.g. 61 = "Slight Rain").
    # This dictionary is used ONLY when weather.com scraping fails, so we can
    # still produce a meaningful condition string for the user from Open-Meteo.
    # Full WMO code reference: https://open-meteo.com/en/docs#weathervariables
    WMO_CODES = {
        0:  "Clear Sky",
        1:  "Mainly Clear",
        2:  "Partly Cloudy",
        3:  "Overcast",
        45: "Fog",
        48: "Depositing Rime Fog",
        51: "Light Drizzle",
        53: "Moderate Drizzle",
        55: "Dense Drizzle",
        56: "Light Freezing Drizzle",
        57: "Heavy Freezing Drizzle",
        61: "Slight Rain",
        63: "Moderate Rain",
        65: "Heavy Rain",
        66: "Light Freezing Rain",
        67: "Heavy Freezing Rain",
        71: "Slight Snowfall",
        73: "Moderate Snowfall",
        75: "Heavy Snowfall",
        77: "Snow Grains",
        80: "Slight Rain Showers",
        81: "Moderate Rain Showers",
        82: "Violent Rain Showers",
        85: "Slight Snow Showers",
        86: "Heavy Snow Showers",
        95: "Thunderstorm",
        96: "Thunderstorm with Slight Hail",
        99: "Thunderstorm with Heavy Hail",
    }

    # ── EPA AQI Category Derivation ────────────────────────────────────────
    # This is used ONLY as a fallback when weather.com AQI scraping fails.
    # We derive a human-readable category from Open-Meteo's us_aqi number
    # using official EPA breakpoints. Primary AQI always comes from weather.com.
    def get_aqi_category(us_aqi_val):
        if us_aqi_val is None:
            return None
        if us_aqi_val <= 50:  return "Good"
        if us_aqi_val <= 100: return "Moderate"
        if us_aqi_val <= 150: return "Unhealthy for Sensitive Groups"
        if us_aqi_val <= 200: return "Unhealthy"
        if us_aqi_val <= 300: return "Very Unhealthy"
        return "Hazardous"

    # ── Fetch Open-Meteo Weather ────────────────────────────────────────────
    # This always runs regardless of whether weather.com scraping succeeds.
    # Open-Meteo is the backbone of our analysis layer — current, hourly,
    # and daily forecasts all come from here and are always stored in DB.
    weather = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": lat,
            "longitude": lon,
            "timezone": TIMEZONE,
            "current": [
                "temperature_2m", "relative_humidity_2m", "apparent_temperature",
                "precipitation", "rain", "snowfall", "cloud_cover",
                "pressure_msl", "surface_pressure", "wind_speed_10m",
                "wind_direction_10m", "wind_gusts_10m",
                "visibility", "uv_index", "is_day", "weather_code"
            ],
            "hourly": [
                "temperature_2m", "relative_humidity_2m", "dew_point_2m",
                "apparent_temperature", "precipitation_probability",
                "precipitation", "rain", "snowfall", "cloud_cover",
                "pressure_msl", "surface_pressure", "wind_speed_10m",
                "wind_direction_10m", "wind_gusts_10m", "visibility",
                "uv_index", "weather_code", "sunshine_duration",
                "is_day", "freezing_level_height"
            ],
            "daily": [
                "temperature_2m_max", "temperature_2m_min",
                "apparent_temperature_max", "apparent_temperature_min",
                "sunrise", "sunset",
                "precipitation_sum", "rain_sum", "snowfall_sum",
                "precipitation_hours", "daylight_duration",
                "wind_speed_10m_max", "wind_gusts_10m_max",
                "wind_direction_10m_dominant",
                "uv_index_max", "shortwave_radiation_sum",
                "et0_fao_evapotranspiration", "weather_code"
            ]
        },
        timeout=30
    ).json()

    # ── Fetch Open-Meteo Air Quality ────────────────────────────────────────
    # Also always runs. Gives us pollutant-level data for analysis and alerts.
    # The us_aqi from here is used as fallback ONLY if weather.com AQI fails.
    aqi = requests.get(
        "https://air-quality-api.open-meteo.com/v1/air-quality",
        params={
            "latitude": lat,
            "longitude": lon,
            "timezone": TIMEZONE,
            "current": [
                "pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide",
                "ozone", "sulphur_dioxide", "us_aqi",
                "dust", "uv_index", "uv_index_clear_sky"
            ],
            "hourly": [
                "pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide",
                "ozone", "sulphur_dioxide", "us_aqi",
                "dust", "uv_index", "uv_index_clear_sky",
                "alder_pollen", "birch_pollen", "grass_pollen"
            ]
        },
        timeout=30
    ).json()

    # ── Attempt Weather.com Scrape ──────────────────────────────────────────
    # This is wrapped in a try/except so any failure — timeout, HTML change,
    # bad URL, missing element — is caught silently. The user never sees an
    # error. Instead we fall back to Open-Meteo data seamlessly.
    # scrape_success and scrape_error are recorded in scraper_runs for health
    # tracking via the scraper_health view (Feature 7).

    scrape_success = False   # will be flipped to True only on clean scrape
    scrape_error   = None    # will store error message if scrape fails

    # All scraped fields default to None — they stay None if scrape fails
    # and the fallback logic below fills them from Open-Meteo instead.
    temp         = None
    condition    = None
    humidity     = None
    visibility   = None
    wind_speed   = None
    scraped_aqi_value    = None   # weather.com AQI number (primary source)
    scraped_aqi_category = None   # weather.com AQI label (primary source)
    pressure     = None
    sunrise      = None
    sunset       = None
    high         = None
    low          = None
    dew_point    = None
    uv_index     = None
    moon_phase   = None

    if url and url != "fallback":
        try:
            headers = {
                "User-Agent": "Mozilla/5.0",
                "Accept-Language": "en-US,en-IN;q=0.9"
            }
            response = requests.get(url, headers=headers, timeout=20)
            soup = BeautifulSoup(response.text, "html.parser")

            # Safe selector helper — returns None instead of crashing if element missing
            def safe_text(selector):
                el = soup.select_one(selector)
                return el.get_text(strip=True) if el else None

            temp              = safe_text("[data-testid='TemperatureValue']")
            condition         = safe_text("[data-testid='wxPhrase']")
            humidity          = safe_text("[data-testid='PercentageValue']")
            visibility        = safe_text("[data-testid='VisibilityValue']")
            wind_speed        = safe_text("[data-testid='Wind']")
            scraped_aqi_value    = safe_text("[data-testid='DonutChartValue']")
            scraped_aqi_category = safe_text("[data-testid='AirQualityCategory']")
            pressure          = safe_text("[data-testid='PressureValue']")

            times   = soup.select("p.TwcSunChart--dateValue--TzXBr")
            sunrise = times[0].get_text(strip=True) if len(times) > 0 else None
            sunset  = times[1].get_text(strip=True) if len(times) > 1 else None

            temps = soup.select("[data-testid='TemperatureValue']")
            high  = temps[1].get_text(strip=True) if len(temps) > 1 else None
            low   = temps[2].get_text(strip=True) if len(temps) > 2 else None

            dew_label  = soup.find("div", string="Dew Point")
            dew_point  = dew_label.find_next("span").get_text(strip=True) if dew_label else None

            uv_label   = soup.find("div", string="UV Index")
            uv_index   = uv_label.find_next("span").get_text(strip=True) if uv_label else None

            moon_label = soup.find("div", string="Moon Phase")
            moon_phase = moon_label.find_next("div").get_text(strip=True) if moon_label else None

            # If temp is None, the page loaded but the data wasn't there —
            # treat this as a failed scrape so fallback kicks in cleanly.
            if not temp:
                raise ValueError("Critical field 'temp' missing after scrape — possible HTML structure change")

            # If we reach here, scrape was fully successful
            scrape_success = True

        except Exception as e:
            # Log the error for health tracking but don't re-raise —
            # the user will still get their weather via Open-Meteo fallback.
            scrape_error = str(e)
            scrape_success = False
            print(f"[{user_id}] weather.com scrape failed — falling back to Open-Meteo. Error: {e}")
    else:
        # URL was missing or explicitly set to "fallback" — skip scrape entirely
        scrape_error   = "No valid URL provided — Open-Meteo fallback used"
        scrape_success = False

    # ── Open-Meteo Fallback Values ──────────────────────────────────────────
    # If weather.com scraping failed for any reason, we fill the display fields
    # from Open-Meteo data. The user gets a complete weather response either way.
    # data_source tells us in the DB which path was taken for this run.

    c  = weather["current"]
    aq = aqi["current"]

    if not scrape_success:
        # Condition text derived from WMO code — e.g. weather_code 63 → "Moderate Rain"
        wmo_code  = c.get("weather_code")
        condition = WMO_CODES.get(wmo_code, f"Weather Code {wmo_code}") if wmo_code else "Unknown"

        # Temperature as string to match the TEXT columns in weather_scraped
        temp      = str(round(c.get("temperature_2m", 0))) + "°" if c.get("temperature_2m") is not None else None
        high      = None   # Open-Meteo daily high is available but in a different structure
        low       = None   # same — not included here to keep fallback simple and clean
        humidity  = str(c.get("relative_humidity_2m")) + "%" if c.get("relative_humidity_2m") is not None else None
        wind_speed = str(round(c.get("wind_speed_10m", 0))) + " km/h" if c.get("wind_speed_10m") is not None else None
        visibility = str(round(c.get("visibility", 0) / 1000, 1)) + " km" if c.get("visibility") is not None else None
        pressure   = str(round(c.get("pressure_msl", 0))) + " hPa" if c.get("pressure_msl") is not None else None
        uv_index   = str(round(c.get("uv_index", 0), 1)) if c.get("uv_index") is not None else None
        dew_point  = None   # not available in current from Open-Meteo
        moon_phase = None   # not available from Open-Meteo at all

        # Use Open-Meteo daily[0] for sunrise/sunset — stored as ISO strings
        d0_sunrise = weather["daily"].get("sunrise", [None])[0]
        d0_sunset  = weather["daily"].get("sunset",  [None])[0]

        def _fmt_om_time(iso_str):
            """Format Open-Meteo ISO datetime (e.g. '2026-03-03T06:12') → '6:12 AM'."""
            if not iso_str:
                return None
            try:
                from datetime import datetime as _dt
                t = _dt.fromisoformat(iso_str)
                return t.strftime("%I:%M %p").lstrip("0")
            except Exception:
                return None

        sunrise = _fmt_om_time(d0_sunrise)
        sunset  = _fmt_om_time(d0_sunset)

        # AQI fallback — derive category from Open-Meteo us_aqi using EPA breakpoints
        # since weather.com AQI scraping failed along with everything else
        om_aqi = aq.get("us_aqi")
        scraped_aqi_value    = str(om_aqi) if om_aqi is not None else None
        scraped_aqi_category = get_aqi_category(om_aqi)

        data_source = "open_meteo_fallback"
    else:
        # Weather.com scrape succeeded — use its AQI as primary.
        # If weather.com AQI specifically was missing (None) even though the rest
        # of the scrape worked, fall back to Open-Meteo AQI for that field only.
        if scraped_aqi_value is None:
            om_aqi = aq.get("us_aqi")
            scraped_aqi_value    = str(om_aqi) if om_aqi is not None else None
            scraped_aqi_category = get_aqi_category(om_aqi)

        data_source = "weather.com"

    # ── Connect to Database ─────────────────────────────────────────────────
    conn = psycopg2.connect(
        host=r.hostname,
        dbname=r.path.lstrip("/"),
        user=r.username,
        password=password,
        port=r.port or 5432,
        sslmode="require"
    )
    cur = conn.cursor()

    # ── Insert Scraper Run First ────────────────────────────────────────────
    # Insert the run row immediately and get back run_id via RETURNING.
    # We update scrape_success and scrape_error on this same row at the end
    # after all inserts are done, giving us a complete health record per run.
    cur.execute("""
        INSERT INTO scraper_runs (user_id, area, ran_at, scrape_success, scrape_error)
        VALUES (%s, %s, NOW(), %s, %s)
        RETURNING id
    """, (user_id, area, scrape_success, scrape_error))
    run_id = cur.fetchone()[0]

    # ── Insert Current Weather ──────────────────────────────────────────────
    # data_source column tells you whether this row came from weather.com
    # or from Open-Meteo fallback — critical for analysis quality awareness.
    # scraped_aqi_value and scraped_aqi_category store the weather.com AQI
    # (or Open-Meteo fallback AQI if weather.com was unavailable).
    cur.execute("""
        INSERT INTO current_weather (
            user_id, area, run_id, timestamp,
            temperature_2m, relative_humidity_2m, apparent_temperature,
            precipitation, rain, snowfall, cloud_cover,
            pressure_msl, surface_pressure,
            wind_speed_10m, wind_direction_10m, wind_gusts_10m,
            visibility, uv_index, is_day, weather_code,
            pm10, pm2_5, carbon_monoxide,
            nitrogen_dioxide, ozone, sulphur_dioxide, us_aqi,
            scraped_aqi_value, scraped_aqi_category, data_source
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        user_id, area, run_id, datetime.now(),
        c.get("temperature_2m"), c.get("relative_humidity_2m"), c.get("apparent_temperature"),
        c.get("precipitation"), c.get("rain"), c.get("snowfall"), c.get("cloud_cover"),
        c.get("pressure_msl"), c.get("surface_pressure"),
        c.get("wind_speed_10m"), c.get("wind_direction_10m"), c.get("wind_gusts_10m"),
        c.get("visibility"), c.get("uv_index"), c.get("is_day"), c.get("weather_code"),
        aq.get("pm10"), aq.get("pm2_5"), aq.get("carbon_monoxide"),
        aq.get("nitrogen_dioxide"), aq.get("ozone"), aq.get("sulphur_dioxide"), aq.get("us_aqi"),
        scraped_aqi_value, scraped_aqi_category, data_source
    ))

    # ── Insert Weather Scraped ──────────────────────────────────────────────
    # This row is always inserted — even on fallback — so the bot always has
    # a weather_scraped row to read from for user-facing output.
    # data_source tells you whether the values came from scraping or Open-Meteo.
    cur.execute("""
        INSERT INTO weather_scraped (
            user_id, area, run_id, timestamp,
            feels_like, condition,
            sunrise, sunset, high, low,
            humidity, pressure, visibility, wind_speed,
            dew_point, uv_index, moon_phase,
            aqi_value, aqi_category,
            data_source
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        user_id, area, run_id, datetime.now(),
        temp, condition,
        sunrise, sunset, high, low,
        humidity, pressure, visibility, wind_speed,
        dew_point, uv_index, moon_phase,
        scraped_aqi_value, scraped_aqi_category,
        data_source
    ))

    # ── Insert Hourly Weather ───────────────────────────────────────────────
    # Always from Open-Meteo — weather.com has no hourly forecast data.
    # We build all 168 rows (7 days × 24 hours) as tuples then insert in
    # one executemany call for efficiency.
    h = weather["hourly"]
    hourly_weather_rows = []
    for i in range(len(h["time"])):
        hourly_weather_rows.append((
            user_id, area, run_id,
            h["time"][i],
            h.get("temperature_2m",            [None])[i],
            h.get("relative_humidity_2m",      [None])[i],
            h.get("dew_point_2m",              [None])[i],
            h.get("apparent_temperature",      [None])[i],
            h.get("precipitation_probability", [None])[i],
            h.get("precipitation",             [None])[i],
            h.get("rain",                      [None])[i],
            h.get("snowfall",                  [None])[i],
            h.get("cloud_cover",               [None])[i],
            h.get("pressure_msl",              [None])[i],
            h.get("surface_pressure",          [None])[i],
            h.get("wind_speed_10m",            [None])[i],
            h.get("wind_direction_10m",        [None])[i],
            h.get("wind_gusts_10m",            [None])[i],
            h.get("visibility",                [None])[i],
            h.get("uv_index",                  [None])[i],
            h.get("weather_code",              [None])[i],
            h.get("sunshine_duration",         [None])[i],
            h.get("is_day",                    [None])[i],
            h.get("freezing_level_height",     [None])[i],
        ))

    cur.executemany("""
        INSERT INTO hourly_weather (
            user_id, area, run_id, timestamp,
            temperature_2m, relative_humidity_2m, dew_point_2m,
            apparent_temperature, precipitation_probability,
            precipitation, rain, snowfall, cloud_cover,
            pressure_msl, surface_pressure,
            wind_speed_10m, wind_direction_10m, wind_gusts_10m,
            visibility, uv_index, weather_code,
            sunshine_duration, is_day, freezing_level_height
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, hourly_weather_rows)

    # ── Insert Daily Weather ────────────────────────────────────────────────
    # Always from Open-Meteo. Gives us 7 days of forecast per run.
    d = weather["daily"]
    daily_weather_rows = []
    for i in range(len(d["time"])):
        daily_weather_rows.append((
            user_id, area, run_id,
            d["time"][i],
            d.get("temperature_2m_max",          [None])[i],
            d.get("temperature_2m_min",          [None])[i],
            d.get("apparent_temperature_max",    [None])[i],
            d.get("apparent_temperature_min",    [None])[i],
            d.get("sunrise",                     [None])[i],
            d.get("sunset",                      [None])[i],
            d.get("precipitation_sum",           [None])[i],
            d.get("rain_sum",                    [None])[i],
            d.get("snowfall_sum",                [None])[i],
            d.get("precipitation_hours",         [None])[i],
            d.get("daylight_duration",           [None])[i],
            d.get("wind_speed_10m_max",          [None])[i],
            d.get("wind_gusts_10m_max",          [None])[i],
            d.get("wind_direction_10m_dominant", [None])[i],
            d.get("uv_index_max",                [None])[i],
            d.get("shortwave_radiation_sum",     [None])[i],
            d.get("et0_fao_evapotranspiration",  [None])[i],
            d.get("weather_code",                [None])[i],
        ))

    cur.executemany("""
        INSERT INTO daily_weather (
            user_id, area, run_id, date,
            temperature_2m_max, temperature_2m_min,
            apparent_temperature_max, apparent_temperature_min,
            sunrise, sunset,
            precipitation_sum, rain_sum, snowfall_sum,
            precipitation_hours, daylight_duration,
            wind_speed_10m_max, wind_gusts_10m_max,
            wind_direction_10m_dominant,
            uv_index_max, shortwave_radiation_sum,
            et0_fao_evapotranspiration, weather_code_max
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, daily_weather_rows)

    # ── Insert Hourly AQI ───────────────────────────────────────────────────
    # Always from Open-Meteo. aqi_category is derived from us_aqi via EPA
    # breakpoints for every hourly row since weather.com only gives current AQI.
    ha = aqi["hourly"]
    hourly_aqi_rows = []
    for i in range(len(ha["time"])):
        us_aqi_val = ha.get("us_aqi", [None])[i]
        hourly_aqi_rows.append((
            user_id, area, run_id,
            ha["time"][i],
            ha.get("pm10",               [None])[i],
            ha.get("pm2_5",              [None])[i],
            ha.get("carbon_monoxide",    [None])[i],
            ha.get("nitrogen_dioxide",   [None])[i],
            ha.get("ozone",              [None])[i],
            ha.get("sulphur_dioxide",    [None])[i],
            us_aqi_val,
            get_aqi_category(us_aqi_val),   # EPA category derived from us_aqi for each hour
            ha.get("dust",               [None])[i],
            ha.get("uv_index",           [None])[i],
            ha.get("uv_index_clear_sky", [None])[i],
            ha.get("alder_pollen",       [None])[i],
            ha.get("birch_pollen",       [None])[i],
            ha.get("grass_pollen",       [None])[i],
        ))

    cur.executemany("""
        INSERT INTO hourly_aqi (
            user_id, area, run_id, timestamp,
            pm10, pm2_5, carbon_monoxide,
            nitrogen_dioxide, ozone, sulphur_dioxide,
            us_aqi, aqi_category,
            dust, uv_index, uv_index_clear_sky,
            alder_pollen, birch_pollen, grass_pollen
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, hourly_aqi_rows)

    # ── Cleanup Records Older Than 15 Days ─────────────────────────────────
    # saved_at on each table defaults to NOW() at insert time, so this
    # reliably removes stale data without any manual timestamp management.
    cur.execute("DELETE FROM current_weather WHERE saved_at < NOW() - INTERVAL '15 days'")
    cur.execute("DELETE FROM hourly_weather  WHERE saved_at < NOW() - INTERVAL '15 days'")
    cur.execute("DELETE FROM daily_weather   WHERE saved_at < NOW() - INTERVAL '15 days'")
    cur.execute("DELETE FROM hourly_aqi      WHERE saved_at < NOW() - INTERVAL '15 days'")
    cur.execute("DELETE FROM weather_scraped WHERE saved_at < NOW() - INTERVAL '15 days'")
    cur.execute("DELETE FROM scraper_runs    WHERE ran_at   < NOW() - INTERVAL '15 days'")

    conn.commit()
    cur.close()
    conn.close()

    print(f"[{user_id}] Scraper done — area: {area} | run_id: {run_id} | source: {data_source} | scrape_success: {scrape_success}")


if __name__ == "__main__":
    run_scraper()