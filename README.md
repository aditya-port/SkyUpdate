# ☁️ SkyUpdate — Weather Intelligence Bot

### 🤖 Try it live → [@SkyUpdate_Bot](https://t.me/SkyUpdate_Bot)

> A Telegram bot that scrapes, stores, and analyses real weather data to answer questions people actually ask — *do I need an umbrella, is it safe to run outside, what should I wear today* — rather than just showing numbers.

---

## 💡 What makes this different

Most weather bots are a thin wrapper around a single API call. SkyUpdate runs a full data pipeline on every request — live scraping, two independent data sources, a normalised relational database, and a 21-function analysis engine that reads from that database to produce prioritised, actionable output. Every weather check leaves a complete record. That data compounds over time into personal statistics, anomaly detection, and community signals.

---

## 🗄️ Database design

The schema is the core of the project. Every fetch writes across six tables simultaneously. Every user action is logged. The database is the source of truth for everything — caching decisions, alert deduplication, feedback tracking, historical baselines, and the insight engine all read directly from it.

### 📐 Schema overview

**🔵 `scraper_runs`** — one row per weather fetch. The anchor table that everything else joins to.
```
id, user_id, area, lat, lon, session_id, ran_at, scrape_success, scrape_error, weather_code
```

**🌡️ `current_weather`** — point-in-time snapshot at fetch time, joined to scraper_runs by run_id.
```
run_id, temperature_2m, relative_humidity_2m, apparent_temperature,
surface_pressure, wind_speed_10m, wind_gusts_10m, wind_direction_10m,
uv_index, is_day, weather_code, us_aqi, scraped_aqi_value, scraped_aqi_category
```

**⏱️ `hourly_weather`** — 168 rows per fetch (7 days × 24 hours). The insight engine's primary input.
```
run_id, timestamp, temperature_2m, apparent_temperature, relative_humidity_2m,
precipitation_probability, rain, snowfall, cloud_cover, wind_speed_10m,
wind_gusts_10m, visibility, uv_index, sunshine_duration, is_day,
freezing_level_height, pressure_msl
```

**📅 `daily_weather`** — 7 rows per fetch. Used for the weather card, weekly forecast, and weekly digest.
```
run_id, date, temperature_2m_max, temperature_2m_min,
apparent_temperature_max, apparent_temperature_min,
rain_sum, snowfall_sum, precipitation_hours, precipitation_sum,
wind_speed_10m_max, wind_gusts_10m_max, wind_direction_10m_dominant,
uv_index_max, sunrise, sunset, daylight_duration, weather_code_max
```

**🏭 `hourly_aqi`** — 168 rows per fetch. Parallel to hourly_weather, joined by run_id + timestamp.
```
run_id, timestamp, pm2_5, pm10, ozone, nitrogen_dioxide, sulphur_dioxide,
carbon_monoxide, us_aqi, dust, alder_pollen, birch_pollen, grass_pollen
```

**🌐 `weather_scraped`** — live-scraped values from weather.com, joined by run_id. These are the display-quality numbers shown to the user — feels-like, condition string, AQI category label, moon phase.
```
run_id, user_id, feels_like, condition, high, low, humidity, pressure,
dew_point, wind_speed, uv_index, aqi_value, aqi_category,
sunrise, sunset, moon_phase, data_source, timestamp
```

**📌 `saved_locations`** — user-saved places with coordinates and weather.com URL cached.
```
id, user_id, nickname, area, lat, lon, url, is_default, created_at
```

**👤 `users`** — customer table with usage counters and preferences.
```
user_id, username, first_name, weather_checks, successful_runs,
contact, alerts_enabled, alert_time, created_at, last_seen
```

**📋 `user_activity`** — full audit log. Every action, every location share, every button tap.
```
user_id, action, detail, area, lat, lon, condition,
url_requested, session_id, timestamp
```

**🔔 `event_reminders`**, **`morning_alerts_log`**, **`alerts_sent`**, **`weekly_digest_log`**, **`insight_feedback`** — scheduling deduplication and feedback capture.

---

## 🔄 Data pipeline

Every weather check runs through the same pipeline:

```
1. 🗺️  Reverse geocode lat/lon → area string  (Nominatim / OpenStreetMap)

2. ⚡  Cache check — query weather_scraped WHERE area = $1
       AND timestamp > NOW() - INTERVAL '30 minutes'
       AND data_source = 'weather.com'
       → hit:  reuse existing run_id, skip to step 5
       → miss: continue

3. 🔍  URL extraction — Selenium headless browser searches DuckDuckGo
       for the weather.com page for this area, extracts the direct URL

4. 🕸️  Scrape weather.com — BeautifulSoup parses current conditions:
       temperature, feels-like, AQI, UV, humidity, pressure, wind,
       sunrise/sunset, condition string → INSERT INTO weather_scraped

5. 📡  Open-Meteo API call — single REST request for lat/lon returns:
       ├─ 168 hourly rows (7d × 24h): temp, rain prob, UV, wind, AQI, pollen…
       ├─ 7 daily rows: max/min, rain_sum, UV_max, wind_max, sunrise, sunset…
       └─ Current conditions snapshot
       → bulk INSERT INTO hourly_weather  (168 rows)
       → bulk INSERT INTO daily_weather   (7 rows)
       → bulk INSERT INTO hourly_aqi      (168 rows)
       → INSERT INTO current_weather
       → INSERT INTO scraper_runs

6. 🧠  Insight engine reads back from DB → produces analysis → sends to Telegram
```

> 💾 On a cold cache with a fresh scrape, a single weather request writes **~345 rows across 6 tables**.

---

## 🧠 Analysis layer

The insight engine reads the hourly and daily tables and runs 21 independent analysis functions. Every function performs a real calculation — no simple threshold flags.

### ⚙️ Techniques used

**🕐 Consecutive window grouping** — custom `group_consecutive_hours()` function that collapses hourly rows into time ranges. Used across rain, UV, wind, PM2.5, ozone, visibility, heat stress, and frost checks. A rain alert says *"heavy rain 2pm–5pm"* not *"rain today"* because the engine identifies the contiguous block of high-probability hours and reports its bounds.

**🌡️ NWS Heat Index formula** — full 9-term polynomial (US National Weather Service) applied per hour using `temperature_2m` × `relative_humidity_2m` from `hourly_weather`. Separate from the Open-Meteo `apparent_temperature` field — used specifically for heat stress classification (mild / moderate / high / dangerous).

**🌧️ 4-tier rain model** — goes beyond max probability:
- Tier 1 (< 10%): counts hours above 6% to detect cumulative dry-day risk
- Tier 2 (10–29%): groups sustained clusters ≥15% probability, reports longest window
- Tier 3 (30–59%): identifies every cluster independently, reports each time range
- Tier 4 (≥60%): classifies intensity from mm/hr rate (light / moderate / heavy), calculates minutes until rain start, generates leave-before framing (*"you have time" / "leave soon" / "starting in X min" / "already ongoing"*)

**📉 Trend detection** — pressure change (hPa delta across remaining hours from `pressure_msl`), AQI trajectory (3-hour rolling mean of `us_aqi` comparing first half of day vs second half)

**🌟 Composite scoring** — `insight_best_outdoor_window` scores every consecutive 2-hour daytime block on 4 dimensions simultaneously (UV, rain probability, apparent temperature, AQI) by joining `hourly_weather` and `hourly_aqi` on `(date, hour)`. Returns the best window with its specific deficiencies listed.

**🚫 Mutual exclusion** — `get_exercise_air_score` re-runs the scorer loop from `get_best_run_time` internally. If any daytime hour would score ≥5/8, it returns `None` immediately. The two functions physically cannot both appear in the same output — no post-processing filter needed.

**📊 Historical anomaly detection** — queries `daily_weather` for the past 30 daily rows in the same calendar month, computes a baseline average, and compares today's mean `temperature_2m` against it. Requires ≥7 historical rows before firing (no false anomalies on a new user).

**🔢 Priority tier sorting** — all 21 functions return independently, results are sorted by tier (1=dangerous → 6=contextual). Tier 1+2 always shown immediately; tier 3–6 collapsed behind a "Show more" button.

| Tier | 🏷️ Category | Examples |
|:---:|---|---|
| 🔴 1 | Dangerous | Heat stress, frost, snow, extreme apparent temperature |
| 🟠 2 | Rain | 4-tier model with intensity + timing framing |
| 🟡 3 | AQI / Air | AQI, PM2.5, ozone, dust, pollen (grass/alder/birch), AQI trend |
| 🟢 4 | UV | UV index with daytime exposure window |
| 🔵 5 | Wind | Wind speed, wind chill |
| ⚪ 6 | Contextual | Sunshine, visibility, pressure trend, best outdoor window, daylight |

---

## ⏰ Scheduled analysis jobs

Four jobs run on the database continuously:

**☀️ Morning alert (7 AM daily)** — reads the full day's `hourly_weather` and `hourly_aqi` for each subscribed user's default location, runs 15 severity scorers (scored 3–10), picks the top 3 by severity score, formats and sends. Uses `morning_alerts_log` with `ON CONFLICT (user_id, alert_date) DO NOTHING` to prevent double-send on restart. Users can snooze one day by pre-inserting tomorrow's date into that table.

**🌧️ Rain proximity alert (every hour)** — queries `hourly_weather WHERE timestamp BETWEEN now AND now + INTERVAL '2 hours'` for all subscribed users. Fires if `MAX(precipitation_probability) > 80`. Deduplication via `alerts_sent` prevents more than one rain alert per user per day.

**📆 Weekly digest (Sunday 8 AM)** — queries `daily_weather` for the next 7 days per user, uses a composite score (`precipitation_sum × 3 + wind_gusts × 0.5`) to identify worst day and (`temperature_2m_max − precipitation_sum × 5`) for best day. Only runs on data where `scraper_runs.ran_at > NOW() - INTERVAL '24 hours'` to avoid sending stale forecasts as "this week".

**🎯 Event reminders (7 AM daily)** — two-pass query: events where `event_date = tomorrow` get a forecast preview, events where `event_date = today` get a morning briefing. `sent` flag set `TRUE` only after the on-day message goes out — prevents partial sends from being silently lost on restart.

---

## ⚡ Caching and deduplication

**🗃️ 30-minute weather cache** — before any scrape or API call, queries `weather_scraped` for a row younger than 30 minutes for the same `area`. On hit, the existing `run_id` is reused and ~345 DB writes are skipped. The weather card footer shows when data is served from cache and its age in minutes.

**🔁 Commit visibility lag fix** — the scraper runs in a thread and commits via a separate connection. A new async connection opened milliseconds later may not see that commit under PostgreSQL's read committed isolation. Fix: query `current_weather` directly by `run_id` (no JOIN), retry once after 500ms if the result is NULL. No schema changes needed.

**🔗 Inline callback data encoding** — inline button `callback_data` encodes area and nickname directly (e.g. `choice_weather|Siliguri, WB|Home`). Bot restarts wipe in-memory session state — encoding into callback_data means every button continues working across restarts without any session persistence layer.

---

## 🌐 Dual data source strategy

weather.com and Open-Meteo serve different roles deliberately:

| | 🌐 weather.com (scraped) | 📡 Open-Meteo (API) |
|---|---|---|
| **✅ Good at** | Display-quality current conditions, AQI with category labels, local condition strings | 7-day forecast, hourly probability data, pollen, UV index per hour |
| **🎯 Used for** | AQI display number, condition label, feels-like on the card | Everything the insight engine computes from |
| **⚠️ Limitation** | Current day only, scraping can fail | AQI numbers can be inflated vs reality |

When the scraper fails, `data_source = 'open_meteo_fallback'` is written to `weather_scraped` and the weather card shows a notice. AQI from Open-Meteo is never shown as a raw number to the user — only the EPA category word is used, since the model values are unreliable.

---

## 📦 What's stored vs what's computed

Nothing is stored that can be derived. The DB holds raw measurements. Analysis runs at read time.

Examples of what is **computed at query time** rather than stored:

- 🌡️ Heat index (NWS formula on `temperature_2m` × `relative_humidity_2m`)
- 🌧️ Rain window groupings (consecutive hour clustering on `precipitation_probability`)
- 🌟 Best outdoor window score (cross-join of `hourly_weather` and `hourly_aqi`)
- 📉 Pressure trend (delta of `pressure_msl` across remaining hours)
- 📊 Historical temperature anomaly (30-row aggregate query on `daily_weather`)
- 👥 Community signal (`COUNT(DISTINCT user_id)` + `MODE() WITHIN GROUP` on `weather_scraped`)
- 🌇 Daylight remaining (subtraction of `sunset` from `NOW()`)

---

## 📊 Personal stats powered by the audit log

Because `user_activity` logs every action and `daily_weather` accumulates over time, `/stats` answers questions most apps never think to ask:

- 🔥 Hottest day ever recorded for this user (across all of `daily_weather`)
- 💧 Rainiest day ever
- 📍 Most-checked location (`GROUP BY area` on `scraper_runs`)
- 📅 Using SkyUpdate since (`MIN(ran_at)` on `scraper_runs`)
- 🌧️ Rainy days this month (`COUNT DISTINCT` dates where `precipitation_sum > 1mm`)

---

## 🚀 Setup

**Requirements:** PostgreSQL · Python 3.11+ · Microsoft Edge + msedgedriver

```bash
pip install python-telegram-bot asyncpg aiohttp beautifulsoup4 selenium python-dotenv requests
```

```

```bash
python bot.py
```

---

## 📟 Commands

| Command | Description |
|---|---|
| `/sharelocation` | 📍 Send GPS location |
| `/savedlocations` | 📌 Load or delete saved places |
| `/insights` | 💡 Full prioritised insight breakdown |
| `/wear` | 👗 Plain-English outfit recommendation |
| `/hourly` | ⏱️ 6-hour forecast strip |
| `/stats` | 📊 Monthly + all-time weather history |
| `/remind` | 🎯 Set an event weather reminder |
| `/details` | 🔬 Humidity, pressure, dew point, UV |
| `/pause` · `/resume` | 🔕 Toggle morning alerts |
| `/alerttime HH:MM` | ⏰ Set your preferred alert time |

---

> 🤖 **Try it:** [@SkyUpdate_Bot](https://t.me/SkyUpdate_Bot)

*Built across sussions and cllimates😉"
Source is closed only for usage. Sorry!
