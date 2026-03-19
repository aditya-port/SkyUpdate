# ☁️ SkyUpdate — PostgreSQL Data Pipeline & Analytics Platform

> A database-driven analytics project focused on designing a normalized PostgreSQL data warehouse, enabling real-time data ingestion, analytical querying, and interactive visualizations for weather and air quality insights.

🤖 **Try it live → [@SkyUpdate_Bot](https://t.me/SkyUpdate_Bot)**

---

## 📌 Executive Summary

SkyUpdate is a production Telegram bot that goes beyond displaying API numbers. Every weather request triggers a complete data pipeline — live scraping, dual-source data ingestion, structured database writes, and a prioritised analysis layer — to answer questions people actually ask: *do I need an umbrella, is it safe to run outside, what's the air like today?*

Key capabilities built and deployed:

- Built an end-to-end data pipeline ingesting and storing ~345 records per request across multiple normalized tables

- Designed and implemented a PostgreSQL data model (16+ tables) enabling scalable, traceable, and query-efficient analytics

- Developed a rule-based analytics engine (21 functions) to generate prioritized, time-windowed insights from raw time-series data

- Implemented automated data workflows & scheduled jobs (alerts, reminders, deduplication) for real-time decision support

- Deployed and automated data pipelines using Supabase (PostgreSQL) and Railway with GitHub integration, ensuring consistent data processing and high availability
---

## 🧩 Business Problem

Weather apps show numbers. They don't tell you what to do with them. A UV index of 9 means nothing to most people — "avoid going outside between 1pm and 3pm today" does. SkyUpdate solves the gap between raw meteorological data and human-readable, actionable guidance.

The secondary problem is data persistence. Most bots are stateless — every request is independent. SkyUpdate accumulates every fetch into a relational database, enabling personal statistics, historical anomaly detection, community signals, and scheduled proactive alerts — none of which are possible without structured storage.

---

## ⚙️ Methodology

**Data Extraction and Ingestion** — each weather fetch runs through a five-stage pipeline. First, a reverse geocode converts raw GPS coordinates into a human-readable area string via OpenStreetMap Nominatim. Next, a 30-minute cache check queries the database to skip redundant pipeline runs. If the cache misses, a DuckDuckGo search (via the `ddgs` library, no browser required) retrieves the location's Weather.com URL. BeautifulSoup then scrapes live conditions — feels-like temperature, AQI with category label, condition string, sunrise/sunset, moon phase. In parallel, a single Open-Meteo REST call returns 168 hourly rows and 7 daily rows covering temperature, precipitation probability, wind, UV, AQI pollutants, and pollen. All of this is written to PostgreSQL in one transaction.

**Database Design** — the schema is built around a central `scraper_runs` table that acts as the spine. Every child table — `current_weather`, `hourly_weather`, `daily_weather`, `hourly_aqi`, `weather_scraped` — references it via `run_id`. This means every data point is traceable to exactly when it was collected, for which user, in which area. User preferences, saved locations, alert logs, feedback, and event reminders are stored in eight additional tables. The full schema includes 16 tables, 2 pending tables, and a `scraper_health` monitoring view.

**Analysis Layer** — the insight engine in `insights_engine.py` runs 21 independent functions at query time — nothing is pre-computed. Techniques include the NWS Heat Index polynomial formula, consecutive window grouping for rain and UV time ranges, 4-tier rain classification with intensity and leave-before framing, composite 8-point outdoor window scoring across simultaneous UV/rain/temperature/AQI dimensions, pressure trend detection, and historical anomaly detection against a 30-row monthly baseline. Results are sorted by priority tier (dangerous → contextual) and the most critical ones surface first.

**Scheduled Jobs** — four background jobs run continuously: a per-user morning alert at a configurable time (querying `users.alert_time`), an hourly rain proximity check, a Sunday weekly digest, and a daily event reminder system with day-before and on-day passes.

---

## 🛠️ Skills

**Languages and Runtimes** — Python 3.11+, SQL (PostgreSQL)

**Database** — PostgreSQL via Supabase, asyncpg (async), psycopg2 (sync), schema design, foreign key modelling, RLS policies, read-only role management, connection pooling (Session Pooler vs Transaction Pooler)

**Data Pipeline** — web scraping with BeautifulSoup, REST API integration (Open-Meteo, OpenStreetMap Nominatim), headless search via `ddgs`, reverse geocoding, 30-minute intelligent caching

**Analysis** — time-series window grouping, NWS Heat Index formula, composite multi-dimensional scoring, trend detection, historical baseline comparison, priority tier sorting, mutual exclusion logic between insight functions

**Bot Framework** — python-telegram-bot v21 with JobQueue, ConversationHandler, inline keyboards, callback data encoding for stateless restart recovery

**Infrastructure** — Railway (cloud deployment), Supabase (managed PostgreSQL), GitHub (version control), environment variable management, Procfile-based worker configuration

**Visualisation** — custom dark-theme weather card image generated with Pillow (PIL), dynamic layout that auto-sizes to content

---

## 📊 Results and Key Features

The pipeline reliably produces ~345 structured database writes per weather request across 6 tables. The insight engine surfaces between 1 and 21 prioritised alerts depending on conditions, with tier-1 dangerous alerts always shown first and lifestyle insights collapsed behind an expandable panel.

The alert system fires per-user at individually configured times (6am–11:59am range), deduplicated at the database level via a `UNIQUE(user_id, alert_date)` constraint — meaning no duplicate alerts even on bot restarts. The rain proximity job checks every subscribed user hourly and fires a push notification if precipitation probability exceeds 80% in the next two hours.

The weather card (visual mode) renders a full dark-theme PNG with temperature, condition, AQI pill, rain probability, UV, wind, sunrise/sunset, and moon phase — all dynamically sized. Users can toggle between visual card and text card via `/settings`.

Public read-only database access is available via the `skyupdate_readonly` role for anyone who wants to query the underlying data directly.

---

## 🔭 Next Steps

The most impactful near-term improvement is URL caching — storing the Weather.com URL in `saved_locations.url` after the first search so subsequent requests for the same location skip the DuckDuckGo call entirely, reducing latency and search API usage to near zero for repeat users.

On the analysis side, multi-day streak detection is partially implemented via `insight_history` but not yet surfaced to users — completing this would allow alerts like *"AQI has been unhealthy 4 days in a row"* which adds meaningful longitudinal context.

Other natural next steps include a web dashboard (Metabase or custom) over the Supabase database for visualising usage patterns, a feedback loop that uses `insight_feedback` thumbs up/down data to tune which insight tiers surface most frequently, and A/B testing of alert formats based on interaction rate tracked in `alerts_sent.interacted`.

KPIs worth tracking as the user base grows: cache hit rate, scrape success rate (via `scraper_health` view), morning alert open rate (`interacted / sent`), and average insight tiers triggered per user per day.

---

> Built with Python, PostgreSQL, Open-Meteo, and too many late nights. 🌧️
