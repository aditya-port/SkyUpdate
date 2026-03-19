import re
from ddgs import DDGS


def _normalise(url: str) -> str:
    """
    Ensures the URL always uses the en-IN locale.
    Handles three cases:
      - already has en-IN           → return as is
      - has a different locale      → replace with en-IN
      - no locale (bare /weather/)  → inject en-IN
    """
    # Already correct
    if "/en-IN/weather/today/" in url:
        return url

    # Has some locale (xx-XX) — replace it with en-IN
    url = re.sub(
        r"weather\.com/[a-z]{2}-[A-Z]{2}/weather/today/",
        "weather.com/en-IN/weather/today/",
        url
    )
    if "/en-IN/weather/today/" in url:
        return url

    # No locale — bare weather.com/weather/today/
    url = url.replace(
        "weather.com/weather/today/",
        "weather.com/en-IN/weather/today/"
    )
    return url


def _is_valid(url: str) -> bool:
    """Only check: URL must contain /weather/today/"""
    return bool(url) and "/weather/today/" in url


def _search(query: str) -> list:
    try:
        with DDGS() as ddgs:
            results = list(ddgs.text(
                query,
                region="in-en",
                max_results=15,
            ))
        return [
            r.get("href", "")
            for r in results
            if "weather.com" in r.get("href", "") and _is_valid(r.get("href", ""))
        ]
    except Exception as e:
        print(f"[URLExtract] DDG error: {e}")
        return []


def get_weather_url(place: str) -> str:
    """
    Main entry point. Identical signature to original url_extract.py.
    Returns a normalised en-IN weather.com URL or 'fallback'.
    """
    print(f"[URLExtract] Searching for: {place}")

    # Attempt 1 — specific query
    urls = _search(f"today weather {place} site:weather.com")

    # Attempt 2 — broader fallback
    if not urls:
        print(f"[URLExtract] Attempt 1 empty — trying broader query")
        urls = _search(f"{place} weather.com today forecast")

    if not urls:
        print(f"[URLExtract] No URL found for: {place}")
        return "fallback"

    # Take first valid result and normalise to en-IN
    raw = urls[0]
    final = _normalise(raw)
    # print(f"[URLExtract] Raw:   {raw}")
    print(f"[URLExtract] Final: {final}")
    return final