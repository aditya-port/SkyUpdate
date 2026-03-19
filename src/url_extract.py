from ddgs import DDGS


def get_weather_url(place: str) -> str:
    """
    Searches for the weather.com today URL for a given place.
    Returns the URL string or 'fallback' if not found.
    Drop-in replacement — identical signature to original.
    """
    try:
        with DDGS() as ddgs:
            results = list(ddgs.text(
                f"today weather {place} site:weather.com",
                region="in-en",
                max_results=10,
            ))

            for r in results:
                url = r.get("href", "")
                if "weather.com" in url and "/weather/today/l/" in url:
                    print(f"[URLExtract] Found: {url}")
                    return url

        print(f"[URLExtract] No URL found for: {place}")
        return "fallback"

    except Exception as e:
        print(f"[URLExtract] ERROR: {e}")
        return "fallback"


if __name__ == "__main__":
    test_places = [
        "Noida, Uttar Pradesh, India",
        "Mumbai, Maharashtra, India",
        "Siliguri, West Bengal, India",
    ]

    for place in test_places:
        print(f"\nSearching: {place}")
        result = get_weather_url(place)
        print(f"Result: {result}")