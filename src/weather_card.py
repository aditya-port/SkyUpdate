"""
weather_card.py  —  SkyUpdate visual weather card
Returns a BytesIO PNG buffer for bot.send_photo().
"""

from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
import os, sys, math

# -- Font resolution: loads from src/fonts/ next to this file ----------------
# Works on Windows, Linux, and Docker without system font install.
# Add Poppins-Bold.ttf (plus Medium/Regular/Light) to src/fonts/.
# Falls back through lighter weights if a variant is missing.

_BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
_FONTS_DIR = os.path.join(_BASE_DIR, "fonts")

def _font_path(filename):
    p = os.path.join(_FONTS_DIR, filename)
    return p if os.path.exists(p) else None

_BOLD = (_font_path("Poppins-Bold.ttf")    or _font_path("Poppins-Medium.ttf")  or _font_path("Poppins-Regular.ttf"))
_MED  = (_font_path("Poppins-Medium.ttf")  or _font_path("Poppins-Regular.ttf") or _BOLD)
_REG  = (_font_path("Poppins-Regular.ttf") or _font_path("Poppins-Light.ttf")   or _MED)
_LT   = (_font_path("Poppins-Light.ttf")   or _REG)

if not _BOLD:
    raise FileNotFoundError(
        f"No Poppins fonts found in {_FONTS_DIR}. "
        "Add Poppins-Bold.ttf (and Medium/Regular/Light) to src/fonts/."
    )

BG        = (15,  15,  18)
DIVIDER   = (40,  40,  50)
WHITE     = (255, 255, 255)
OFF_WHITE = (222, 222, 232)
MUTED     = (112, 114, 132)
ACCENT    = (100, 170, 255)
GREEN     = ( 85, 210, 125)
YELLOW    = (205, 215,  75)
ORANGE    = (255, 155,  60)
RED       = (255,  75,  75)
SUN_COL   = (255, 185,  65)
DOT       = (48,  50,  65)

PAD = 48   # horizontal padding only — width is now content-fitted

def _f(path, size):
    return ImageFont.truetype(path, size)

def _tw(draw, text, font):
    return int(draw.textlength(text, font=font))

def _aqi_col(v):
    if v is None:  return MUTED
    if v <=  50:   return GREEN
    if v <= 100:   return YELLOW
    if v <= 150:   return ORANGE
    return RED

def _uv_col(v):
    if v is None: return MUTED
    try:
        n = float(str(v).split()[0])
        if n <= 2: return GREEN
        if n <= 5: return YELLOW
        if n <= 7: return ORANGE
        return RED
    except Exception:
        return MUTED

def _pill(draw, x, y, w, h, fill, r=14):
    draw.rounded_rectangle([x, y, x+w, y+h], radius=r, fill=fill)

def _sun_icon(draw, cx, cy, r=9, rising=True):
    col = SUN_COL if rising else ACCENT
    draw.ellipse([cx-r, cy-r, cx+r, cy+r], fill=col)
    for angle in [0, 90, 180, 270]:
        rad = math.radians(angle)
        x1 = int(cx+(r+3)*math.cos(rad)); y1 = int(cy+(r+3)*math.sin(rad))
        x2 = int(cx+(r+6)*math.cos(rad)); y2 = int(cy+(r+6)*math.sin(rad))
        draw.line([x1,y1,x2,y2], fill=col, width=2)
    if rising:
        draw.polygon([(cx-3,cy+r+8),(cx+3,cy+r+8),(cx,cy+r+3)], fill=col)
    else:
        draw.polygon([(cx-3,cy+r+3),(cx+3,cy+r+3),(cx,cy+r+8)], fill=col)

def _moon_icon(draw, cx, cy, r=9):
    draw.ellipse([cx-r, cy-r, cx+r, cy+r], fill=(165,170,210))
    draw.ellipse([cx-r+5, cy-r-2, cx+r+5, cy+r-2], fill=BG)


def build_weather_card(data: dict) -> BytesIO:
    temp         = data.get("temperature", 0)
    condition    = data.get("condition", "")
    area         = data.get("area", "")
    humidity     = data.get("humidity")
    wind_speed   = data.get("wind_speed")
    wind_dir     = data.get("wind_dir", "")
    wind_gusts   = data.get("wind_gusts")
    uv           = data.get("uv_index")
    feels_like   = data.get("feels_like")
    aqi          = data.get("aqi")
    aqi_cat      = data.get("aqi_category", "")
    rain_chance  = data.get("rain_chance")
    rain_mm      = data.get("rain_mm")
    high         = data.get("high")
    low          = data.get("low")
    sunrise      = data.get("sunrise")
    sunset       = data.get("sunset")
    daylight_hrs = data.get("daylight_hrs")   # e.g. 12.4
    pressure     = data.get("pressure")
    dew_point    = data.get("dew_point")
    visibility   = data.get("visibility")
    moon_phase   = data.get("moon_phase")
    data_source  = data.get("data_source", "")
    updated_str  = data.get("updated", "")

    has_pills  = any(v is not None for v in [feels_like, rain_chance, aqi])
    has_hilow  = high is not None and low is not None
    has_sun    = sunrise or sunset
    has_extras = any(v is not None for v in [pressure, dew_point, visibility])

    # ── Font sizes — everything bumped up ────────────────────────────────────
    SZ_AREA   = 30
    SZ_APP    = 18
    SZ_TEMP   = 112
    SZ_DEG    = 54
    SZ_COND   = 34
    SZ_META   = 26     # humidity / wind / UV row
    SZ_PVAL   = 26     # pill value
    SZ_PLBL   = 17     # pill label
    SZ_FOOTER = 18

    # ── Measure widest line to fit canvas ────────────────────────────────────
    _img_m  = Image.new("RGB",(1200,50))
    _draw_m = ImageDraw.Draw(_img_m)

    def mw(text, path, size):
        return int(_draw_m.textlength(text, font=ImageFont.truetype(path, size)))

    # Build candidate wide strings
    meta_parts = []
    if humidity   is not None: meta_parts.append(f"Humidity  {humidity}%")
    if wind_speed is not None:
        ws = f"Wind  {wind_speed} km/h{(' '+wind_dir) if wind_dir else ''}"
        if wind_gusts: ws += f"  (gusts {wind_gusts})"
        meta_parts.append(ws)
    if uv is not None: meta_parts.append(f"UV  {uv}")
    meta_str = "   ·   ".join(meta_parts)

    extras = []
    if pressure   is not None: extras.append(f"Pressure  {pressure} hPa")
    if dew_point  is not None: extras.append(f"Dew  {dew_point}°")
    if visibility is not None: extras.append(f"Visibility  {visibility} km")
    extras_str = "   ·   ".join(extras)

    sun_str = ""
    if sunrise: sun_str += f"  {sunrise}"
    if sunset:  sun_str += f"          {sunset}"
    if daylight_hrs: sun_str += f"   ({daylight_hrs} hrs daylight)"
    sun_str = sun_str.strip()

    candidates = [
        mw(meta_str,   _REG,  SZ_META),
        mw(extras_str, _REG,  SZ_META),
        mw(sun_str,    _REG,  SZ_META) + 60,  # extra for icons
        mw(condition,  _MED,  SZ_COND),
        mw(area,       _MED,  SZ_AREA) + mw("SkyUpdate", _LT, SZ_APP) + 60,
    ]
    content_w = max(candidates) if candidates else 600
    W = max(580, content_w + PAD * 2)

    # ── Row heights ───────────────────────────────────────────────────────────
    ROW_AREA   = 38
    ROW_DIV    = 22
    ROW_TEMP   = 126
    ROW_COND   = 48
    ROW_META   = 52
    ROW_PILL   = 84
    ROW_HILOW  = 50
    ROW_SUN    = 54
    ROW_EXTRAS = 50
    ROW_MOON   = 46
    ROW_FOOTER = 44
    BOT_PAD    = 44

    H  = 52 + ROW_AREA + ROW_DIV + ROW_TEMP + ROW_COND + ROW_META
    if has_pills:  H += ROW_PILL
    if has_hilow:  H += ROW_HILOW
    if has_sun:    H += ROW_SUN
    if has_extras: H += ROW_EXTRAS
    if moon_phase: H += ROW_MOON
    H += 20   # pre-divider gap
    if updated_str or data_source == "open_meteo_fallback":
        H += ROW_FOOTER
    H += BOT_PAD

    # ── Canvas ────────────────────────────────────────────────────────────────
    img  = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)

    for i, c in enumerate([(85,150,255),(95,162,255),(108,172,255),(125,182,255)]):
        draw.rectangle([0, i, W, i+1], fill=c)

    fa   = _f(_MED,  SZ_AREA);  fapp = _f(_LT,   SZ_APP)
    ft   = _f(_BOLD, SZ_TEMP);  fdeg = _f(_BOLD, SZ_DEG)
    fc   = _f(_MED,  SZ_COND);  fm   = _f(_REG,  SZ_META)
    fpv  = _f(_MED,  SZ_PVAL);  fpl  = _f(_LT,   SZ_PLBL)
    fft  = _f(_LT,   SZ_FOOTER)

    y = 50

    # Area + watermark
    app_txt = "SkyUpdate"
    draw.text((W - PAD - _tw(draw, app_txt, fapp), y+3), app_txt, font=fapp, fill=(180, 182, 200))
    draw.text((PAD, y), area, font=fa, fill=OFF_WHITE)
    y += ROW_AREA
    draw.rectangle([PAD, y, W-PAD, y+1], fill=DIVIDER)
    y += ROW_DIV

    # Temperature
    ts = str(temp)
    tw_ = _tw(draw, ts, ft)
    draw.text((PAD, y), ts, font=ft, fill=WHITE)
    draw.text((PAD+tw_+3, y+12), "°", font=fdeg, fill=(148,151,175))
    y += ROW_TEMP

    # Condition
    draw.text((PAD, y), condition, font=fc, fill=OFF_WHITE)
    y += ROW_COND

    # Meta row — label muted, value coloured for UV
    x = PAD
    for i, (lbl, val, vcol) in enumerate([
        p for p in [
            ("Humidity",  f"{humidity}%",    MUTED)     if humidity   is not None else None,
            ("Wind",      f"{wind_speed} km/h{(' '+wind_dir) if wind_dir else ''}{('  (gusts '+str(wind_gusts)+')') if wind_gusts else ''}", MUTED) if wind_speed is not None else None,
            ("UV",        str(uv),           _uv_col(uv)) if uv is not None else None,
        ] if p is not None
    ]):
        if i > 0:
            draw.text((x, y+1), "·", font=fm, fill=DOT)
            x += _tw(draw, "·", fm) + 14
        lw = _tw(draw, lbl+"  ", fm)
        draw.text((x, y), lbl+"  ", font=fm, fill=MUTED)
        draw.text((x+lw, y), val, font=fm, fill=vcol)
        x += lw + _tw(draw, val, fm) + 14
    y += ROW_META

    # Pills
    if has_pills:
        pills = []
        if feels_like  is not None: pills.append(("Feels like", f"{feels_like}°", ACCENT))
        if rain_chance is not None:
            rv = f"{rain_chance}%"
            if rain_mm: rv += f"  ~{rain_mm}mm"
            pills.append(("Rain", rv, (110,155,255)))
        if aqi is not None:
            pills.append(("Air Quality", f"{aqi}  {aqi_cat}" if aqi_cat else str(aqi), _aqi_col(aqi)))
        px = PAD
        for lbl, val, col in pills:
            pw = max(148, max(_tw(draw,val,fpv), _tw(draw,lbl,fpl)) + 44)
            _pill(draw, px, y, pw, 64, (24,24,32), r=15)
            draw.text((px+16, y+7),  lbl, font=fpl, fill=MUTED)
            draw.text((px+16, y+27), val, font=fpv, fill=col)
            px += pw + 12
        y += ROW_PILL

    # H / L
    if has_hilow:
        draw.text((PAD, y), f"H  {high}°", font=fm, fill=ORANGE)
        hw = _tw(draw, f"H  {high}°", fm)
        draw.text((PAD+hw+20, y), "·", font=fm, fill=DOT)
        draw.text((PAD+hw+38, y), f"L  {low}°", font=fm, fill=ACCENT)
        y += ROW_HILOW

    # Sunrise / Sunset / Daylight
    if has_sun:
        ir = 9
        x  = PAD
        if sunrise:
            _sun_icon(draw, x+ir+1, y+13, r=ir, rising=True)
            x += ir*2+12
            draw.text((x, y), sunrise, font=fm, fill=MUTED)
            x += _tw(draw, sunrise, fm)+28
            draw.text((x, y), "·", font=fm, fill=DOT)
            x += _tw(draw, "·", fm)+28
        if sunset:
            _sun_icon(draw, x+ir+1, y+13, r=ir, rising=False)
            x += ir*2+12
            draw.text((x, y), sunset, font=fm, fill=MUTED)
            x += _tw(draw, sunset, fm)
        if daylight_hrs:
            draw.text((x+20, y), f"  ({daylight_hrs} hrs)", font=fm, fill=(78,82,105))
        y += ROW_SUN

    # Extras: pressure · dew · visibility
    if has_extras:
        ep_list = []
        if pressure   is not None: ep_list.append(f"Pressure  {pressure} hPa")
        if dew_point  is not None: ep_list.append(f"Dew  {dew_point}°")
        if visibility is not None: ep_list.append(f"Visibility  {visibility} km")
        x = PAD
        for i, ep in enumerate(ep_list):
            if i > 0:
                draw.text((x, y), "·", font=fm, fill=DOT)
                x += _tw(draw, "·", fm)+14
            draw.text((x, y), ep, font=fm, fill=MUTED)
            x += _tw(draw, ep, fm)+14
        y += ROW_EXTRAS

    # Moon phase
    if moon_phase:
        _MOON_PCT = {
            "new moon":              0,
            "waxing crescent":      12,
            "first quarter":        25,
            "waxing gibbous":       62,
            "full moon":           100,
            "waning gibbous":       75,
            "last quarter":         25,
            "third quarter":        25,
            "waning crescent":      12,
        }
        pct = _MOON_PCT.get(moon_phase.lower().strip())
        pct_str = f"  {pct}%" if pct is not None else ""
        _moon_icon(draw, PAD+10, y+13, r=10)
        draw.text((PAD+28, y), f"{moon_phase}{pct_str}", font=fm, fill=MUTED)
        y += ROW_MOON

    # Footer
    y += 20
    draw.rectangle([PAD, y, W-PAD, y+1], fill=DIVIDER)
    y += 14
    fp = []
    if updated_str: fp.append(updated_str)
    if data_source == "open_meteo_fallback": fp.append("⚠ Showing estimates — live scrape unavailable")
    if fp:
        draw.text((PAD, y), "  ·  ".join(fp), font=fft, fill=(58,60,78))

    buf = BytesIO()
    img.save(buf, format="PNG", optimize=True)
    buf.seek(0)
    return buf


if __name__ == "__main__":
    sample = {
        "temperature":  28,
        "condition":    "Partly Cloudy",
        "area":         "New Delhi",
        "humidity":     68,
        "wind_speed":   14,
        "wind_dir":     "NW",
        "wind_gusts":   22,
        "uv_index":     6,
        "feels_like":   31,
        "aqi":          85,
        "aqi_category": "Moderate",
        "rain_chance":  20,
        "rain_mm":      2.4,
        "high":         33,
        "low":          24,
        "sunrise":      "6:14 AM",
        "sunset":       "6:48 PM",
        "daylight_hrs": 12.4,
        "pressure":     1012,
        "dew_point":    19,
        "visibility":   8.2,
        "moon_phase":   "Waxing Crescent",
        "updated":      "Updated 4 min ago",
    }
    buf = build_weather_card(sample)
    with open("/home/claude/preview_card.png", "wb") as f:
        f.write(buf.read())
    print(f"Saved")