"""
welcome_card.py
───────────────
Generates a static PNG welcome card for new SkyUpdate users.
Returns a BytesIO buffer ready for bot.reply_photo().

Usage:
    from welcome_card import build_welcome_card
    buf = build_welcome_card("Aditya")
    await update.message.reply_photo(photo=buf)

Requires: Pillow (already in requirements.txt)
Fonts:    uses Poppins from src/fonts/
"""

from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
import os
import math

# ── Font resolution ───────────────────────────────────────────────────────────
_BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
_FONTS_DIR = os.path.join(_BASE_DIR, "fonts")

def _fp(filename):
    p = os.path.join(_FONTS_DIR, filename)
    return p if os.path.exists(p) else None

_BOLD = _fp("Poppins-Bold.ttf")    or _fp("Poppins-Medium.ttf")  or _fp("Poppins-Regular.ttf")
_MED  = _fp("Poppins-Medium.ttf")  or _fp("Poppins-Regular.ttf") or _BOLD
_REG  = _fp("Poppins-Regular.ttf") or _fp("Poppins-Light.ttf")   or _MED
_LT   = _fp("Poppins-Light.ttf")   or _REG

def _f(path, size):
    return ImageFont.truetype(path, size)

# ── Palette ───────────────────────────────────────────────────────────────────
SKY_TOP    = (200, 230, 248)
SKY_BOT    = (176, 212, 236)
MTN1       = (176, 204, 223)
MTN2       = (168, 200, 220)
SNOW_GND   = (234, 246, 255)
SNOW_W     = (255, 255, 255)
CLOUD      = (255, 255, 255)
ACCENT     = (74,  159, 196)
TEXT_DARK  = (26,  74,  98)
TEXT_MID   = (58,  122, 156)
TEXT_LIGHT = (122, 170, 191)
TEXT_LBL   = (44,  95,  122)
DIVIDER    = (168, 200, 220)
PEN_BK     = (28,  28,  46)
PEN_WH     = (238, 244, 250)
BEAK_C     = (245, 166, 35)
BLUSH_C    = (255, 179, 179)
SCARF_R    = (231, 76,  60)
SCARF_D    = (192, 57,  43)
STAR_C     = (255, 209, 102)
WAND_C     = (200, 160, 64)
FEET_C     = (245, 166, 35)

W, H = 680, 240


# ── Helpers ───────────────────────────────────────────────────────────────────
def _el(draw, cx, cy, rx, ry, fill):
    draw.ellipse([cx - rx, cy - ry, cx + rx, cy + ry], fill=fill)

def _ci(draw, cx, cy, r, fill):
    _el(draw, cx, cy, r, r, fill)

def _star(draw, cx, cy, r, fill):
    pts = []
    for i in range(10):
        a   = math.radians(i * 36 - 90)
        rad = r if i % 2 == 0 else r * 0.42
        pts.append((cx + rad * math.cos(a), cy + rad * math.sin(a)))
    draw.polygon(pts, fill=fill)

def _rr(draw, x, y, w, h, r, fill):
    draw.rounded_rectangle([x, y, x + w, y + h], radius=r, fill=fill)


# ── Background ────────────────────────────────────────────────────────────────
def _draw_bg(draw):
    draw.rectangle([0, 0, W, H],      fill=SKY_TOP)
    draw.rectangle([0, H//2, W, H],   fill=SKY_BOT)
    draw.polygon([(0,210),(75,120),(150,210)],    fill=MTN1)
    draw.polygon([(530,210),(630,105),(680,210)], fill=MTN1)
    draw.polygon([(410,210),(500,130),(590,210)], fill=MTN2)
    _el(draw, 340, 238, 380, 42, SNOW_GND)
    _el(draw, 340, 232, 330, 28, SNOW_W)
    for cx,cy,rx,ry in [(565,42,58,25),(608,50,40,20),(530,50,34,17)]:
        _el(draw, cx, cy, rx, ry, CLOUD)
    for cx,cy,rx,ry in [(112,34,42,19),(145,40,30,15)]:
        _el(draw, cx, cy, rx, ry, CLOUD)
    draw.rectangle([0, 0, W, 5], fill=ACCENT)


# ── Text ──────────────────────────────────────────────────────────────────────
def _draw_text(draw, name):
    tx = 415
    fl = _f(_MED,  12)
    fg = _f(_BOLD, 36)
    fs = _f(_MED,  14)
    ff = _f(_REG,  13)
    fh = _f(_LT,   11)
    draw.text((tx, 42),  "SKYUPDATE",                        font=fl, fill=TEXT_LBL,   anchor="mm")
    draw.text((tx, 84),  f"Hey, {name}!",                   font=fg, fill=TEXT_DARK,  anchor="mm")
    draw.text((tx, 112), "Your personal weather companion",  font=fs, fill=TEXT_MID,   anchor="mm")
    draw.line([(338,126),(492,126)], fill=DIVIDER, width=1)
    for i, txt in enumerate(["☁  Weather + AQI", "💡  Daily insights"]):
        draw.text((310, 144+i*22), txt, font=ff, fill=TEXT_DARK, anchor="lm")
    for i, txt in enumerate(["⏰  Morning alerts", "📍  Saved locations"]):
        draw.text((428, 144+i*22), txt, font=ff, fill=TEXT_DARK, anchor="lm")
    draw.text((tx, 212), "Share your location below to begin", font=fh, fill=TEXT_LIGHT, anchor="mm")


# ── Penguin ───────────────────────────────────────────────────────────────────
def _draw_penguin(draw):
    cx, cy = 148, 200

    # Snow mound
    _el(draw, cx, 230, 112, 30, SNOW_GND)
    _el(draw, cx, 224,  92, 22, SNOW_W)

    # Body + belly
    _el(draw, cx, cy,     36, 40, PEN_BK)
    _el(draw, cx, cy + 8, 24, 30, PEN_WH)

    # Head + face
    hcy = cy - 68
    _ci(draw, cx, hcy, 34, PEN_BK)
    _el(draw, cx, hcy + 4, 21, 19, PEN_WH)

    # Eyes
    for ex, ey in [(cx-11, hcy-4), (cx+11, hcy-4)]:
        _ci(draw, ex, ey, 8, SNOW_W)
        _el(draw, ex+1, ey+1, 4, 5, PEN_BK)
        _ci(draw, ex-1, ey-1, 2, SNOW_W)

    # Beak
    bky = hcy + 10
    draw.polygon([(cx-6,bky),(cx+6,bky),(cx,bky+9)], fill=BEAK_C)

    # Blush
    for bx in [cx-18, cx+18]:
        _el(draw, bx, hcy+6, 8, 4, (*BLUSH_C, 110))

    # Scarf
    _rr(draw, cx-42, cy-30, 84, 12, 6, SCARF_R)
    draw.line([(cx-36, cy-18), (cx-38, cy-4), (cx-34, cy+10)],
              fill=SCARF_R, width=7)
    draw.line([(cx-34, cy+10), (cx-32, cy+22)],
              fill=SCARF_D, width=6)

    # Left wing
    _el(draw, cx-40, cy-4, 13, 20, PEN_BK)

    # Right wing + wand
    draw.ellipse([cx+31, cy-32, cx+55, cy+8], fill=PEN_BK)
    draw.line([(cx+50, cy-22), (cx+70, cy-58)], fill=WAND_C, width=3)
    _star(draw, cx+70, cy-68, 9, STAR_C)
    for ox, oy, r in [(14,-6,3),(-10,-8,2),(12,6,2)]:
        _ci(draw, cx+70+ox, cy-68+oy, r, STAR_C)

    # Feet
    _el(draw, cx-14, cy+36, 13, 6, FEET_C)
    _el(draw, cx+14, cy+38, 13, 6, FEET_C)


# ── Border ────────────────────────────────────────────────────────────────────
def _draw_border(draw):
    draw.rounded_rectangle([0, 0, W-1, H-1], radius=22,
                            outline=(168,200,220), width=2, fill=None)


# ── Public API ────────────────────────────────────────────────────────────────
def build_welcome_card(first_name: str) -> BytesIO:
    """
    Builds a static PNG welcome card.
    Returns BytesIO — pass directly to reply_photo().
    """
    name = (first_name or "there").strip().title()

    img  = Image.new("RGB", (W, H), SKY_TOP)
    draw = ImageDraw.Draw(img)

    _draw_bg(draw)
    _draw_text(draw, name)
    _draw_penguin(draw)
    _draw_border(draw)

    buf = BytesIO()
    img.save(buf, format="PNG", optimize=True)
    buf.seek(0)
    return buf


if __name__ == "__main__":
    print("Generating welcome card...")
    buf = build_welcome_card("Aditya")
    out = os.path.join(_BASE_DIR, "welcome_preview.png")
    with open(out, "wb") as f:
        f.write(buf.read())
    print(f"Saved → {out}")