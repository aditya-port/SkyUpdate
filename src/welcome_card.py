"""
welcome_card.py
───────────────
Generates an animated GIF welcome card for new SkyUpdate users.
Returns a BytesIO buffer ready for bot.reply_animation().

Usage:
    from welcome_card import build_welcome_card
    buf = build_welcome_card("Aditya")
    await update.message.reply_animation(animation=buf)

Requires: Pillow (already in requirements.txt)
Fonts:    uses Poppins from src/fonts/
"""

from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
import os
import math
import random

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

W, H        = 680, 240
FRAMES      = 36
FRAME_MS    = 55
BLINK_START = 28
BLINK_END   = 32

# ── Fixed snowflake seeds ─────────────────────────────────────────────────────
random.seed(42)
FLAKES = [
    {
        "x":     random.randint(280, 670),
        "speed": random.uniform(4.5, 8.0),
        "r":     random.uniform(1.8, 3.2),
        "phase": random.uniform(0, FRAMES),
        "drift": random.uniform(-0.4, 0.4),
    }
    for _ in range(22)
]


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
    fl  = _f(_MED,  12)
    fg  = _f(_BOLD, 36)
    fs  = _f(_MED,  14)
    ff  = _f(_REG,  13)
    fh  = _f(_LT,   11)
    draw.text((tx, 42),  "SKYUPDATE",                        font=fl, fill=TEXT_LBL,   anchor="mm")
    draw.text((tx, 84),  f"Hey, {name}!",                   font=fg, fill=TEXT_DARK,  anchor="mm")
    draw.text((tx, 112), "Your personal weather companion",  font=fs, fill=TEXT_MID,   anchor="mm")
    draw.line([(338,126),(492,126)], fill=DIVIDER, width=1)
    for i, txt in enumerate(["☁  Weather + AQI","💡  Daily insights"]):
        draw.text((310, 144+i*22), txt, font=ff, fill=TEXT_DARK, anchor="lm")
    for i, txt in enumerate(["⏰  Morning alerts","📍  Saved locations"]):
        draw.text((428, 144+i*22), txt, font=ff, fill=TEXT_DARK, anchor="lm")
    draw.text((tx, 212), "Share your location below to begin", font=fh, fill=TEXT_LIGHT, anchor="mm")


# ── Snowflakes ────────────────────────────────────────────────────────────────
def _draw_snow(draw, frame):
    for flake in FLAKES:
        y = ((flake["phase"] + frame * flake["speed"]) % (H + 20)) - 10
        x = flake["x"] + flake["drift"] * frame
        _ci(draw, round(x), round(y), round(flake["r"]), SNOW_W)


# ── Penguin ───────────────────────────────────────────────────────────────────
def _draw_penguin(draw, frame):
    cx, cy = 148, 200
    t = frame / FRAMES
    wave = math.sin(t * 2 * math.pi)

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

    # Eyes with blink
    eye_sy = 1.0
    if BLINK_START <= frame <= BLINK_END:
        p = (frame - BLINK_START) / (BLINK_END - BLINK_START)
        eye_sy = max(0.08, 1.0 - math.sin(p * math.pi) * 0.92)

    for ex, ey in [(cx-11, hcy-4), (cx+11, hcy-4)]:
        _ci(draw, ex, ey, 8, SNOW_W)
        ry = max(1, round(5 * eye_sy))
        _el(draw, ex+1, ey+1, 4, ry, PEN_BK)
        if eye_sy > 0.3:
            _ci(draw, ex-1, ey-1, 2, SNOW_W)

    # Beak
    bky = hcy + 10
    draw.polygon([(cx-6,bky),(cx+6,bky),(cx,bky+9)], fill=BEAK_C)

    # Blush
    for bx in [cx-18, cx+18]:
        _el(draw, bx, hcy+6, 8, 4, (*BLUSH_C, 110))

    # Scarf base
    _rr(draw, cx-42, cy-30, 84, 12, 6, SCARF_R)

    # Flying scarf tail
    cp_x  = cx - 36 + wave * 10
    cp_y  = cy - 18 + 16 + wave * 4
    end_x = cx - 32 + wave * 14
    end_y = cy - 18 + 32 + wave * 6
    end2x = end_x + wave * 6
    end2y = end_y + 14
    draw.line([(cx-36, cy-18), (cp_x, cp_y), (end_x, end_y)],
              fill=SCARF_R, width=7)
    draw.line([(end_x, end_y), (end2x, end2y)],
              fill=SCARF_D, width=6)

    # Left wing
    _el(draw, cx-40, cy-4, 13, 20, PEN_BK)

    # Right wing waving
    wa = math.radians(wave * 22)
    wx = cx + 36 + math.cos(wa) * 6
    wy = cy - 14
    draw.ellipse([wx-5, wy-18, wx+19, wy+6], fill=PEN_BK)

    # Wand
    wx1 = cx + 48 + wave * 5
    wy1 = cy - 22 + wave * 3
    wx2 = cx + 68 + wave * 7
    wy2 = cy - 58 + wave * 4
    draw.line([(wx1,wy1),(wx2,wy2)], fill=WAND_C, width=3)

    # Star at wand tip
    sp = 1.0 + 0.25 * math.sin(t * 4 * math.pi)
    _star(draw, wx2, wy2-10, round(9*sp), STAR_C)

    # Sparkles
    for i,(ox,oy,br) in enumerate([(14,-6,3.5),(-10,-8,2.5),(12,6,2.8),(-4,-14,2)]):
        phase = t * 2 * math.pi + i * 1.5
        r = max(0.5, br * abs(math.sin(phase)))
        _ci(draw, round(wx2+ox), round(wy2-10+oy), round(r), STAR_C)

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
    Builds an animated GIF welcome card.
    Returns BytesIO — pass directly to reply_animation().
    """
    name   = (first_name or "there").strip().title()
    frames = []

    for f_idx in range(FRAMES):
        img  = Image.new("RGB", (W, H), SKY_TOP)
        draw = ImageDraw.Draw(img)
        _draw_bg(draw)
        _draw_snow(draw, f_idx)
        _draw_text(draw, name)
        _draw_penguin(draw, f_idx)
        _draw_border(draw)
        frames.append(img.convert("P", palette=Image.ADAPTIVE, colors=128))

    buf = BytesIO()
    frames[0].save(
        buf,
        format="GIF",
        save_all=True,
        append_images=frames[1:],
        duration=FRAME_MS,
        loop=0,
        optimize=False,
    )
    buf.seek(0)
    return buf


if __name__ == "__main__":
    print("Generating welcome card...")
    buf = build_welcome_card("Aditya")
    out = os.path.join(_BASE_DIR, "welcome_preview.gif")
    with open(out, "wb") as f:
        f.write(buf.read())
    print(f"Saved → {out}")