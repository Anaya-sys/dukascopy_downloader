"""
Color + font palette ported 1:1 from the original Bloomberg-style HTML terminal.
All radii are 0 to keep the sharp, dense "terminal" look in CustomTkinter.
"""

# --- Palette (matches the CSS :root variables exactly) ---
BG0 = "#030303"   # deepest background
BG1 = "#0a0a0a"   # panel background
BG2 = "#111111"   # header / bars
BG3 = "#181818"   # hover / active
BORDER = "#222222"
BORDER_HI = "#2e2e2e"

GREEN = "#00e676"
GREEN_DIM = "#007a3d"
AMBER = "#ffb300"
AMBER_DIM = "#7a5200"
RED = "#f44336"
RED_DIM = "#7a1e18"
CYAN = "#00bcd4"

WHITE = "#e8e8e8"
MUTED = "#888888"
LABEL = "#555555"

# --- Fonts (tuples; built lazily once a Tk root exists) ---
MONO = "Courier New"
SANS = "Arial"


def f_mono(size=11, weight="normal"):
    return (MONO, size, weight)


def f_sans(size=11, weight="normal"):
    return (SANS, size, weight)


# Fixed geometry for the static 800x600 window
WIN_W = 800
WIN_H = 600

TOPBAR_H = 28
MENUBAR_H = 36
TICKER_H = 24
WORKSPACE_H = WIN_H - TOPBAR_H - MENUBAR_H - TICKER_H  # 524

COL_LEFT_W = 234
COL_CENTER_W = WIN_W - COL_LEFT_W  # 570  (right panel removed)