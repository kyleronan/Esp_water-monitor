"""Generate esp32s3_wroom1_pinout.png — the project pin-reference diagram.

Edit the LEFT / RIGHT / BOTTOM pin lists below and re-run:

    py -3.11 docs/hardware/images/generate_pinout.py

History:
  2026-07-26  Irrigation relay coils corrected: GPIO21 = open, GPIO14 = close
              (pre-3.13.1 firmware and the old diagram had them swapped).
"""
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, Rectangle

# ----------------------------------------------------------------------------
# Categories: (fill color, text color)
# ----------------------------------------------------------------------------
CATS = {
    "gnd":      ("#1a1a1a", "white"),
    "pwr":      ("#8b1a1a", "white"),
    "btn":      ("#ead9b0", "#8a6d3b"),
    "led":      ("#b0184f", "white"),
    "endstop":  ("#a07000", "white"),
    "usb":      ("#0d7a86", "white"),
    "un":       ("#e4e4e4", "#888888"),
    "pressure": ("#1a7d2e", "white"),
    "j14":      ("#444a52", "white"),
    "flow":     ("#0a5bd3", "white"),
    "psram":    ("#d8d8d8", "#777777"),
    "spi":      ("#cc3311", "white"),
    "relay":    ("#5b3fa8", "white"),
}

# (label, category, dashed_connector)
LEFT = [
    ("GND · pin 1", "gnd", False),
    ("3V3 · power", "pwr", False),
    ("EN · enable btn", "btn", True),
    ("GPIO04 · LED close Main", "led", False),
    ("GPIO05 · LED close Irr.", "led", False),
    ("GPIO06 · LED open Irr.", "led", False),
    ("GPIO07 · LED open Main", "led", False),
    ("GPIO15 · Endstop close Main", "endstop", False),
    ("GPIO16 · Endstop open Main", "endstop", False),
    ("GPIO17 · Endstop close Irr.", "endstop", False),
    ("GPIO18 · Endstop open Irr.", "endstop", False),
    ("GPIO08", "un", True),
    ("GPIO19 · USB D−", "usb", False),
    ("GPIO20 · USB D+", "usb", False),
]

RIGHT = [
    ("GND · pin 40", "gnd", False),
    ("GPIO01 · Pressure Main", "pressure", False),
    ("GPIO02 · Pressure Irr.", "pressure", False),
    ("GPIO43 · TXD0", "un", True),
    ("GPIO44 · RXD0", "un", True),
    ("GPIO42 · J14 Pin 3", "j14", False),
    ("GPIO41 · J14 Pin 2", "j14", False),
    ("GPIO40 · J14 Pin 1", "j14", False),
    ("GPIO39 · Flow Rate Main", "flow", False),
    ("GPIO38 · Flow Rate Irr.", "flow", False),
    ("GPIO37 · PSRAM", "psram", True),
    ("GPIO36 · PSRAM", "psram", True),
    ("GPIO35 · PSRAM", "psram", True),
    ("GPIO00 · BOOT btn", "btn", True),
]

# Bottom row is ordered by physical pin position, left to right.
# 2026-07-26 correction: GPIO14 = Relay close Irr., GPIO21 = Relay open Irr.
BOTTOM = [
    ("GPIO03", "un", True),
    ("GPIO46", "un", True),
    ("GPIO09", "un", True),
    ("GPIO10 · SPI CS", "spi", False),
    ("GPIO11 · SPI CS", "spi", False),
    ("GPIO12 · SPI CLK", "spi", False),
    ("GPIO13 · SPI MISO", "spi", False),
    ("GPIO14 · Relay close Irr.", "relay", False),
    ("GPIO21 · Relay open Irr.", "relay", False),
    ("GPIO47 · Relay close Main", "relay", False),
    ("GPIO48 · Relay open Main", "relay", False),
    ("GPIO45", "un", True),
]

LEGEND = [
    ("Pressure", "pressure"),
    ("Flow Rate", "flow"),
    ("End stops", "endstop"),
    ("LEDs", "led"),
    ("Relays", "relay"),
    ("SPI", "spi"),
    ("USB D±", "usb"),
    ("J14", "j14"),
    ("PSRAM / sys", "psram"),
    ("Board btn", "btn"),
]

FOOTER = ("GPIO35–37 connected to PSRAM internally · dashed = "
          "unassigned or internal · Irr. = Irrigation")


def pill(ax, x, y, w, h, label, cat, ha="center", fontsize=8.6):
    fill, tc = CATS[cat]
    ax.add_patch(FancyBboxPatch(
        (x, y), w, h,
        boxstyle="round,pad=0.002,rounding_size=0.008",
        linewidth=0, facecolor=fill, mutation_aspect=1.0))
    ax.text(x + w / 2, y + h / 2, label, ha="center", va="center",
            fontsize=fontsize, color=tc, family="DejaVu Sans")


def main():
    fig = plt.figure(figsize=(9.0, 8.8), dpi=100)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    chip_l, chip_r = 0.305, 0.665
    chip_t, chip_b = 0.865, 0.345

    # Keepout zone (antenna)
    ax.add_patch(Rectangle((chip_l - 0.01, chip_t), chip_r - chip_l + 0.02,
                           0.075, facecolor="white", edgecolor="#bbbbbb",
                           hatch="///", linewidth=1.0))
    ax.text((chip_l + chip_r) / 2, chip_t + 0.037,
            "Keepout zone — PCB trace antenna", ha="center", va="center",
            fontsize=8.5, color="#888888")

    # Chip body
    ax.add_patch(Rectangle((chip_l, chip_b), chip_r - chip_l, chip_t - chip_b,
                           facecolor="#f4f4f4", edgecolor="#aaaaaa",
                           linestyle=(0, (4, 3)), linewidth=1.2))
    ax.add_patch(FancyBboxPatch((0.40, 0.63), 0.17, 0.045,
                                boxstyle="round,pad=0.004",
                                facecolor="#e9e9e9", edgecolor="#bbbbbb"))
    ax.text(0.485, 0.6525, "ESP32-S3-WROOM-1", ha="center", va="center",
            fontsize=9.5, color="#333333", weight="bold")

    # Legend
    lx = 0.035
    for label, cat in LEGEND:
        fill, _ = CATS[cat]
        ax.add_patch(Rectangle((lx, 0.968), 0.011, 0.014, facecolor=fill,
                               linewidth=0))
        ax.text(lx + 0.016, 0.975, label, ha="left", va="center", fontsize=8.3,
                color="#222222")
        lx += 0.016 + 0.0088 * len(label) + 0.016

    ph, gap = 0.0245, 0.0122
    step = ph + gap

    # Left column
    y0 = chip_t - 0.035
    for i, (label, cat, dashed) in enumerate(LEFT):
        y = y0 - i * step
        pill(ax, 0.035, y - ph / 2, 0.225, ph, label, cat)
        ls = (0, (2, 2)) if dashed else "solid"
        ax.plot([0.262, chip_l], [y, y], color="#999999", lw=1.0,
                linestyle=ls, zorder=1)
        ax.add_patch(Rectangle((chip_l - 0.009, y - 0.005), 0.009, 0.010,
                               facecolor="#333333", linewidth=0))

    # Right column
    for i, (label, cat, dashed) in enumerate(RIGHT):
        y = y0 - i * step
        pill(ax, 0.740, y - ph / 2, 0.225, ph, label, cat)
        ls = (0, (2, 2)) if dashed else "solid"
        ax.plot([chip_r, 0.738], [y, y], color="#999999", lw=1.0,
                linestyle=ls, zorder=1)
        ax.add_patch(Rectangle((chip_r, y - 0.005), 0.009, 0.010,
                               facecolor="#333333", linewidth=0))

    # Bottom row (vertical pills)
    n = len(BOTTOM)
    span = chip_r - chip_l + 0.03
    x0 = chip_l - 0.015 + span / (2 * n)
    pw = 0.026
    for i, (label, cat, dashed) in enumerate(BOTTOM):
        x = x0 + i * (span / n)
        fill, tc = CATS[cat]
        ax.add_patch(FancyBboxPatch(
            (x - pw / 2, 0.075), pw, 0.235,
            boxstyle="round,pad=0.002,rounding_size=0.008",
            linewidth=0, facecolor=fill))
        ax.text(x, 0.075 + 0.235 / 2, label, ha="center", va="center",
                fontsize=8.2, color=tc, rotation=90, family="DejaVu Sans")
        ls = (0, (2, 2)) if dashed else "solid"
        ax.plot([x, x], [0.312, chip_b], color="#999999", lw=1.0,
                linestyle=ls, zorder=1)
        ax.add_patch(Rectangle((x - 0.005, chip_b - 0.009), 0.010, 0.009,
                               facecolor="#333333", linewidth=0))

    # Footer
    ax.plot([0.03, 0.97], [0.045, 0.045], color="#dddddd", lw=1.0)
    ax.text(0.5, 0.028, FOOTER, ha="center", va="center", fontsize=8.3,
            color="#888888")

    out = Path(__file__).with_name("esp32s3_wroom1_pinout.png")
    fig.savefig(out, dpi=100, facecolor="white")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
