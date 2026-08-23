ARG BUILD_FROM
FROM ${BUILD_FROM}

ENV LANG=C.UTF-8 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# scikit-learn powers the dev47 TinyModel tier (47b). It is installed from
# ALPINE'S PACKAGE, before pip, and is ALLOWED TO FAIL. Three things forced
# that shape, all learned the hard way on this base image:
#
#  1. scikit-learn ships no musllinux wheel. Asking pip for it makes pip fetch
#     the 7 MB sdist and try to compile it; the HA base has no compiler, so the
#     IMAGE BUILD FAILS. (river and aiohttp install fine because they DO ship
#     musllinux wheels — that is the whole difference.)
#  2. apk must run BEFORE pip. py3-scikit-learn pulls py3-numpy/py3-scipy, and
#     pip then satisfies river's numpy requirement from those instead of
#     installing a second, conflicting copy into the same site-packages.
#  3. `|| true` is deliberate. The tier is optional at runtime —
#     `tinymodel.sklearn_available()` reports it unavailable and the kNN ladder
#     serves exactly as before dev47 — so a missing package must cost the
#     feature, never the add-on.
#
# Do NOT "fix" this by adding gcc/g++/gfortran to force a source build: that is
# a many-minute compile and a much larger image, for a tier designed to be
# optional.
RUN apk add --no-cache python3 py3-pip \
 && { apk add --no-cache py3-scikit-learn || echo 'py3-scikit-learn unavailable: TinyModel tier stays off, kNN ladder serves'; } \
 && pip3 install --no-cache-dir --break-system-packages \
        fastapi==0.115.0 \
        uvicorn==0.30.6 \
        websockets==13.1 \
        jinja2==3.1.6 \
        aiohttp==3.10.11 \
        python-multipart==0.0.22 \
        river==0.24.2 \
        dtaidistance==2.3.13 \
        paho-mqtt==2.1.0

# dev46: config.yaml must reach the image, not just the build context. Both
# the boot version line (46g) and the static-asset cache-buster read the
# `version:` field from here at runtime; without it they fall back to
# "unknown"/"dev", which made the boot line useless for verifying a deploy AND
# pinned every asset URL to ?v=dev — so browsers kept serving cached CSS/JS
# across upgrades.
COPY config.yaml /opt/water_monitor/config.yaml
COPY app /opt/water_monitor/app
COPY rootfs /

LABEL \
    io.hass.name="Water Monitor" \
    io.hass.description="Intelligent water monitoring with fixture signature recognition" \
    io.hass.type="addon" \
    io.hass.version="0.3.0"
