ARG BUILD_FROM
FROM ${BUILD_FROM}

ENV LANG=C.UTF-8 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apk add --no-cache python3 py3-pip \
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
