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
        jinja2==3.1.4 \
        aiohttp==3.10.5 \
        python-multipart==0.0.9 \
        river==0.24.2 \
        dtaidistance==2.3.13 \
        paho-mqtt==2.1.0

COPY app /opt/water_monitor/app
COPY rootfs /

LABEL \
    io.hass.name="Water Monitor" \
    io.hass.description="Intelligent water monitoring with fixture signature recognition" \
    io.hass.type="addon" \
    io.hass.version="0.2.0-dev"
