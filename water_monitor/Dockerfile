# ── base image ─────────────────────────────────────────────────────────────
# A plain FROM, per the Supervisor's build.yaml migration: base images are
# published as multi-platform manifests now, so buildx picks the right one from
# the --platform it is given and a per-arch map is unnecessary. build.yaml is
# gone; this line is what replaced its `build_from:` block.
#
# Pinned rather than floating on :bookworm so the Debian release — and with it
# the Python minor version, and therefore which manylinux wheels pip resolves —
# cannot move under the add-on without a deliberate rebuild.
#
# Debian rather than Alpine is a dependency decision, not a taste one:
# scikit-learn publishes NO musllinux wheel at any version (every release
# through 1.9.0 is manylinux-only), and Alpine's own py3-scikit-learn is built
# against a py3-numpy too old to satisfy river's `numpy>=2.3.4` floor. On musl
# those two requirements are mutually exclusive and the model tier is
# unreachable by construction; on glibc every piece resolves from a pinned
# prebuilt wheel.
#
# amd64 and aarch64 only — that is the whole of what this manifest carries, and
# it is why config.yaml no longer lists armhf/armv7/i386. Home Assistant
# stopped publishing 32-bit Debian bases after bookworm-2025.11.1, and neither
# scikit-learn nor river ships a wheel for those architectures on any libc, so
# the cluster engine could not run there either.
FROM ghcr.io/home-assistant/base-debian:bookworm-2026.08.0

ENV LANG=C.UTF-8 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# scikit-learn powers the dev47 TinyModel tier (47b). On this Debian base it
# installs from a pinned manylinux wheel like everything else — no compiler,
# no apk/pip split, no ABI negotiation. Three earlier shapes failed on Alpine
# and are recorded here so they are not retried:
#
#  1. `pip install scikit-learn` on musl -> no wheel exists -> pip fetches the
#     sdist and tries to compile -> no compiler in the base -> BUILD FAILS.
#  2. `apk add py3-scikit-learn` -> installs, but Alpine builds it against
#     Alpine's py3-numpy (1.25.2 on 3.20), which cannot satisfy river's
#     `numpy>=2.3.4`. pip then replaces numpy underneath it and the compiled
#     extensions no longer load: 'numpy.dtype size changed'.
#  3. `pip --only-binary=:all:` on musl -> resolves nothing, tier simply absent.
#
# It is still ALLOWED TO FAIL. armv7/armhf/i386 have no scikit-learn wheel on
# any libc (river has none there either), so on those arches the tier stays
# off and `tinymodel.sklearn_available()` reports it — the kNN ladder serves
# exactly as before dev47. A missing optional tier must never cost the add-on.
#
# Do NOT add gcc/g++/gfortran to force a source build: many minutes and a much
# larger image, for a tier designed to be optional. And do NOT substitute
# river's own classifiers — ARF/Hoeffding were measured against this home's
# 848 labels and lose ~10 points on the refit-on-cadence arm that ships.
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-pip \
 && rm -rf /var/lib/apt/lists/* \
 && pip3 install --no-cache-dir --break-system-packages \
        fastapi==0.115.0 \
        uvicorn==0.30.6 \
        websockets==13.1 \
        jinja2==3.1.6 \
        aiohttp==3.10.11 \
        python-multipart==0.0.22 \
        river==0.24.2 \
        dtaidistance==2.3.13 \
        paho-mqtt==2.1.0 \
 && { pip3 install --no-cache-dir --break-system-packages --only-binary=:all: \
        scikit-learn==1.7.2 && echo 'SKLEARN: installed from manylinux wheel' \
      || echo 'SKLEARN: no wheel for this arch - TinyModel tier stays off'; }

# Say in the BUILD LOG whether the tier will actually work. Installing a
# package is not the same as being able to import it — an ABI mismatch shows
# up only at import, which is how the Alpine build looked healthy while the
# tier was dead. Non-fatal: this reports, it never gates. main.py logs the
# same probe at boot, and that is the one you can actually read, because the
# Supervisor only dumps build output when a build FAILS.
RUN python3 -c "import sklearn, numpy, river; print('SKLEARN: import OK', sklearn.__version__, 'numpy', numpy.__version__, 'river', river.__version__)" \
 || echo 'SKLEARN: NOT IMPORTABLE - TinyModel tier stays off, kNN ladder serves'

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
