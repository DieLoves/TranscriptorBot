FROM ghcr.io/astral-sh/uv:0.12.13@sha256:b485bd65cc2cf1c9a93b3554012c9c3778cf7b1b5fd3d3096ce9e1226c97e1e6 AS uv
FROM denoland/deno:2.9.7@sha256:fa335acdf6b72106eda2cb6a8cb5f4187e7630e357467489db4b2e7352d5e432 AS deno
FROM python:3.12.15-slim-trixie@sha256:05cda9777409a9c3ffddd94a4c476b79f0769a0b4857f0c7ed9226b6800b0d6f AS build

COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --locked --no-dev --no-editable

# Build verified upstream media tools without the distribution's large codec/GUI tree.
FROM python:3.12.15-slim-trixie@sha256:05cda9777409a9c3ffddd94a4c476b79f0769a0b4857f0c7ed9226b6800b0d6f AS media
COPY deploy/debian.sources /etc/apt/sources.list.d/debian.sources
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl xz-utils gcc libc6-dev make nasm \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /source
RUN curl --fail --silent --show-error --location https://ffmpeg.org/releases/ffmpeg-9.0.2.tar.xz -o ffmpeg.tar.xz \
    && echo "8c3850283eb25fa026482078a04051e0be17347b09ef81a0849bec15a96e002e  ffmpeg.tar.xz" | sha256sum -c - \
    && tar -xf ffmpeg.tar.xz --strip-components=1 \
    && ./configure --prefix=/opt/media --disable-doc --disable-debug --disable-network \
       --disable-autodetect --disable-ffplay --disable-avdevice \
    && make -j2 \
    && make install

FROM python:3.12.15-slim-trixie@sha256:05cda9777409a9c3ffddd94a4c476b79f0769a0b4857f0c7ed9226b6800b0d6f
COPY deploy/debian-runtime.sources /etc/apt/sources.list.d/debian.sources
RUN apt-get update \
    && apt-get install -y --no-install-recommends liblzma5=5.8.1-1+deb13u2 ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && python -m pip uninstall --yes pip \
    && groupadd --gid 10001 transcriptbot \
    && useradd --uid 10001 --gid 10001 --no-create-home transcriptbot \
    && install -d -m 0700 -o 10001 -g 10001 /data /work /work/jobs /telegram

COPY --from=media /opt/media/bin/ffmpeg /opt/media/bin/ffprobe /usr/local/bin/
COPY --from=deno /usr/bin/deno /usr/local/bin/deno
COPY --from=build /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DENO_DIR=/tmp/deno
WORKDIR /app
USER 10001:10001
ENTRYPOINT ["transcriptbot"]
CMD ["bot"]
