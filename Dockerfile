# The image is meant to be used as an executable that invokes pytest
# on schematools by default.
# To test schematools against the DSO-API, run:
# docker run <image_name> /app/dso-api/src
FROM python:3.11-slim-bullseye AS builder
RUN apt update && apt install --no-install-recommends -y \
    build-essential \
    libgeos-dev \
    libpq-dev \
    git

RUN git clone https://github.com/Amsterdam/dso-api /app/dso-api
# Dependencies are resolved during requirements compilation
RUN pip install --no-deps -r /app/dso-api/src/requirements.txt --no-cache-dir

# So we can use local schemas
RUN git clone https://github.com/Amsterdam/amsterdam-schema.git /app/ams-schema

FROM python:3.11-slim-bullseye
RUN apt update && apt install --no-install-recommends -y \
    curl \
    libgdal28 \
    libgeos-c1v5 \
    libpq5 \
    media-types \
    netcat-openbsd

# Copy python build artifacts from builder image
COPY --from=builder /usr/local/bin/ /usr/local/bin/
COPY --from=builder /usr/local/lib/python3.11/site-packages/ /usr/local/lib/python3.11/site-packages/
COPY --from=builder /app/dso-api /app/dso-api
COPY --from=builder /app/ams-schema /app/ams-schema

COPY . /app/schema-tools
RUN pip install -e /app/schema-tools[tests] --no-cache-dir


ENV DSO_STATIC_DIR=/static
WORKDIR /app/schema-tools

ENTRYPOINT ["pytest"]
