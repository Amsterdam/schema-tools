# Start runtime image,
FROM amsterdam/python:3.9-slim-buster

RUN apt update
RUN apt-get update \
 && apt-get autoremove -y \
 && apt-get install --no-install-recommends -y \
        libpq-dev \
        python-dev \
        gcc \
        git

WORKDIR /app
COPY . ./
RUN pip install -e ".[django,tests]"
# So we can use local schemas
RUN git clone https://github.com/Amsterdam/amsterdam-schema.git /tmp/ams-schema

USER datapunt
