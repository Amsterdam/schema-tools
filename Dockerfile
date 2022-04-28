FROM amsterdam/python:3.9-buster

WORKDIR /app
COPY . ./
RUN pip install -e ".[django,tests]"
# So we can use local schemas
RUN git clone https://github.com/Amsterdam/amsterdam-schema.git /tmp/ams-schema

USER datapunt
