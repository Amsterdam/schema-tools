# Start runtime image,
FROM amsterdam/python:3.9-buster

WORKDIR /app
COPY . ./
RUN pip install -e ".[django,tests]"

USER datapunt
