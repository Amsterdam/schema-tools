# Start runtime image,
FROM amsterdam/python:3.8-buster

WORKDIR /app
COPY . ./
RUN pip install -e .
# Install django dependencies
RUN pip install -e ".[django]"
# Install test dependencies
RUN pip install -e ".[tests]"

USER datapunt
