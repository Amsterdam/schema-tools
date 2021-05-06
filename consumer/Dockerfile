# WIP: image to be used for the Kafka producer
# Needs to be tested in the Azure AKS cluster
FROM amsterdam/python:3.8-buster

WORKDIR /app

# For local testing of a not yet published schematools library
# copy the wheel into the container and install it

# COPY amsterdam_schema_tools-*.whl .
# RUN pip install "$(ls amsterdam_schema_tools-*.whl)[kafka]"

RUN pip install amsterdam-schema-tools[kafka]

CMD ["sleep", "infinity"]

USER datapunt
