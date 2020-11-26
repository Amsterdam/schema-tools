# Start runtime image,
FROM amsterdam/python:3.8-buster

WORKDIR /app
COPY . ./
RUN pip install -e .

USER datapunt
CMD ["sh .jenkins/test.sh"]
