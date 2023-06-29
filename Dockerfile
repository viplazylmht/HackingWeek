FROM tiangolo/uvicorn-gunicorn-fastapi:python3.10-slim

LABEL maintainer="Duy Ha <viplazylmht@gmail.com>"

RUN pip install --no-cache-dir pymongo

COPY ./app /app
