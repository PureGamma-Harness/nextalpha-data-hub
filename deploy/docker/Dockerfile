FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY pyproject.toml README.md requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

COPY src /app/src
RUN pip install --no-cache-dir --no-deps .

CMD ["nextalpha-watcher", "run", "--config", "/app/config.yaml"]
