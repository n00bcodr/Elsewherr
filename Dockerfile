FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /usr/local/bin/uv

WORKDIR /usr/src/app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV SCHEDULE=None

RUN apt-get update && apt-get install -y --no-install-recommends cron && rm -rf /var/lib/apt/lists/*

COPY /requirements.txt .
RUN uv pip install --system --no-cache -r requirements.txt

COPY . .

CMD ["sh", "/usr/src/app/run.sh"]

