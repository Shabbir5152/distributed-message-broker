# syntax=docker/dockerfile:1
FROM python:3.10-slim

WORKDIR /app

# 1. Copy ONLY the dependencies first. 
# Docker layer caching will perfectly freeze this step so long as requirements.txt doesn't change.
COPY requirements.txt .

# 2. Use BuildKit's cache mount.
# This ensures that even if you DO change requirements.txt, Python doesn't have to re-download 
# the wheel packages from the internet—it pulls them instantly from the local builder cache.
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --default-timeout=1000 -r requirements.txt

# 3. Copy the rest of the application code
COPY . .
