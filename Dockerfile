FROM python:3.12-alpine
# uvicorn[standard] brings the websockets implementation; without it /ws answers 404.
RUN pip install --no-cache-dir fastapi==0.115.6 "uvicorn[standard]==0.34.0" httpx==0.28.1 tzdata==2025.2
WORKDIR /srv
COPY app.py hermes.py store.py tiles.py ./
ENV BRIGHTHERMES_DIR=/data
VOLUME /data
EXPOSE 8650
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8650", "--ws-ping-interval", "25", "--ws-ping-timeout", "20"]
