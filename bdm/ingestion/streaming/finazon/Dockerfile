FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY websocket_client.py .

CMD ["python", "websocket_client.py", "--help"]
