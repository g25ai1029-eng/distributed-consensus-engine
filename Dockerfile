FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y gcc libffi-dev && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

RUN mkdir -p /data /keys

RUN KEY_DIR=/keys python src/crypto_utils.py

EXPOSE 5000

CMD ["python", "src/node.py"]
