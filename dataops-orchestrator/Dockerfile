FROM python:3.13-slim

WORKDIR /app

# MinIO client (copied from the official image: the dl.min.io download URL answered 410
# to BuildKit): the testbed registry uses it to create per-testbed users and
# policies on the Data Lake (boto3 cannot do MinIO admin calls).
COPY --from=minio/mc:latest /usr/bin/mc /usr/local/bin/mc

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

# Write stdout/stderr straight through instead of buffering it. Without this, print() and log
# output can sit in Python's buffer for a long time when not attached to a terminal, so errors
# (e.g. a failing piveau call) never show up in `docker logs` when you need them.
ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
