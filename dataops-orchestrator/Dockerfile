# MinIO client, built from source. MinIO has withdrawn its prebuilt artifacts (the dl.min.io
# download answers 410, minio/mc is gone from Docker Hub and quay.io needs auth), but the
# module is still on the Go proxy. Pinned for reproducible builds. The testbed registry uses
# it to create per-testbed users and policies on the Data Lake (boto3 cannot do MinIO admin
# calls).
FROM golang:1.25 AS mc
RUN CGO_ENABLED=0 GOBIN=/out go install github.com/minio/mc@v0.0.0-20251106162529-77f82e18b540

FROM python:3.13-slim

WORKDIR /app

COPY --from=mc /out/mc /usr/local/bin/mc

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

# Write stdout/stderr straight through instead of buffering it. Without this, print() and log
# output can sit in Python's buffer for a long time when not attached to a terminal, so errors
# (e.g. a failing piveau call) never show up in `docker logs` when you need them.
ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
