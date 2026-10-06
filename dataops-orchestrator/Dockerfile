FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

# Write stdout/stderr straight through instead of buffering it. Without this, print() and log
# output can sit in Python's buffer for a long time when not attached to a terminal, so errors
# (e.g. a failing piveau call) never show up in `docker logs` when you need them.
ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
