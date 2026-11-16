# PyPOTS imputation service: PyPOTS (CPU torch) with the project's runner, plus the HTTP service the Airflow DAG calls.
# The runner is WaveStitchPlus/dockers/tools/PyPOTS_app/run_imputation.py; build.sh copies it into ./runner first.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1

# torch from the CPU index (about 200 MB; the default wheel brings CUDA libraries the service does not use), then PyPOTS.
RUN pip install torch==2.8.0 --index-url https://download.pytorch.org/whl/cpu
COPY runner/requirements.txt /srv/runner-requirements.txt
RUN grep -viE '^torch' /srv/runner-requirements.txt > /srv/rest.txt && pip install -r /srv/rest.txt
COPY requirements.txt /srv/requirements.txt
RUN pip install -r /srv/requirements.txt

COPY runner/run_imputation.py /app/PyPOTS_app/run_imputation.py
COPY app.py /srv/app.py

# Runs as the Airflow user (uid 50000, group 0) so the files it writes on the shared volume are the worker's to read.
ENV HOME=/tmp \
    PYPOTS_WORK_ROOT=/opt/airflow/wsp-work \
    PYPOTS_APP_DIR=/app/PyPOTS_app
RUN mkdir -p /opt/airflow/wsp-work \
 && chgrp -R 0 /app /opt/airflow/wsp-work \
 && chmod -R g+rwX /app /opt/airflow/wsp-work
USER 50000:0

WORKDIR /srv
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=5 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=4).status == 200 else 1)"
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
