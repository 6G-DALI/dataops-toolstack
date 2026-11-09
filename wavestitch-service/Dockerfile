# WaveStitch+ imputation service: the WaveStitch+ CPU image (torch, pykeops, the app runners) plus the HTTP service the
# Airflow DAG calls. Built on top of the project's own image, which is built from the WaveStitchPlus repo:
#
#     ./build.sh            # builds wavestitchplus-cpu:latest, then sparkworks/wavestitch:dev from it
#
ARG BASE=wavestitchplus-cpu:latest
FROM ${BASE}

COPY requirements.txt /srv/requirements.txt
RUN pip install --no-cache-dir -r /srv/requirements.txt

COPY app.py /srv/app.py

# Runs as the Airflow user (uid 50000, group 0) so the files it writes on the shared volume are the worker's to read.
# The runners write caches under HOME and, relative to /app, scratch directories; give group 0 write access to both.
ENV HOME=/tmp \
    WSP_WORK_ROOT=/opt/airflow/wsp-work \
    WSP_APP_DIR=/app/WaveStitchPlus_app \
    PYTHONUNBUFFERED=1
RUN mkdir -p /opt/airflow/wsp-work \
 && chgrp -R 0 /app /opt/airflow/wsp-work /airflow \
 && chmod -R g+rwX /app /opt/airflow/wsp-work /airflow
USER 50000:0

WORKDIR /srv
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=5 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=4).status == 200 else 1)"
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
