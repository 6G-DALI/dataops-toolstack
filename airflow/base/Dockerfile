# syntax=docker/dockerfile:1
#
# Custom Airflow image for 6G-DALI DataOps.
#
# Extends the official Apache Airflow image and pre-installs the Python packages
# that were previously injected at runtime through _PIP_ADDITIONAL_REQUIREMENTS.
# Baking them into the image means every Airflow container (worker, scheduler,
# apiserver, …) starts faster and does not need PyPI access at boot.
#
# Keep AIRFLOW_VERSION in sync with the base image used in docker-compose.yaml.
# The CI workflow also uses this value to tag the published image.

ARG AIRFLOW_VERSION=3.1.8
FROM apache/airflow:${AIRFLOW_VERSION}

# --- OS-level build dependencies (uncomment if a package needs compiling) -----
# USER root
# RUN apt-get update \
#     && apt-get install -y --no-install-recommends build-essential \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/*
# USER airflow

# --- Python packages ----------------------------------------------------------
# Install as the unprivileged "airflow" user (the image's default user), which
# is the supported way to extend the official image.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt