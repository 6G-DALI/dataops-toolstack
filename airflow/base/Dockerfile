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
#
# Installed under Airflow's own constraints file, so adding a package here cannot
# move a dependency Airflow pins (numpy, scikit-learn, pydantic, ...) off the
# version it was tested with. The file is fetched at build time, as PyPI is.
# A requirement that contradicts a constraint fails the build rather than
# silently overriding it.
ARG AIRFLOW_VERSION
COPY requirements.txt /requirements.txt
RUN PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')" \
 && pip install --no-cache-dir -r /requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# --- OS runtime libraries -------------------------------------------------------
# u8darts imports LightGBM at `from darts.models import ...`, and LightGBM loads
# the OpenMP runtime, which the Airflow image does not ship: without it the Darts
# imputation engine (and darts/kalman with it) dies with
# `OSError: libgomp.so.1: cannot open shared object file`.
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends libgomp1 \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*
USER airflow
