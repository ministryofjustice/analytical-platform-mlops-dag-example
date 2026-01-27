#checkov:skip=CKV_DOCKER_2: HEALTHCHECK not required
FROM ghcr.io/ministryofjustice/analytical-platform-airflow-python-base:1.24.1@sha256:ce3298a43c74b6811ee7d60d8dc44cced167f92570f027e2aff911ed096ce1f7

ARG MOJAP_IMAGE_VERSION="default"
ENV MOJAP_IMAGE_VERSION=${MOJAP_IMAGE_VERSION}

USER root

WORKDIR /opt/analyticalplatform

COPY requirements.txt requirements.txt
COPY src/ .

RUN <<EOF
pip install --no-cache-dir --requirement requirements.txt
EOF

USER ${CONTAINER_UID}

ENTRYPOINT ["python3", "main.py"]
