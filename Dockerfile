#checkov:skip=CKV_DOCKER_2: HEALTHCHECK not required
FROM ghcr.io/ministryofjustice/analytical-platform-airflow-python-base:1.21.0@sha256:c312e931a1a4822af79b2fe567c62f37ee1264dd34507a9ccd16b98c95bf7ea9

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
