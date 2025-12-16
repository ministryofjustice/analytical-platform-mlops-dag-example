#checkov:skip=CKV_DOCKER_2: HEALTHCHECK not required
FROM ghcr.io/ministryofjustice/analytical-platform-airflow-python-base:1.22.0@sha256:1fbc48bbe820be43c5910af9990a47d537808470bb465beb6634e30f23f2aace

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
