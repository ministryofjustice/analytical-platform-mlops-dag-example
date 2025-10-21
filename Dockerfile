FROM ghcr.io/ministryofjustice/analytical-platform-airflow-python-base:1.19.0@sha256:594f9d28b5b53ad55f96dce790774dccc93db892a167bcbd832bb9a06fea0e1b

ARG MOJAP_IMAGE_VERSION="default"
ENV MOJAP_IMAGE_VERSION=${MOJAP_IMAGE_VERSION}

USER root                 
                       
COPY requirements.txt requirements.txt 
COPY src/ .

RUN <<EOF
pip install --no-cache-dir --requirement requirements.txt
EOF

USER ${CONTAINER_UID}

ENTRYPOINT ["python3", "main.py"]
