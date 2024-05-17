FROM gcr.io/dataflow-templates-base/python3-template-launcher-base
ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}
RUN apt-get update && apt-get install -y libffi-dev && rm -rf /var/lib/apt/lists/*
COPY dif_csv_to_bq.py .
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/dif_csv_to_bq.py"
RUN python3 -m pip install apache-beam[gcp]==2.56.0 pyyaml