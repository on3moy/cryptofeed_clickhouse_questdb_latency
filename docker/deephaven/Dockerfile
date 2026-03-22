FROM ghcr.io/deephaven/server-ui:41.0

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONPATH="${PYTHONPATH}:/data/storage/notebooks"

RUN pip install --no-cache-dir pyarrow connectorx psycopg2-binary sqlalchemy

COPY ./config/deephaven.prop /opt/deephaven/config/deephaven.prop
COPY ./apps /apps
