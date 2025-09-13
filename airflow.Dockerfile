# airflow.Dockerfile
FROM apache/airflow:2.9.0

# Switch to airflow user right away (the base image has it set up already)
USER airflow

# Install extra Python packages into the Airflow env
RUN pip install --no-cache-dir \
    dbt-postgres \
    psycopg2-binary \
    python-dotenv \
    pandas \
    openpyxl \
    fastapi \
    uvicorn

WORKDIR /opt/airflow

