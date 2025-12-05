# Build stage
# FROM postgres:16
# COPY intidb/database.sql /docker-entrypoint-intidb.d/

FROM apache/airflow:3.1.3-python3.10

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# replace dbt-postgres with another supported adapter if you're using a different warehouse type
RUN python -m venv dbt_venv  && source dbt_venv/bin/activate  && uv \
pip install dbt-core && uv pip install --no-cache-dir dbt-postgres  && deactivate

RUN uv pip install apache-airflow-providers-ftp>=3.13.3 \
    && uv pip install apache-airflow-providers-http>=5.5.0 \
    # && uv pip install apache-airflow-providers-fab>=3.0.1 \
    && uv pip install apache-airflow-providers-mysql>=6.3.5 \
    && uv pip install apache-airflow-providers-postgres>=6.4.1 \
    && uv pip install astronomer-cosmos>=1.11.1 \
    && uv pip install fhir-resources>=5.1.1 \
    && uv pip install great-expectations>=1.9.0 \
    && uv pip install openpyxl>=3.1.5 \
    && uv pip install pandas==2.1.4 \
    && uv pip install pandera>=0.26.1 \
    && uv pip install psycopg2-binary>=2.9.11 \
    && uv pip install pyarrow>=20.0.0 \
    && uv pip install pydbml>=1.2.1 \
    && uv pip install python-dateutil>=2.9.0.post0 \
    && uv pip install sqlalchemy>=1.4.54 \
    && uv pip install ujson>=5.10.0 \
    && uv pip install webdav4>=0.10.0 \
    && uv pip install xlsxwriter>=3.2.3 \
    && uv pip install xgboost>=3.0.5 \
    && uv pip install joblib==1.2.0 \
    && uv pip install scikit-learn==1.1.3 \
    && uv pip install imbalanced-learn==0.12.4 \
    && uv pip install numpy==1.23.5 \
    && uv pip install matplotlib>=3.10.7 \
