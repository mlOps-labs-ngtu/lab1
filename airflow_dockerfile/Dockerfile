FROM apache/airflow:slim-3.3.2-python3.13

USER airflow
COPY requirements.txt /requirements.txt
RUN python -m pip install --upgrade pip && \
  pip install --no-cache-dir -r /requirements.txt
