FROM apache/airflow:2.9.3

# Les dependances Python du projet sont installees dans l'image Airflow custom.
COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt
