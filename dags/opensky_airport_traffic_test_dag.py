"""DAG de test pour extraire les arrivees et departs OpenSky d'un aeroport."""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta, timezone
import logging
import os
import requests
import snowflake.connector
import json
import uuid


# URL de base de l'API OpenSky.
# Les endpoints precis sont ajoutes ensuite, par exemple /flights/arrival.
OPENSKY_BASE_URL = "https://opensky-network.org/api"

# Endpoint OAuth2 utilise pour recuperer un token d'acces OpenSky.
OPENSKY_TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

# Code ICAO de Paris Charles de Gaulle.
# OpenSky attend un code ICAO comme LFPG, pas un code IATA comme CDG.
AIRPORT_ICAO = "LFPG"
VALID_FLIGHT_TYPES = {"arrival", "departure"}

# Fenetre fixe temporaire pour stabiliser les tests manuels.
# Elle sera remplacee par une fenetre dynamique quand le chargement Bronze sera valide.
USE_FIXED_TEST_WINDOW = True
TEST_WINDOW_BEGIN_TS = 1778962333
TEST_WINDOW_END_TS = 1778965933

# Les appels API peuvent echouer temporairement.
# Airflow retentera la task avant de la considerer definitivement en erreur.
DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def get_opensky_token():
    # OpenSky utilise un token OAuth2 : on le recupere avant d'appeler l'API.
    client_id = os.getenv("OPENSKY_CLIENT_ID")
    client_secret = os.getenv("OPENSKY_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise ValueError("OpenSky credentials are missing from environment variables")

    try:
        response = requests.post(
            OPENSKY_TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            },
            timeout=30,
        )
        response.raise_for_status()
    except requests.RequestException:
        logging.error("Echec de recuperation du token OpenSky.")
        raise

    token_data = response.json()
    return token_data["access_token"]


def build_time_window():
    if USE_FIXED_TEST_WINDOW:
        return TEST_WINDOW_BEGIN_TS, TEST_WINDOW_END_TS

    # Les donnees arrivals/departures ne sont pas garanties en temps reel.
    # On interroge donc une fenetre passee pour augmenter les chances d'obtenir des resultats.
    end_time = datetime.now(timezone.utc) - timedelta(days=2)
    begin_time = end_time - timedelta(hours=1)

    # OpenSky attend begin/end sous forme de timestamps Unix en secondes.
    return int(begin_time.timestamp()), int(end_time.timestamp())


def get_snowflake_connection():
    # Connexion reutilisable vers Snowflake.
    # Les valeurs viennent des variables d'environnement injectees par Docker Compose.
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )

    return conn


def get_bronze_table_name(flight_type):
    if flight_type == "arrival":
        return "OPENSKY_ARRIVALS_RAW"

    if flight_type == "departure":
        return "OPENSKY_DEPARTURES_RAW"

    raise ValueError(f"Invalid flight_type: {flight_type}")


def load_flights_to_snowflake(flight_type, airport_icao, begin, end, records):
    table_name = get_bronze_table_name(flight_type)
    ingestion_id = str(uuid.uuid4())

    logging.info(
        "Chargement Bronze Snowflake : %s vols vers %s",
        len(records),
        table_name,
    )

    if not records:
        logging.info("Aucun vol a charger dans Snowflake pour %s.", flight_type)
        return

    rows = [
        (
            ingestion_id,
            airport_icao,
            begin,
            end,
            json.dumps(record),
        )
        for record in records
    ]

    conn = get_snowflake_connection()
    cursor = None

    try:
        cursor = conn.cursor()

        insert_sql = f"""
            INSERT INTO {table_name} (
                INGESTION_ID,
                INGESTION_TIMESTAMP,
                AIRPORT_ICAO,
                BEGIN_TS,
                END_TS,
                RAW_RECORD
            )
            VALUES (
                %s,
                CURRENT_TIMESTAMP(),
                %s,
                %s,
                %s,
                PARSE_JSON(%s)
            )
        """

        cursor.executemany(insert_sql, rows)
        conn.commit()

        logging.info(
            "Chargement Bronze termine : %s lignes inserees dans %s.",
            len(rows),
            table_name,
        )

    finally:
        if cursor:
            cursor.close()
        conn.close()


def extract_airport_flights(flight_type):
    # Le meme bloc sert aux arrivees et aux departs.
    # On valide le type pour eviter de construire une URL OpenSky invalide.
    if flight_type not in VALID_FLIGHT_TYPES:
        raise ValueError(f"Invalid flight_type: {flight_type}")

    begin, end = build_time_window()

    url = f"{OPENSKY_BASE_URL}/flights/{flight_type}"

    params = {
        "airport": AIRPORT_ICAO,
        "begin": begin,
        "end": end,
    }

    token = get_opensky_token()

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = requests.get(
        url,
        params=params,
        headers=headers,
        timeout=30,
    )

    # Logs techniques utiles pour verifier l'appel sans afficher toute la reponse.
    logging.info("Code HTTP recu : %s", response.status_code)
    logging.info("URL appelee : %s", response.url)

    # Cas nominal : la reponse est OK, on peut convertir le JSON.
    if response.status_code == 200:
        data = response.json()
        logging.info("Nombre de vols %s recuperes : %s", flight_type, len(data))
        if data:
            # On affiche les champs du premier vol pour comprendre la structure brute.
            logging.info(
                "Champs disponibles pour %s : %s",
                flight_type,
                list(data[0].keys()),
            )

        load_flights_to_snowflake(
            flight_type=flight_type,
            airport_icao=AIRPORT_ICAO,
            begin=begin,
            end=end,
            records=data,
        )

        return

    # Pour cet endpoint, un 404 peut simplement vouloir dire qu'aucun vol n'a ete trouve.
    # On le traite donc comme un cas attendu, pas comme une erreur critique.
    if response.status_code == 404:
        logging.warning("Aucun vol trouve pour cette fenetre.")
        return

    # Pour les autres erreurs HTTP, on log un message clair puis on fait echouer la task.
    logging.error(
        "Erreur API OpenSky. Arret de la task. Code HTTP : %s. Reponse : %s",
        response.status_code,
        response.text[:500],
    )
    response.raise_for_status()


def extract_airport_arrivals():
    extract_airport_flights("arrival")


def extract_airport_departures():
    extract_airport_flights("departure")


dag = DAG(
    dag_id="opensky_airport_traffic_test_dag",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
)

extract_arrivals_task = PythonOperator(
    task_id="extract_airport_arrivals",
    python_callable=extract_airport_arrivals,
    dag=dag,
)

extract_departures_task = PythonOperator(
    task_id="extract_airport_departures",
    python_callable=extract_airport_departures,
    dag=dag,
)

extract_arrivals_task >> extract_departures_task
