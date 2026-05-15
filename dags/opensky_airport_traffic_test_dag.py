#Ce fichier définit un DAG Airflow de test pour interroger l’API OpenSky sur les arrivées à l’aéroport Paris Charles de Gaulle (LFPG).
#Il calcule automatiquement une fenêtre passée d’une heure, appelle l’endpoint /flights/arrival, puis écrit dans les logs Airflow le code HTTP, l’URL appelée et le nombre d’arrivées récupérées.
#Le but est de valider l’appel API et la logique de fenêtre temporelle avant de stocker les données dans Snowflake Bronze.

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta, timezone
import logging
import requests
import os


# URL de base de l'API OpenSky.
# Les endpoints precis seront ajoutes ensuite, par exemple /flights/arrival.
OPENSKY_BASE_URL = "https://opensky-network.org/api"

OPENSKY_TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"


# Code ICAO de Paris Charles de Gaulle.
# OpenSky attend un code ICAO comme LFPG, pas un code IATA comme CDG.
AIRPORT_ICAO = "LFPG"

def get_opensky_token():
    client_id = os.getenv("OPENSKY_CLIENT_ID")
    client_secret = os.getenv("OPENSKY_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise ValueError("OpenSky credentials are missing from environment variables")

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

    token_data = response.json()
    return token_data["access_token"]



def build_time_window():
    # Les donnees arrivals/departures ne sont pas garanties en temps reel.
    # On interroge donc une fenetre passee pour augmenter les chances d'obtenir des resultats.
    end_time = datetime.now(timezone.utc) - timedelta(days=2)
    begin_time = end_time - timedelta(hours=1)

    # OpenSky attend begin/end sous forme de timestamps Unix en secondes.
    return int(begin_time.timestamp()), int(end_time.timestamp())


    
def extract_airport_flights(flight_type):
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
    logging.info("Code HTTP reçu : %s", response.status_code)
    logging.info("URL appelée : %s", response.url)

    # Cas nominal : la reponse est OK, on peut convertir le JSON.
    if response.status_code == 200:
        data = response.json()
        logging.info("Nombre de vols %s récupérés : %s", flight_type, len(data))
        if data:
            logging.info(
                "Champs disponibles pour %s : %s",
                flight_type,
                list(data[0].keys()),
            )
        return

    # Pour cet endpoint, un 404 peut simplement vouloir dire qu'aucun vol n'a ete trouve.
    # On le traite donc comme un cas attendu, pas comme une erreur critique.
    if response.status_code == 404:
        logging.warning("Aucun vol trouvé pour cette fenêtre.")
        return

    # Pour les autres erreurs HTTP, on log un message clair puis on fait echouer la task.
    logging.error(
        "Erreur API OpenSky. Arrêt de la task. Code HTTP : %s",
        response.status_code,
    )
    response.raise_for_status()


def extract_airport_arrivals():
    extract_airport_flights("arrival")


def extract_airport_departures():
    extract_airport_flights("departure")


dag = DAG(
    dag_id="opensky_airport_traffic_test_dag",
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
