# OpenSky API Notes - Airport Traffic Analytics

Objectif : centraliser les informations utiles pour construire les appels API OpenSky du projet.

Ce fichier sert de reference de travail avant de coder les DAGs Airflow.

## Orientation du projet

Le projet se concentre sur l'analyse du trafic aeroportuaire :

- arrivees par aeroport
- departs par aeroport
- comparaison arrivees / departs
- evolution du trafic dans le temps
- preparation d'une architecture Bronze / Silver / Gold

Le projet ne cherche pas a suivre les avions en temps reel comme objectif principal.

## Source principale

Documentation OpenSky REST API :

https://openskynetwork.github.io/opensky-api/rest.html

Endpoints prioritaires :

- Arrivals by Airport
- Departures by Airport

## Base URL

```text
https://opensky-network.org/api
```

## Endpoint - Arrivals by Airport

Objectif : recuperer les vols arrives dans un aeroport sur une periode donnee.

Endpoint :

```text
GET /flights/arrival
```

URL complete :

```text
https://opensky-network.org/api/flights/arrival
```

Parametres attendus :

```text
airport = code ICAO de l'aeroport
begin   = timestamp Unix de debut
end     = timestamp Unix de fin
```

Exemple conceptuel :

```text
https://opensky-network.org/api/flights/arrival?airport=LFPG&begin=...&end=...
```

Points importants :

- OpenSky attend un code ICAO, pas un code IATA.
- Exemple : Paris CDG = `LFPG`, pas `CDG`.
- Les donnees arrivals ne sont pas garanties en temps reel.
- Il faut utiliser une periode passee.
- L'intervalle de temps doit rester court pour eviter les erreurs et limiter le volume.
- Un HTTP `404` peut signifier qu'aucun vol n'a ete trouve sur la periode.

## Endpoint - Departures by Airport

Objectif : recuperer les vols partis d'un aeroport sur une periode donnee.

Endpoint :

```text
GET /flights/departure
```

URL complete :

```text
https://opensky-network.org/api/flights/departure
```

Parametres attendus :

```text
airport = code ICAO de l'aeroport
begin   = timestamp Unix de debut
end     = timestamp Unix de fin
```

Exemple conceptuel :

```text
https://opensky-network.org/api/flights/departure?airport=LFPG&begin=...&end=...
```

Points a verifier en pratique :

- comportement exact sans authentification
- limites de duree de la fenetre temporelle
- structure de reponse comparee aux arrivals
- traitement du HTTP `404`

## Codes aeroport utiles

OpenSky utilise les codes ICAO.

| Ville / aeroport | IATA | ICAO |
| --- | --- | --- |
| Paris Charles de Gaulle | CDG | LFPG |
| Paris Orly | ORY | LFPO |
| London Heathrow | LHR | EGLL |
| Frankfurt | FRA | EDDF |
| Amsterdam Schiphol | AMS | EHAM |
| Madrid Barajas | MAD | LEMD |
| Rome Fiumicino | FCO | LIRF |

Premier aeroport recommande pour les tests :

```text
LFPG
```

Pourquoi :

- gros aeroport europeen
- pertinent pour un projet francais
- volume de vols probablement suffisant
- simple a expliquer dans le README

## Timestamps Unix

OpenSky attend `begin` et `end` sous forme de timestamps Unix.

Un timestamp Unix represente un instant en secondes depuis le 1er janvier 1970 UTC.

Exemple :

```text
2026-05-13 00:00:00 UTC -> timestamp Unix
```

Decision projet :

- commencer avec une fenetre courte, par exemple 1 heure
- utiliser une date passee, par exemple avant-hier
- eviter de requeter les donnees du jour pour arrivals/departures

## Strategie de test API

Premier test recommande :

```text
endpoint : /flights/arrival
airport  : LFPG
window   : 1 heure passee
```

Objectif du premier DAG :

- construire l'URL
- envoyer la requete HTTP
- logger le code HTTP
- convertir la reponse en JSON si HTTP 200
- compter le nombre de vols retournes
- logger un resume clair

Le DAG ne doit pas encore :

- charger Snowflake
- transformer les donnees
- faire des KPIs
- stocker dans Gold/Silver

## Gestion des codes HTTP

Comportement attendu :

| Code | Interpretation projet |
| --- | --- |
| 200 | Reponse OK, parser le JSON |
| 400 | Requete mal construite, verifier les parametres |
| 401 | Authentification probablement requise |
| 404 | Aucun vol trouve ou ressource absente selon le contexte |
| 429 | Rate limit, trop d'appels |
| 5xx | Probleme cote API OpenSky |

Decision projet :

- `200` : success
- `404` : log warning et traiter comme liste vide dans certains cas
- autres erreurs : lever une exception pour faire echouer la task Airflow

## Securite et configuration

Pour les premiers tests, l'endpoint peut etre appele sans secret si OpenSky l'autorise.

Si authentification necessaire :

- ne jamais mettre les credentials dans le code
- utiliser `.env`
- ajouter uniquement des placeholders dans `.env.example`
- transmettre les variables au conteneur Airflow via Docker Compose

Variables possibles plus tard :

```text
OPENSKY_USERNAME=
OPENSKY_PASSWORD=
OPENSKY_CLIENT_ID=
OPENSKY_CLIENT_SECRET=
```

Le type exact dependra du mode d'authentification retenu.

## Risques techniques anticipes

### 1. Donnees non disponibles pour aujourd'hui

Les endpoints arrivals/departures peuvent ne pas exposer les donnees du jour.

Mitigation :

- tester avec une periode passee
- commencer par avant-hier

### 2. Fenetre temporelle trop large

Une fenetre trop large peut produire une erreur ou trop de volume.

Mitigation :

- commencer avec 1 heure
- augmenter progressivement a 24 heures si stable

### 3. 404 mal interprete

Un `404` peut correspondre a aucune donnee sur la periode.

Mitigation :

- logger clairement le cas
- ne pas traiter automatiquement tous les `404` comme une panne API

### 4. Rate limit

OpenSky peut limiter le nombre d'appels.

Mitigation :

- pas de schedule frequent pendant les tests
- commencer en declenchement manuel
- ajouter retries raisonnables plus tard

### 5. Dependances Airflow

Ajouter `requests` dans `requirements.txt` ne garantit pas que le conteneur Airflow l'installe.

Mitigation :

- tester l'import dans le DAG
- si besoin, construire une image Airflow custom
- eviter les installations manuelles dans les conteneurs

### 6. Secrets dans GitHub

Le repo est public.

Mitigation :

- garder `.env` ignore par Git
- maintenir `.env.example`
- verifier `git status` avant chaque commit

## Prochaines decisions a prendre

- choisir la premiere fenetre de temps exacte pour `LFPG`
- decider si on commence par arrivals uniquement ou arrivals + departures
- tester si OpenSky accepte l'appel sans authentification
- definir le format du log de resume
- definir la future structure Bronze Snowflake

## Decision actuelle

Pour la prochaine implementation :

```text
Endpoint : /flights/arrival
Aeroport : LFPG
Fenetre : 1 heure dans le passe
Sortie : logs Airflow uniquement
Stockage : aucun pour l'instant
```
