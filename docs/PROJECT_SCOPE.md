# Airport Traffic Analytics - Project Scope

## Objectif

Construire un pipeline data engineering portfolio pour analyser le trafic d'aeroports europeens a partir de l'API OpenSky.

Le projet doit demontrer :

- orchestration avec Airflow
- ingestion API authentifiee
- stockage historique dans Snowflake
- architecture Medallion Bronze / Silver / Gold
- transformations analytiques avec dbt
- exposition BI avec Power BI Desktop
- maitrise des couts et des limites API

## Question metier

Comment evoluent les arrivees, les departs et les routes estimees des grands aeroports europeens ?

## Donnees retenues

Donnees principales :

- arrivees
- departs
- routes estimees
- callsigns
- duree approximative du vol
- qualite des donnees

Donnees enrichies possibles :

- trajectoires ponctuelles via `/tracks`
- categorie large d'aeronef via `/states/all?extended=1`

## Definitions simples

Callsign :

- identifiant radio / operationnel transmis par l'avion
- exemple : `AFR123`, `DLH456`
- utile pour compter les vols ou identifier des frequences
- pas toujours parfaitement propre ou stable

Route estimee :

- relation entre aeroport de depart estime et aeroport d'arrivee estime
- exemple : `LFPG -> EDDF`

Trajectoire :

- suite de points geographiques dans le temps
- latitude, longitude, altitude, timestamp
- plus detaillee qu'une route estimee
- a utiliser seulement pour quelques vols, pas pour tout le volume

Duree approximative :

- calculee avec `lastSeen - firstSeen`
- represente la duree observee par OpenSky
- ce n'est pas forcement la duree commerciale officielle du vol

## Endpoints OpenSky

Endpoints principaux :

```text
/flights/arrival
/flights/departure
```

Endpoints bonus :

```text
/tracks
/states/all?extended=1
```

Endpoints non prioritaires :

```text
/flights/all
/flights/aircraft
/states/own
```

## Aeroports cibles

MVP initial :

```text
LFPG - Paris Charles de Gaulle
```

Extension dashboard :

```text
LFPG - Paris Charles de Gaulle
LFPO - Paris Orly
EGLL - London Heathrow
EGKK - London Gatwick
EDDF - Frankfurt
EHAM - Amsterdam Schiphol
LEMD - Madrid Barajas
LEBL - Barcelona
LIRF - Rome Fiumicino
LOWW - Vienna
```

Ces aeroports permettent d'avoir assez de volume pour creer des filtres Power BI utiles.

## Strategie d'ingestion

Pattern retenu :

```text
petites fenetres regulieres + stockage cumulatif dans Snowflake
```

Strategie recommandee :

- recuperer J-2 pendant le developpement
- passer eventuellement a J-1 si les donnees sont stables
- commencer en execution manuelle Airflow
- planifier quotidiennement seulement apres stabilisation
- eviter les backfills massifs au debut

Pourquoi J-2 :

- les endpoints arrivals/departures ne sont pas garantis en temps reel
- les donnees sont mises a jour par batch
- J-2 reduit le risque de reponse vide ou incomplete

## Architecture cible

```text
OpenSky API
    -> Airflow
    -> Snowflake Bronze
    -> dbt Silver
    -> dbt Gold
    -> Power BI Desktop
```

## Architecture Medallion

Bronze :

- reflet brut des endpoints
- peu ou pas de transformation
- objectif : tracabilite source

Tables Bronze retenues :

```text
bronze.opensky_arrivals_raw
bronze.opensky_departures_raw
```

Pourquoi deux tables Bronze :

- un endpoint API = une table raw
- meilleure tracabilite
- isolation si un endpoint change
- logique proche des pratiques professionnelles

Silver :

```text
silver.opensky_airport_flights
```

Role :

- unifier arrivals et departures
- ajouter `flight_type`
- typer les champs
- nettoyer les valeurs
- preparer les donnees analytiques

Gold :

Tables possibles :

```text
gold.airport_traffic_daily
gold.airport_route_summary
gold.airport_callsign_summary
gold.data_quality_summary
```

Role :

- tables pretes pour Power BI
- agregations simples
- KPIs clairs

## KPIs dashboard

Vue globale :

- nombre d'arrivees
- nombre de departs
- trafic total
- ratio arrivees / departs
- evolution par jour

Routes :

- routes estimees les plus frequentes
- trafic par route
- aeroports de depart / arrivee les plus frequents

Operations :

- duree approximative moyenne
- distribution des durees
- top callsigns

Qualite :

- vols sans callsign
- vols sans aeroport de depart estime
- vols sans aeroport d'arrivee estime
- valeurs nulles par champ
- evolution du nombre de vols ingeres par batch

## Strategie Power BI

Choix recommande :

```text
Power BI Desktop en Import mode
```

Pourquoi :

- gratuit localement
- evite les requetes Snowflake a chaque interaction
- limite la consommation de credits
- le fichier `.pbix` reste exploitable meme si Snowflake est suspendu

A eviter au debut :

```text
DirectQuery
```

Pourquoi :

- chaque interaction peut interroger Snowflake
- plus risqué pour les couts
- moins utile pour un projet portfolio

Fallback :

```text
Snowflake Gold -> export CSV -> Power BI Desktop
```

Ce fallback permet de garder un dashboard demonstrable meme si la connexion directe Power BI / Snowflake bloque.

## Snowflake - couts et limites

Principaux postes de cout :

- compute : warehouse actif
- storage : donnees stockees
- data transfer : surtout egress hors Snowflake

Risque principal :

```text
warehouse laisse actif
```

Strategie cout :

```text
WAREHOUSE_SIZE = XSMALL
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE
```

Regles projet :

- ne pas utiliser DirectQuery au debut
- ne pas lancer de backfill massif
- verifier regulierement l'usage Snowflake
- suspendre automatiquement les warehouses
- garder les volumes raisonnables

Hypothese :

- avec 3 mois d'essai et environ 400 credits, la limite principale sera probablement la duree de l'essai, pas les credits
- apres suspension du compte Snowflake, Power BI Import mode reste consultable avec les donnees deja importees
- les refreshs depuis Snowflake ne fonctionneront plus tant que le compte est suspendu

## OpenSky - limites et risques

Risques :

- quotas API
- `429 Too Many Requests`
- endpoints historiques limites dans le temps
- arrivals/departures non temps reel
- reponses vides possibles
- endpoint `/tracks` experimental

Mitigations :

- execution manuelle au debut
- fenetres courtes
- 10 aeroports maximum dans le MVP et extension proche
- pas de tracks pour tous les vols
- logs clairs sur code HTTP et nombre de lignes
- stockage cumulatif dans Snowflake

## Decisions actuelles

- Le projet reste centre sur l'analyse de trafic aeroportuaire.
- Les endpoints principaux sont arrivals et departures.
- Les routes estimees, callsigns et durees approximatives sont inclus dans le scope.
- Les trajectoires et categories aeronefs sont des enrichissements bonus.
- Bronze garde une table par endpoint.
- Silver unifie les donnees.
- Gold expose les KPIs pour Power BI.
- Power BI doit utiliser Import mode.
- Snowflake doit etre configure avec un warehouse X-Small et auto-suspend.

## Prochaine phase

Avant de charger des donnees :

- creer ou verifier le compte Snowflake
- creer un warehouse X-Small avec auto-suspend
- creer database et schemas `BRONZE`, `SILVER`, `GOLD`
- choisir la strategie d'installation des dependances Airflow pour Snowflake
- creer les tables Bronze raw
