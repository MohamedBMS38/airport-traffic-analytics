# Flight Tracker - Roadmap Checklist

Objectif du projet : construire un pipeline data engineering portfolio avec Airflow, Snowflake et dbt, en architecture Medallion Bronze / Silver / Gold.

Regle de travail : chaque case doit correspondre a une action que je comprends et que je peux refaire seul.

## Phase 1 - Fondations environnement

- [x] Ouvrir le bon dossier projet dans VS Code : `C:\Users\moham\projet_vol`
- [x] Verifier que Docker Desktop fonctionne
- [x] Verifier `docker version`
- [x] Verifier `docker compose version`
- [x] Creer ou verifier le fichier `.env`
- [x] Creer ou verifier le fichier `docker-compose.yml`
- [x] Valider le Compose avec `docker compose config`
- [x] Creer/verifier les dossiers montes par Docker : `dags/`, `scripts/`, `data/`, `logs/`, `plugins/`
- [ ] Nettoyer le fichier `.env` si besoin
- [ ] Creer un fichier `.env.example` sans secrets reels
- [ ] Remplir `requirements.txt` avec les dependances utiles
- [ ] Ajouter un `.gitignore` adapte au projet

## Phase 1 bis - Git et GitHub

- [ ] Verifier que Git est installe
- [ ] Configurer `user.name` et `user.email` Git si besoin
- [ ] Initialiser le repository Git local
- [ ] Verifier le statut avec `git status`
- [ ] Creer un `.gitignore` avant le premier commit
- [ ] Verifier que `.env`, `logs/` et fichiers temporaires ne seront pas suivis
- [ ] Faire un premier commit propre
- [ ] Creer un repository GitHub
- [ ] Connecter le repository local au repository GitHub
- [ ] Pousser le premier commit sur GitHub
- [ ] Verifier sur GitHub qu'aucun secret n'est present

## Phase 2 - Airflow local

- [x] Demarrer uniquement Postgres avec Docker Compose
- [x] Verifier que Postgres est `healthy`
- [x] Lancer `airflow-init`
- [x] Verifier que `airflow-init` termine avec `code 0`
- [x] Demarrer `airflow-webserver`
- [x] Demarrer `airflow-scheduler`
- [x] Acceder a l'interface Airflow sur `http://localhost:8080`
- [x] Se connecter a l'interface Airflow
- [x] Creer un premier DAG simple avec une seule task
- [x] Declencher le DAG manuellement
- [x] Lire les logs de la task dans l'interface Airflow
- [x] Creer un DAG avec deux tasks reliees par une dependance
- [x] Comprendre `task_1 >> task_2`
- [x] Creer un DAG de test Flight Tracker avec trois tasks : start, extract, end
- [x] Verifier le graph du DAG dans l'interface Airflow
- [x] Verifier les logs des trois tasks
- [ ] Revoir les noms de `dag_id` pour eviter les doublons
- [ ] Comprendre la difference entre `Audit Log` et logs de task

## Phase 3 - Ingestion API OpenSky

- [ ] Ajouter `requests` dans `requirements.txt`
- [ ] Comprendre le role de `requests.get()`
- [ ] Creer le fichier `dags/opensky_api_test_dag.py`
- [ ] Creer une fonction `extract_opensky_data`
- [ ] Appeler l'endpoint OpenSky depuis une task Airflow
- [ ] Verifier le code HTTP de la reponse
- [ ] Convertir la reponse en JSON
- [ ] Identifier la cle `states`
- [ ] Compter le nombre d'avions recuperes
- [ ] Logger un resume clair dans Airflow
- [ ] Gerer le cas ou l'API retourne une erreur
- [ ] Gerer le cas ou `states` est vide ou absent
- [ ] Ajouter un timeout a l'appel API
- [ ] Tester le DAG depuis l'interface Airflow
- [ ] Lire les logs de l'extraction

## Phase 4 - Snowflake Bronze

- [ ] Creer ou verifier le compte Snowflake
- [ ] Creer un warehouse dedie au projet
- [ ] Configurer `AUTO_SUSPEND`
- [ ] Configurer `AUTO_RESUME`
- [ ] Creer une database projet
- [ ] Creer les schemas `BRONZE`, `SILVER`, `GOLD`
- [ ] Comprendre le type Snowflake `VARIANT`
- [ ] Ajouter les variables Snowflake dans `.env`
- [ ] Ajouter les variables Snowflake dans `.env.example` sans secrets
- [ ] Installer/configurer le connecteur Snowflake Python
- [ ] Tester une connexion Snowflake depuis Airflow
- [ ] Creer une table Bronze pour stocker le JSON brut
- [ ] Charger une reponse OpenSky brute dans Bronze
- [ ] Ajouter un timestamp d'ingestion
- [ ] Verifier les lignes dans Snowflake

Important : aucune transformation metier en Bronze.

## Phase 5 - Stabilisation pipeline

- [ ] Ajouter un `batch_id` ou identifiant d'execution
- [ ] Ajouter `ingestion_timestamp`
- [ ] Ajouter `source_extracted_at` si pertinent
- [ ] Logger le nombre d'enregistrements charges
- [ ] Logger les erreurs de maniere lisible
- [ ] Comprendre l'idempotence
- [ ] Definir une strategie anti-doublons
- [ ] Tester un relancement du DAG
- [ ] Verifier que le relancement ne casse pas les donnees

## Phase 6 - Profilage / EDA leger

- [ ] Extraire un echantillon de Bronze
- [ ] Observer la structure des donnees OpenSky
- [ ] Identifier les champs importants
- [ ] Mesurer les valeurs nulles
- [ ] Identifier les types attendus
- [ ] Noter les anomalies possibles
- [ ] Definir les premieres regles de nettoyage pour Silver

## Phase 7 - dbt Silver

- [ ] Installer dbt avec l'adapter Snowflake
- [ ] Initialiser un projet dbt
- [ ] Configurer le profil dbt Snowflake
- [ ] Tester la connexion dbt
- [ ] Declarer les sources Bronze
- [ ] Creer un modele staging
- [ ] Extraire les champs utiles depuis `VARIANT`
- [ ] Typer les colonnes
- [ ] Nettoyer les valeurs incoherentes
- [ ] Dedoublonner si necessaire
- [ ] Ajouter des tests dbt simples
- [ ] Generer la documentation dbt

## Phase 8 - dbt Gold

- [ ] Choisir 3 a 5 KPIs simples
- [ ] Creer un premier modele Gold agrege
- [ ] Calculer le nombre d'avions par pays
- [ ] Calculer des indicateurs par altitude ou zone
- [ ] Ajouter des tests sur les modeles Gold
- [ ] Documenter les modeles Gold
- [ ] Verifier les couts des requetes Snowflake

## Phase 9 - Orchestration complete

- [ ] Faire executer l'ingestion Bronze par Airflow
- [ ] Faire executer dbt apres l'ingestion
- [ ] Ordonner le pipeline complet : API -> Bronze -> Silver -> Gold
- [ ] Ajouter des retries raisonnables
- [ ] Verifier les logs de chaque etape
- [ ] Tester un run complet depuis Airflow

## Phase 10 - Projet GitHub portfolio

- [ ] Rediger un README clair
- [ ] Ajouter un schema d'architecture
- [ ] Expliquer les choix techniques
- [ ] Expliquer pourquoi Spark n'est pas utilise dans ce projet
- [ ] Ajouter des captures Airflow
- [ ] Ajouter des captures Snowflake
- [ ] Ajouter des captures dbt docs si possible
- [ ] Documenter comment lancer le projet localement
- [ ] Documenter les limites du projet
- [ ] Verifier qu'aucun secret n'est versionne
- [ ] Faire une revue finale du repository

## Extension possible - Spark dans un autre projet

- [ ] Garder Spark pour un projet separe vraiment oriente gros volumes
- [ ] Construire plus tard un projet PySpark avec fichiers Parquet ou Delta Lake
- [ ] Expliquer pourquoi Spark devient utile dans ce second projet
