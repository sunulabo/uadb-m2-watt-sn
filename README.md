# ⚡ Watt-SN — Intelligence Énergétique & Prédiction de Délestage

> Système Big Data temps réel pour la prédiction des délestages et l'optimisation de l'injection solaire au Sénégal.

[![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python)](https://python.org)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-orange?logo=apachespark)](https://spark.apache.org)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.5.0-black?logo=apachekafka)](https://kafka.apache.org)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7.2-017CEE?logo=apacheairflow)](https://airflow.apache.org)
[![HBase](https://img.shields.io/badge/Apache%20HBase-2.1.2-red)](https://hbase.apache.org)
[![License](https://img.shields.io/badge/Licence-MIT-green)](LICENSE)

---

## 📋 Table des matières

- [Contexte](#-contexte)
- [Architecture](#-architecture)
- [Résultats obtenus](#-résultats-obtenus)
- [Équipe](#-équipe)
- [Prérequis](#-prérequis)
- [Installation](#-installation)
- [Démarrage](#-démarrage)
- [Services & Ports](#-services--ports)
- [Structure du projet](#-structure-du-projet)
- [Scripts Python](#-scripts-python)
- [Conventions Git](#-conventions-git)
- [Workflow de développement](#-workflow-de-développement)
- [Résolution de problèmes](#-résolution-de-problèmes-fréquents)
- [Barème & Livrables](#-barème--livrables)

---

## 🌍 Contexte

Le Sénégal produit environ **800 MW** pour une demande nationale de **~1 200 MW**, générant des délestages fréquents qui coûtent **~2% du PIB**. Watt-SN est un pipeline Big Data complet qui :

- **Ingère** les données de consommation Senelec, météo et GIS en temps réel via Apache NiFi
- **Traite** les flux avec Spark Structured Streaming (fenêtres 1h, watermark 10min)
- **Classifie** les zones en **🔴 ROUGE** (>10 000 MW total) / **🟠 ORANGE** (>5 000 MW) / **🟢 VERT**
- **Anonymise** les compteurs industriels via SHA-256 (Privacy by Design)
- **Prédit** les délestages avec RandomForest (MLlib)
- **Orchestre** le retraining automatique via Apache Airflow

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         SOURCES DE DONNÉES                          │
│   Senelec API        Données Météo        Données GIS               │
│  (consommation)     (ensoleillement)      (zones réseau)            │
└──────────────┬──────────────┬─────────────────┬─────────────────────┘
               │              │                 │
               ▼              ▼                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        APACHE NIFI :8081                            │
│              Ingestion multi-sources + routage des flux             │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
               ┌───────────────┼───────────────┐
               ▼               ▼               ▼
        watt_conso_raw  watt_solaire_raw  watt_alerts
┌─────────────────────────────────────────────────────────────────────┐
│                    APACHE KAFKA :9092                               │
│              Broker de messages — 3 topics                          │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   SPARK STRUCTURED STREAMING :8080                  │
│  • Fenêtres 1h (slide 15min)   • Watermark 10min                   │
│  • Anonymisation SHA-256        • Classification ROUGE/ORANGE/VERT  │
│  • RandomForest MLlib           • ~600 batches traités              │
└──────────────┬───────────────────────────────┬──────────────────────┘
               │                               │
               ▼                               ▼
┌──────────────────────────┐   ┌───────────────────────────────────────┐
│     APACHE HBASE :16010  │   │         APACHE HIVE :10000            │
│  Alertes temps réel      │   │  conso_historique (ORC/SNAPPY)        │
│  watt:conso_temps_reel   │   │  vue_risque_zone                      │
│  watt:alertes            │   │  vue_solaire_optimisation             │
│  watt:solaire            │   │  vue_delestage_tournant               │
└──────────────────────────┘   └───────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    APACHE AIRFLOW :8083                             │
│  DAG watt_sn_dag.py — schedule @hourly                             │
│  BranchPythonOperator → retraining si zones ROUGE > 2              │
│  1 run réussi documenté — Accuracy RandomForest validée            │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 📊 Résultats obtenus

| Métrique | Valeur | Détail |
|----------|--------|--------|
| **Batches Spark traités** | ~600 | Pipeline local[2], fenêtres 1h |
| **Alertes ROUGE générées** | ✅ | Zones DAKAR_NORD, DAKAR_SUD, THIES actives |
| **Alertes ORANGE générées** | ✅ | Zones SAINT_LOUIS, KAOLACK actives |
| **DAG Airflow** | 1 run réussi | 6 tâches, BranchPythonOperator opérationnel |
| **Accuracy RandomForest** | validée | Retraining déclenché automatiquement |
| **Tables Hive** | 3 tables + 3 vues | ORC/SNAPPY, partitionnées par date |
| **Tables HBase** | 3 tables | Namespace `watt`, Thrift port 9090 |
| **Topics Kafka** | 3 topics | watt_conso_raw, watt_solaire_raw, watt_alerts |
| **Anonymisation SHA-256** | ✅ | 0 PII dans HBase (compteurs industriels) |
| **Zones couvertes** | 7 | Dakar Nord/Sud, Thiès, Saint-Louis, Ziguinchor, Kaolack, Tambacounda |

---

## 👥 Équipe

| Rôle | Nom & Prénom | Email | Branche Git |
|------|-------------|-------|-------------|
| **Data Engineering & Infrastructure** | MBAYE Moustapha | mbayemustaf2000@gmail.com | `dev-moustapha` |
| **Data Science & ML & Streaming** | WONE Aissata | aissatawone06@gmail.com | `dev-aissata` |

**Encadrant :** Mr Ahmed Ben Sidy Bouya SEYE — Senior Big Data & AI Engineer | Groupe Sonatel

**Dépôt officiel :** [github.com/sunulabo/uadb-m2-watt-sn](https://github.com/sunulabo/uadb-m2-watt-sn)

---

## 🛠️ Prérequis

| Outil          | Version minimale | Vérification |
|----------------|------------------|--------------------------|
| Docker Desktop | 4.0+             | `docker --version` |
| Docker Compose | 2.0+             | `docker compose version` |
| Python         | 3.11+            | `python3 --version` |
| Git            | 2.30+            | `git --version` |
| RAM disponible | 8 GB minimum     | — |
| Espace disque  | 20 GB minimum    | — |

> ⚠️ **Important** : Ne pas lancer Watt-SN en même temps que Cloudera QuickStart (conflit de RAM si insuffisant).

---

## 📦 Installation

### 1. Cloner le dépôt

```bash
git clone https://github.com/sunulabo/uadb-m2-watt-sn.git
cd uadb-m2-watt-sn
```

### 2. Créer sa branche de travail

```bash
# Moustapha
git checkout -b dev-moustapha

# Aissata
git checkout -b dev-aissata
git merge origin/dev-moustapha   # récupérer le travail existant
```

### 3. Créer l'environnement Python

```bash
python3 -m venv .venv
source .venv/bin/activate        # macOS/Linux
# .venv\Scripts\activate         # Windows

pip install -r requirements.txt
```

---

## 🚀 Démarrage

### Lancer toute l'infrastructure

```bash
cd docker
docker compose up -d
```

> Pour une démonstration allégée (économie de RAM) :
> ```bash
> docker compose up -d zookeeper kafka hbase spark-master
> ```

### Initialiser HBase (première fois uniquement)

```bash
# Créer le namespace 'watt' dans HBase shell
docker exec -it watt_hbase hbase shell
# Dans le shell HBase :
create_namespace 'watt'
exit

# Créer les 3 tables
python scripts/hbase_setup.py
```

### Initialiser Hive (première fois uniquement)

```bash
docker exec --user root -it watt_hive bash -c "mkdir -p /user/hive/warehouse && chmod -R 777 /user/hive"
docker cp scripts/hive_setup.sql watt_hive:/tmp/hive_setup.sql
docker exec -it watt_hive beeline -u jdbc:hive2://localhost:10000 -n hive -p hive123 -f /tmp/hive_setup.sql
```

### Vérifier que tout tourne

```bash
docker compose ps
```

### Arrêter (en conservant les données)

```bash
cd docker
docker compose down          # conserve les volumes
docker compose down -v       # supprime aussi les volumes (reset complet)
```

---

## 🌐 Services & Ports

| Service | URL | Credentials |
|---------|-----|-------------|
| **Apache NiFi** | http://localhost:8081/nifi | admin / adminpassword123 |
| **Spark Master UI** | http://localhost:8080 | — |
| **Spark Worker UI** | http://localhost:8082 | — |
| **Apache Airflow** | http://localhost:8083 | admin / admin123 |
| **Kafka UI** | http://localhost:8085 | — |
| **HBase Master UI** | http://localhost:16010 | — |
| **HBase REST** | http://localhost:8086 | — |
| **Kafka Broker** | localhost:9092 | — |
| **ZooKeeper** | localhost:2181 | — |
| **HBase Thrift** | localhost:9090 | — |
| **HiveServer2** | localhost:10000 | hive / hive123 |

---

## 📁 Structure du projet

```
uadb-m2-watt-sn/
│
├── docker/
│   └── docker-compose.yml              # Infrastructure complète (14 services)
│
├── scripts/
│   ├── hbase_setup.py                  # Création tables HBase ✅
│   ├── hbase_consumer.py               # Consumer Kafka → HBase ✅
│   ├── hive_setup.sql                  # Tables + vues Hive ✅
│   ├── schema.py                       # Schéma Pandera + validation ✅
│   ├── kafka_producer_watt_sn.py       # Simulateur données (seed=42) ✅
│   └── streaming_watt_sn.py            # Pipeline Spark Streaming ✅
│
├── dags/
│   └── watt_sn_dag.py                  # DAG Airflow MLOps ✅
│
├── nifi_templates/
│   └── template_nifi_equipe06.xml      # Template NiFi exporté
│
├── dashboard/
│   ├── dashboard_watt_sn.py            # Dashboard Matplotlib
│   └── *.png                           # Graphiques générés
│
├── rapport/
│   └── rapport.pdf                     # Livrable final (5-7 pages)
│
├── data/                               # Données simulées (non commitées)
├── models/                             # Modèles .pkl (non commités)
├── .venv/                              # Environnement Python (non commité)
├── requirements.txt                    # Dépendances Python ✅
├── .gitignore                          # Exclusions Git ✅
└── README.md                           # Ce fichier ✅
```

---

## 🐍 Scripts Python

### `hbase_setup.py` — Initialisation HBase

```bash
python scripts/hbase_setup.py
```

Crée 3 tables dans le namespace `watt` :
- `watt:conso_temps_reel` — familles : `info`, `conso`
- `watt:alertes` — familles : `info`, `alerte`, `action`
- `watt:solaire` — familles : `info`, `solaire`, `prevision`

> ⚠️ Le namespace `watt` doit être créé manuellement via `hbase shell` avant d'exécuter ce script.

### `kafka_producer_watt_sn.py` — Simulateur de données

```bash
python scripts/kafka_producer_watt_sn.py
```

- **seed=42** pour la reproductibilité
- **7 zones** : DAKAR_NORD, DAKAR_SUD, THIES, SAINT_LOUIS, ZIGUINCHOR, KAOLACK, TAMBACOUNDA
- Profil consommation jour/nuit réaliste (pic 7h-9h et 18h-22h)
- Envoie vers `watt_conso_raw` et `watt_solaire_raw` toutes les 5 secondes

### `streaming_watt_sn.py` — Pipeline Spark Streaming

```bash
# Étape 1 : Copier les JARs Kafka dans le container Spark (une seule fois)
docker cp ~/Downloads/spark-sql-kafka-0-10_2.12-3.5.1.jar watt_spark_master:/opt/spark/jars/
docker cp ~/Downloads/kafka-clients-3.4.1.jar watt_spark_master:/opt/spark/jars/
docker cp ~/Downloads/spark-token-provider-kafka-0-10_2.12-3.5.1.jar watt_spark_master:/opt/spark/jars/
docker cp ~/Downloads/commons-pool2-2.11.1.jar watt_spark_master:/opt/spark/jars/

# Étape 2 : Copier le script dans le container
docker cp scripts/streaming_watt_sn.py watt_spark_master:/opt/spark/work-dir/streaming_watt_sn.py

# Étape 3 : Lancer le pipeline (mode local[2] recommandé)
docker exec -it watt_spark_master bash
/opt/spark/bin/spark-submit --master local[2] /opt/spark/work-dir/streaming_watt_sn.py
```

Fonctionnalités du pipeline :
- Fenêtres 1h avec slide 15 min
- Watermark 10 minutes (gestion des événements en retard)
- Anonymisation SHA-256 des compteurs industriels
- Classification **ROUGE** (>10 000 MW) / **ORANGE** (>5 000 MW) / **VERT**
- Écriture alertes → topic `watt_alerts`

### `hbase_consumer.py` — Consumer Kafka → HBase

```bash
python scripts/hbase_consumer.py
```

- Lit le topic `watt_alerts` en continu
- Écrit les alertes ROUGE et ORANGE dans `watt:alertes`
- Se lance sur la machine hôte (happybase non disponible dans le container Spark)

### Pipeline complet (3 terminaux simultanés)

```bash
# Terminal 1 — Producteur de données
python scripts/kafka_producer_watt_sn.py

# Terminal 2 — Consumer HBase
python scripts/hbase_consumer.py

# Terminal 3 — Pipeline Spark Streaming
docker exec -it watt_spark_master bash
/opt/spark/bin/spark-submit --master local[2] /opt/spark/work-dir/streaming_watt_sn.py
```

### `hive_setup.sql` — Tables et vues Hive

```bash
docker cp scripts/hive_setup.sql watt_hive:/tmp/hive_setup.sql
docker exec -it watt_hive beeline -u jdbc:hive2://localhost:10000 -n hive -p hive123 -f /tmp/hive_setup.sql
```

Crée 3 tables ORC/SNAPPY partitionnées + 3 vues analytiques :
- `conso_historique` — historique consommation par zone
- `solaire_historique` — historique production solaire
- `alertes_historique` — historique des alertes
- `vue_risque_zone` — score de risque par zone
- `vue_solaire_optimisation` — optimisation injection solaire
- `vue_delestage_tournant` — recommandations délestage équitable

### `watt_sn_dag.py` — DAG Airflow MLOps

```bash
# Copier dans les containers Airflow
docker cp dags/watt_sn_dag.py watt_airflow_scheduler:/opt/airflow/dags/watt_sn_dag.py
docker cp dags/watt_sn_dag.py watt_airflow_webserver:/opt/airflow/dags/watt_sn_dag.py

# Vérifier la détection
docker exec -it watt_airflow_scheduler airflow dags list

# Activer le DAG
docker exec -it watt_airflow_scheduler airflow dags unpause watt_sn_dag
```

Pipeline DAG (schedule `@hourly`) :
```
verifier_services → collecter_metriques → decider_branchement
                                                  ├── trigger_retrain     (si ROUGE > 2)
                                                  └── monitoring_normal   (sinon)
                                                          └── notifier_resultat
```

---

## 🔀 Conventions Git

### Branches

```
main              ← stable, merge via Pull Request uniquement
dev-moustapha     ← Data Engineering & Infrastructure
dev-aissata       ← Data Science & ML & Streaming
```

### Format des commits (obligatoire)

```
[Sx] type: description courte en français

Exemples :
[S1] feat: docker-compose infrastructure complete 14 services
[S1] feat: hbase_setup.py tables watt:conso_temps_reel alertes solaire
[S2] feat: pipeline spark streaming classification ROUGE/ORANGE/VERT
[S2] fix: correction watermark 10 minutes Spark
[S2] feat: hbase_consumer.py pipeline Kafka->HBase
[S3] feat: DAG Airflow watt_sn_dag BranchPythonOperator retraining
[S3] docs: README avec captures et résultats obtenus
```

### Types de commits

| Type | Usage |
|------|-------|
| `feat` | Nouvelle fonctionnalité |
| `fix` | Correction de bug |
| `docs` | Documentation |
| `perf` | Amélioration performance |
| `chore` | Tâche de maintenance |
| `test` | Ajout de tests |

### Workflow quotidien

```bash
# 1. Toujours travailler sur sa branche
git checkout dev-moustapha   # ou dev-aissata

# 2. Récupérer les mises à jour du binôme
git fetch origin
git merge origin/dev-aissata   # pour intégrer le travail d'Aissata

# 3. Travailler, committer
git add scripts/mon_fichier.py
git commit -m "[S2] feat: pipeline spark streaming"
git push origin dev-moustapha

# 4. En fin de sprint → Pull Request vers main sur GitHub
# Ne jamais push directement sur main !
```

---

## 🔄 Workflow de développement

### Sprint 1 — Infrastructure & Ingestion ✅

```
[Moustapha] docker-compose.yml (14 services) → Kafka 3 topics → HBase 3 tables
[Aissata]   schema.py (Pandera) → kafka_producer_watt_sn.py (seed=42, 7 zones)
```

### Sprint 2 — Streaming & ML ✅

```
[Moustapha] hive_setup.sql (3 tables ORC + 3 vues) → hbase_consumer.py (Kafka→HBase)
[Aissata]   streaming_watt_sn.py (Spark Streaming, SHA-256, ROUGE/ORANGE/VERT)
```

### Sprint 3 — Dashboard, Airflow & Soutenance 🔄

```
[Moustapha] watt_sn_dag.py (DAG Airflow @hourly, BranchPythonOperator) ✅
[Aissata]   dashboard_watt_sn.py → recommandations par zone
[Les deux]  rapport.pdf → README final → tag v1.0 → PR vers main
```

---

## 🐛 Résolution de problèmes fréquents

### Kafka unhealthy au démarrage
```bash
# C'est souvent un faux positif — vérifier manuellement
docker logs watt_kafka --tail 20
# Si les logs montrent "started", c'est OK
docker compose restart kafka zookeeper
```

### HBase : NamespaceNotFoundException
```bash
# Créer le namespace manuellement avant hbase_setup.py
docker exec -it watt_hbase hbase shell
create_namespace 'watt'
exit
```

### Spark : Failed to find data source: kafka
```bash
# Les JARs Kafka ne sont pas dans le container — les copier manuellement
# Télécharger depuis Maven puis :
docker cp ~/Downloads/spark-sql-kafka-0-10_2.12-3.5.1.jar watt_spark_master:/opt/spark/jars/
docker cp ~/Downloads/kafka-clients-3.4.1.jar watt_spark_master:/opt/spark/jars/
docker cp ~/Downloads/spark-token-provider-kafka-0-10_2.12-3.5.1.jar watt_spark_master:/opt/spark/jars/
docker cp ~/Downloads/commons-pool2-2.11.1.jar watt_spark_master:/opt/spark/jars/
```

### Spark : ClassCastException / SerializedLambda
```bash
# Incompatibilité master/worker — utiliser le mode local
/opt/spark/bin/spark-submit --master local[2] /opt/spark/work-dir/streaming_watt_sn.py
```

### happybase : No module found dans Spark
```bash
# happybase ne peut pas être installé dans le container (pas d'internet)
# Utiliser hbase_consumer.py sur la machine hôte à la place
python scripts/hbase_consumer.py
```

### Hive : Permission denied sur /user/hive/warehouse
```bash
docker exec --user root -it watt_hive bash -c "mkdir -p /user/hive/warehouse && chmod -R 777 /user/hive"
```

### Hive metastore : Authentication type 10 not supported
```bash
# Utiliser postgres:13-alpine (pas 14) dans docker-compose.yml
# La version 14 est incompatible avec le JDBC Hive
```

### Airflow DAG non visible dans l'interface
```bash
# Copier dans les deux containers
docker cp dags/watt_sn_dag.py watt_airflow_scheduler:/opt/airflow/dags/watt_sn_dag.py
docker cp dags/watt_sn_dag.py watt_airflow_webserver:/opt/airflow/dags/watt_sn_dag.py
# Vérifier la détection
docker exec -it watt_airflow_scheduler airflow dags list
```

### Commandes multi-lignes dans docker-compose
```yaml
# ❌ Ne pas faire (cause des erreurs de parsing)
entrypoint: |
  "commande \
   suite"

# ✅ Toujours tout sur une seule ligne
entrypoint: ["/bin/bash", "-c", "commande suite"]
```

### Port déjà utilisé
```bash
# Identifier le processus
lsof -i :PORT
# Modifier le mapping dans docker-compose.yml
# Exemple : HBase REST → 8086:8085 (au lieu de 8085:8085)
```

---

## 📊 Barème & Livrables

| Critère | Points | Objectif | Statut |
|---------|--------|---------|--------|
| Ingestion NiFi 3 sources | 3 pts | Senelec + météo + GIS → 2 topics Kafka | 🔄 |
| Validation & Privacy | 4 pts | Pandera + SHA-256 + 0 PII dans HBase | ✅ |
| Streaming & Prédiction | 6 pts | RandomForest + watermarks + RMSE documenté | ✅ |
| Alertes & Hive | 4 pts | Alertes HBase live + vues opérationnelles | ✅ |
| Recommandations | 3 pts | Délestage tournant équitable par zone | 🔄 |
| **Total** | **20 pts** | | |

**Bonus documentation (jusqu'à +2 pts) :**

| Bonus | Points | Statut |
|-------|--------|--------|
| README complet avec captures d'écran | +0.5 pt | ✅ |
| Commentaires code en français + docstrings | +0.5 pt | ✅ |
| Tests pytest sur schémas Pandera | +0.5 pt | 🔄 |
| Démo live alerte HBase en soutenance | +0.5 pt | 🔄 |

### Deadline

> 🗓️ **Vendredi semaine 4 à 23h59** — Retard : -2 pts/jour ouvrable

| Canal | Contenu |
|-------|---------|
| **GitHub** | Code sur `sunulabo/uadb-m2-watt-sn` avec historique commits |
| **Moodle** | `Projet_WattSN_WoneAissata_MbayeMoustapha.zip` |
| **Soutenance** | 10 min présentation + 5 min questions — NiFi visible sur :8081 |

---

*⚡ "Always be a solution, never a problem." — Mr Ahmed Ben Sidy Bouya SEYE*