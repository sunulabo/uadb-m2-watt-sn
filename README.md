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
- [Équipe](#-équipe)
- [Prérequis](#-prérequis)
- [Installation](#-installation)
- [Démarrage](#-démarrage)
- [Services & Ports](#-services--ports)
- [Structure du projet](#-structure-du-projet)
- [Conventions Git](#-conventions-git)
- [Scripts Python](#-scripts-python)
- [Workflow de développement](#-workflow-de-développement)
- [Barème & Livrables](#-barème--livrables)

---

## 🌍 Contexte

Le Sénégal produit environ **800 MW** pour une demande nationale de **~1 200 MW**, générant des délestages fréquents qui coûtent **~2% du PIB**. Watt-SN est un pipeline Big Data complet qui :

- **Ingère** les données de consommation Senelec, météo et GIS en temps réel via Apache NiFi
- **Traite** les flux avec Spark Structured Streaming (fenêtres 1h, watermark 10min)
- **Classifie** les zones en **🔴 ROUGE** (>900 MW) / **🟠 ORANGE** (>700 MW) / **🟢 VERT**
- **Anonymise** les compteurs industriels via SHA-256 (Privacy by Design)
- **Prédit** les délestages 24h à l'avance avec RandomForest (MLlib)
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
│  • RandomForest MLlib           • RMSE documenté                    │
└──────────────┬───────────────────────────────┬──────────────────────┘
               │                               │
               ▼                               ▼
┌──────────────────────────┐   ┌───────────────────────────────────────┐
│     APACHE HBASE :16010  │   │         APACHE HIVE :10000            │
│  Alertes temps réel      │   │  conso_historique (ORC/SNAPPY)        │
│  watt:conso_temps_reel   │   │  vue_risque_zone                      │
│  watt:alertes            │   │  vue_solaire_optimisation             │
│  watt:solaire            │   └───────────────────────────────────────┘
└──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    APACHE AIRFLOW :8083                             │
│  DAG watt_sn_dag.py — schedule @hourly                             │
│  BranchPythonOperator → retraining si zones ROUGE > 2              │
└─────────────────────────────────────────────────────────────────────┘
```

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

### Initialiser HBase (première fois uniquement)

```bash
# Créer le namespace 'watt' dans HBase shell
docker exec -it watt_hbase hbase shell
# Dans le shell HBase :
create_namespace 'watt'
exit

# Créer les tables
cd ..
python scripts/hbase_setup.py
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
│   └── docker-compose.yml          # Infrastructure complète (14 services)
│
├── scripts/
│   ├── hbase_setup.py              # Création tables HBase ✅
│   ├── schema.py                   # Schéma Pandera + validation
│   ├── kafka_producer_watt_sn.py   # Simulateur données (seed=42)
│   ├── streaming_watt_sn.py        # Pipeline Spark Streaming
│   ├── train_random_forest.py      # Entraînement modèle ML
│   └── hive_setup.sql              # Tables + vues Hive
│
├── dags/
│   └── watt_sn_dag.py              # DAG Airflow MLOps
│
├── nifi_templates/
│   └── template_nifi_equipe06.xml  # Template NiFi exporté
│
├── dashboard/
│   ├── dashboard_watt_sn.py        # Dashboard Matplotlib
│   └── *.png                       # Graphiques générés
│
├── rapport/
│   └── rapport.pdf                 # Livrable final (5-7 pages)
│
├── data/                           # Données simulées (non commitées)
├── models/                         # Modèles .pkl (non commités)
├── .venv/                          # Environnement Python (non commité)
├── requirements.txt                # Dépendances Python ✅
├── .gitignore                      # Exclusions Git ✅
└── README.md                       # Ce fichier ✅
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
[S3] docs: README avec captures NiFi et résultats RMSE
[S3] perf: RMSE 142 → 98 RandomForest optimisé
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

## 🐍 Scripts Python

### `hbase_setup.py` — Initialisation HBase

```bash
python scripts/hbase_setup.py
```

Crée 3 tables dans le namespace `watt` :
- `watt:conso_temps_reel` — familles : `info`, `conso`
- `watt:alertes` — familles : `info`, `alerte`, `action`
- `watt:solaire` — familles : `info`, `solaire`, `prevision`

### `kafka_producer_watt_sn.py` — Simulateur de données

```bash
python scripts/kafka_producer_watt_sn.py
```

- **seed=42** pour la reproductibilité
- **7 zones** : DAKAR_NORD, DAKAR_SUD, THIES, SAINT_LOUIS, ZIGUINCHOR, KAOLACK, TAMBACOUNDA
- Profil consommation jour/nuit réaliste
- Envoie vers `watt_conso_raw` et `watt_solaire_raw` toutes les 5 secondes

### `streaming_watt_sn.py` — Pipeline Spark Streaming

```bash
spark-submit \
  --master spark://localhost:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  scripts/streaming_watt_sn.py
```

- Fenêtres 1h avec slide 15 min
- Watermark 10 minutes
- Anonymisation SHA-256 des compteurs
- Classification ROUGE (>900 MW) / ORANGE (>700 MW) / VERT

### `hive_setup.sql` — Tables Hive

```bash
docker exec -it watt_hive beeline -u jdbc:hive2://localhost:10000 -f /scripts/hive_setup.sql
```

---

## 🔄 Workflow de développement

### Sprint 1 — Infrastructure & Ingestion ✅

```
[Moustapha] docker-compose.yml → Kafka topics → NiFi → HBase setup
[Aissata]   schema.py (Pandera) → kafka_producer_watt_sn.py
```

### Sprint 2 — Streaming & ML

```
[Moustapha] hive_setup.sql → vues analytiques → connexion Spark→HBase
[Aissata]   streaming_watt_sn.py → anonymisation SHA-256 → classification
```

### Sprint 3 — Dashboard & Soutenance

```
[Moustapha] DAG Airflow watt_sn_dag.py → retraining automatique
[Aissata]   dashboard_watt_sn.py → recommandations par zone
[Les deux]  rapport.pdf → README final → tag v1.0 → PR vers main
```

---

## 📊 Barème & Livrables

| Critère | Points | Objectif |
|---------|--------|---------|
| Ingestion NiFi 3 sources | 3 pts | Senelec + météo + GIS → 2 topics Kafka |
| Validation & Privacy | 4 pts | Pandera + SHA-256 + 0 PII dans HBase |
| Streaming & Prédiction | 6 pts | RandomForest + watermarks + RMSE documenté |
| Alertes & Hive | 4 pts | Alertes HBase live + vues opérationnelles |
| Recommandations | 3 pts | Délestage tournant équitable par zone |
| **Total** | **20 pts** | |

**Bonus documentation (jusqu'à +2 pts) :**
- README complet avec captures d'écran : +0.5 pt
- Commentaires code en français + docstrings : +0.5 pt
- Tests pytest sur schémas Pandera : +0.5 pt
- Démo live alerte HBase en soutenance : +0.5 pt

### Deadline

> 🗓️ **Vendredi semaine 4 à 23h59** — Retard : -2 pts/jour ouvrable

| Canal | Contenu |
|-------|---------|
| GitHub | Code sur `sunulabo/uadb-m2-watt-sn` avec historique commits |
| Moodle | `Projet_WattSN_WoneAissata_MbayeMoustapha.zip` |
| Soutenance | 10 min présentation + 5 min questions — NiFi visible sur :8081 |

---

## 🐛 Résolution de problèmes fréquents

### Kafka unhealthy au démarrage
```bash
# C'est souvent un faux positif — vérifier manuellement
docker logs watt_kafka --tail 20
# Si les logs montrent "Started", relancer simplement :
docker compose up -d kafka
```

### HBase : NamespaceNotFoundException
```bash
# Créer le namespace manuellement
docker exec -it watt_hbase hbase shell
create_namespace 'watt'
exit
```

### Port déjà utilisé
```bash
# Identifier le processus
lsof -i :PORT
# Changer le mapping dans docker-compose.yml
```

### NiFi lent au démarrage
```bash
# NiFi prend 3-5 minutes — vérifier les logs
docker logs watt_nifi --tail 10
# Attendre que la ligne "Started" apparaisse
```

### Commandes multi-lignes dans docker-compose
```yaml
# ❌ Ne pas faire (cause des erreurs de parsing)
command: |
  "kafka-topics --create \
   --topic mon_topic"

# ✅ Toujours tout sur une seule ligne
entrypoint: ["/bin/bash", "-c", "kafka-topics --create --topic mon_topic"]
```

---

*⚡ "Always be a solution, never a problem." — Mr Ahmed Ben Sidy Bouya SEYE*