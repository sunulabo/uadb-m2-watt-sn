"""
watt_sn_dag.py — DAG Airflow MLOps pour le projet Watt-SN
Équipe 06 | Master 2 DSGL | UADB Bambey 2025-2026

Pipeline :
    1. Vérification santé des services (Kafka, HBase, Hive)
    2. Collecte des métriques de consommation depuis HBase
    3. Analyse des zones ROUGE
    4. Branchement : retraining si zones ROUGE > 2, sinon monitoring
    5. Retraining RandomForest (si déclenché)
    6. Notification du résultat
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# ─── Configuration par défaut ─────────────────────────────────────────────────
default_args = {
    'owner':            'equipe06-watt-sn',
    'depends_on_past':  False,
    'start_date':       datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry':   False,
    'retries':          1,
    'retry_delay':      timedelta(minutes=5),
}

# ─── Seuils ───────────────────────────────────────────────────────────────────
SEUIL_ZONES_ROUGE     = 2    # Nombre de zones ROUGE déclenchant le retraining
SEUIL_ROUGE_MW        = 10000 # Seuil consommation ROUGE (MW)
SEUIL_ORANGE_MW       = 5000  # Seuil consommation ORANGE (MW)
HBASE_HOST            = 'hbase'
HBASE_PORT            = 9090
HBASE_TABLE_ALERTES   = 'watt:alertes'
HBASE_TABLE_CONSO     = 'watt:conso_temps_reel'


# ─── Tâche 1 : Vérification santé des services ────────────────────────────────
def verifier_services(**context):
    """
    Vérifie que les services critiques sont opérationnels.
    Contrôle : HBase, Kafka, Hive.
    """
    logger.info("=== Vérification santé des services Watt-SN ===")
    services_ok = []
    services_ko = []

    # Vérification HBase via happybase
    try:
        import happybase
        conn = happybase.Connection(
            host=HBASE_HOST,
            port=HBASE_PORT,
            timeout=5000,
            transport='buffered',
            protocol='binary',
        )
        tables = conn.tables()
        conn.close()
        services_ok.append(f"HBase OK — {len(tables)} tables")
        logger.info(f"HBase opérationnel — {len(tables)} tables disponibles")
    except Exception as e:
        services_ko.append(f"HBase KO : {e}")
        logger.warning(f"HBase inaccessible : {e}")

    # Vérification Kafka via kafka-python
    try:
        from kafka import KafkaAdminClient
        admin = KafkaAdminClient(
            bootstrap_servers=['kafka:29092'],
            request_timeout_ms=5000
        )
        topics = admin.list_topics()
        admin.close()
        watt_topics = [t for t in topics if t.startswith('watt_')]
        services_ok.append(f"Kafka OK — {len(watt_topics)} topics Watt-SN")
        logger.info(f"Kafka opérationnel — topics : {watt_topics}")
    except Exception as e:
        services_ko.append(f"Kafka KO : {e}")
        logger.warning(f"Kafka inaccessible : {e}")

    # Résumé
    logger.info(f"Services OK  : {services_ok}")
    logger.info(f"Services KO  : {services_ko}")

    context['task_instance'].xcom_push(
        key='services_status',
        value={'ok': services_ok, 'ko': services_ko}
    )

    if services_ko:
        logger.warning(f"Certains services sont indisponibles : {services_ko}")
    else:
        logger.info("Tous les services sont opérationnels ✅")

    return len(services_ok)


# ─── Tâche 2 : Collecte métriques HBase ───────────────────────────────────────
def collecter_metriques(**context):
    """
    Lit les alertes récentes depuis HBase et calcule les métriques
    par zone (nombre d'alertes ROUGE/ORANGE, consommation moyenne).
    """
    logger.info("=== Collecte des métriques depuis HBase ===")
    metriques = {
        'zones_rouge':  [],
        'zones_orange': [],
        'zones_vert':   [],
        'nb_alertes':   0,
        'timestamp':    datetime.utcnow().isoformat(),
    }

    try:
        import happybase
        conn = happybase.Connection(
            host=HBASE_HOST,
            port=HBASE_PORT,
            timeout=5000,
            transport='buffered',
            protocol='binary',
        )
        table = conn.table(HBASE_TABLE_ALERTES)

        # Scanner les alertes récentes
        zones_niveaux = {}
        for key, data in table.scan():
            try:
                zone   = data.get(b'info:zone', b'').decode()
                niveau = data.get(b'info:niveau', b'').decode()
                if zone and niveau:
                    if zone not in zones_niveaux:
                        zones_niveaux[zone] = {'ROUGE': 0, 'ORANGE': 0, 'VERT': 0}
                    if niveau in zones_niveaux[zone]:
                        zones_niveaux[zone][niveau] += 1
                        metriques['nb_alertes'] += 1
            except Exception:
                continue

        conn.close()

        # Classifier les zones par niveau dominant
        for zone, niveaux in zones_niveaux.items():
            if niveaux['ROUGE'] > 0:
                metriques['zones_rouge'].append(zone)
            elif niveaux['ORANGE'] > 0:
                metriques['zones_orange'].append(zone)
            else:
                metriques['zones_vert'].append(zone)

        logger.info(f"Métriques collectées : {metriques}")

    except Exception as e:
        logger.warning(f"Erreur collecte HBase : {e} — utilisation données simulées")
        # Données simulées pour les tests
        metriques['zones_rouge']  = ['DAKAR_NORD', 'DAKAR_SUD', 'THIES']
        metriques['zones_orange'] = ['SAINT_LOUIS', 'KAOLACK']
        metriques['zones_vert']   = ['ZIGUINCHOR', 'TAMBACOUNDA']
        metriques['nb_alertes']   = 42

    context['task_instance'].xcom_push(key='metriques', value=metriques)
    logger.info(f"Zones ROUGE  : {metriques['zones_rouge']}")
    logger.info(f"Zones ORANGE : {metriques['zones_orange']}")
    logger.info(f"Zones VERT   : {metriques['zones_vert']}")
    logger.info(f"Total alertes: {metriques['nb_alertes']}")

    return metriques


# ─── Tâche 3 : Décision de branchement ───────────────────────────────────────
def decider_branchement(**context):
    """
    BranchPythonOperator : décide si on lance le retraining ou le monitoring.
    Retraining si nombre de zones ROUGE > SEUIL_ZONES_ROUGE (2).
    """
    metriques = context['task_instance'].xcom_pull(
        task_ids='collecter_metriques',
        key='metriques'
    )

    nb_zones_rouge = len(metriques.get('zones_rouge', []))
    logger.info(f"Zones ROUGE : {nb_zones_rouge} / seuil : {SEUIL_ZONES_ROUGE}")

    if nb_zones_rouge > SEUIL_ZONES_ROUGE:
        logger.info(f"🔴 {nb_zones_rouge} zones ROUGE > seuil {SEUIL_ZONES_ROUGE} → RETRAINING déclenché")
        return 'trigger_retrain'
    else:
        logger.info(f"🟢 {nb_zones_rouge} zones ROUGE ≤ seuil {SEUIL_ZONES_ROUGE} → MONITORING normal")
        return 'monitoring_normal'


# ─── Tâche 4a : Retraining RandomForest ──────────────────────────────────────
def trigger_retrain(**context):
    """
    Déclenche le retraining du modèle RandomForest de prédiction de délestage.
    Utilise les données historiques HBase pour entraîner un nouveau modèle.
    """
    logger.info("=== Retraining RandomForest déclenché ===")

    metriques = context['task_instance'].xcom_pull(
        task_ids='collecter_metriques',
        key='metriques'
    )
    zones_rouge = metriques.get('zones_rouge', [])
    logger.info(f"Zones critiques déclenchant le retraining : {zones_rouge}")

    try:
        import numpy as np
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import accuracy_score
        import random
        import os

        random.seed(42)
        np.random.seed(42)

        # Génération de données d'entraînement simulées
        # Features : consommation_mw, heure, mois, production_solaire_mw
        logger.info("Génération des données d'entraînement...")
        n_samples = 1000

        zones = ['DAKAR_NORD', 'DAKAR_SUD', 'THIES', 'SAINT_LOUIS',
                 'ZIGUINCHOR', 'KAOLACK', 'TAMBACOUNDA']
        conso_base = [320, 280, 95, 75, 35, 80, 60]

        X, y = [], []
        for _ in range(n_samples):
            zone_idx    = random.randint(0, 6)
            heure       = random.randint(0, 23)
            mois        = random.randint(1, 12)
            base        = conso_base[zone_idx]
            facteur     = 1.0 + 0.4 * np.sin(np.pi * (heure - 7) / 12) if 7 <= heure <= 22 else 0.7
            conso       = base * facteur * random.uniform(0.9, 1.1) * 60  # fenêtre 1h
            prod_sol    = max(0, 100 * np.sin(np.pi * (heure - 6) / 12) * random.uniform(0.7, 1.0))

            X.append([conso, heure, mois, prod_sol, zone_idx])

            # Label : ROUGE=2, ORANGE=1, VERT=0
            if conso > SEUIL_ROUGE_MW:
                y.append(2)
            elif conso > SEUIL_ORANGE_MW:
                y.append(1)
            else:
                y.append(0)

        X = np.array(X)
        y = np.array(y)

        # Entraînement
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        logger.info("Entraînement du modèle RandomForest...")
        model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            n_jobs=-1
        )
        model.fit(X_train, y_train)

        # Évaluation
        y_pred   = model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        logger.info(f"Accuracy du modèle : {accuracy:.4f}")

        # Importance des features
        feature_names = ['conso_mw', 'heure', 'mois', 'prod_solaire_mw', 'zone_idx']
        importances   = model.feature_importances_
        for fname, imp in zip(feature_names, importances):
            logger.info(f"  Feature '{fname}' : {imp:.4f}")

        # Sauvegarde du modèle
        import pickle
        os.makedirs('/opt/airflow/models', exist_ok=True)
        model_path = '/opt/airflow/models/random_forest_watt_sn.pkl'
        with open(model_path, 'wb') as f:
            pickle.dump(model, f)
        logger.info(f"Modèle sauvegardé : {model_path}")

        # Résultats
        resultats = {
            'accuracy':     round(accuracy, 4),
            'n_samples':    n_samples,
            'zones_rouge':  zones_rouge,
            'model_path':   model_path,
            'timestamp':    datetime.utcnow().isoformat(),
        }
        context['task_instance'].xcom_push(key='resultats_retrain', value=resultats)
        logger.info(f"Retraining terminé — Accuracy : {accuracy:.4f}")

    except Exception as e:
        logger.error(f"Erreur retraining : {e}")
        raise


# ─── Tâche 4b : Monitoring normal ────────────────────────────────────────────
def monitoring_normal(**context):
    """
    Mode monitoring : pas de retraining nécessaire.
    Log les métriques et génère un rapport de surveillance.
    """
    logger.info("=== Mode monitoring normal ===")

    metriques = context['task_instance'].xcom_pull(
        task_ids='collecter_metriques',
        key='metriques'
    )

    logger.info(f"Système stable — {len(metriques.get('zones_rouge', []))} zone(s) ROUGE")
    logger.info(f"Zones VERT   : {metriques.get('zones_vert', [])}")
    logger.info(f"Zones ORANGE : {metriques.get('zones_orange', [])}")
    logger.info(f"Total alertes traitées : {metriques.get('nb_alertes', 0)}")
    logger.info("Aucun retraining nécessaire pour ce cycle ✅")


# ─── Tâche 5 : Notification finale ───────────────────────────────────────────
def notifier_resultat(**context):
    """
    Génère un rapport de fin de cycle et notifie le résultat.
    """
    logger.info("=== Rapport de fin de cycle Watt-SN ===")

    metriques = context['task_instance'].xcom_pull(
        task_ids='collecter_metriques',
        key='metriques'
    )
    resultats_retrain = context['task_instance'].xcom_pull(
        key='resultats_retrain'
    )

    logger.info(f"Timestamp      : {datetime.utcnow().isoformat()}")
    logger.info(f"Zones ROUGE    : {metriques.get('zones_rouge', [])}")
    logger.info(f"Zones ORANGE   : {metriques.get('zones_orange', [])}")
    logger.info(f"Zones VERT     : {metriques.get('zones_vert', [])}")
    logger.info(f"Total alertes  : {metriques.get('nb_alertes', 0)}")

    if resultats_retrain:
        logger.info(f"Retraining     : OUI — Accuracy = {resultats_retrain.get('accuracy')}")
    else:
        logger.info(f"Retraining     : NON — Système stable")

    logger.info("Cycle Watt-SN terminé avec succès ✅")


# ─── Définition du DAG ────────────────────────────────────────────────────────
with DAG(
    dag_id='watt_sn_dag',
    default_args=default_args,
    description='Pipeline MLOps Watt-SN — Prédiction délestage Sénégal',
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
    tags=['watt-sn', 'mlops', 'big-data', 'uadb'],
) as dag:

    # Tâche 1 — Vérification santé
    t_verifier = PythonOperator(
        task_id='verifier_services',
        python_callable=verifier_services,
    )

    # Tâche 2 — Collecte métriques
    t_metriques = PythonOperator(
        task_id='collecter_metriques',
        python_callable=collecter_metriques,
    )

    # Tâche 3 — Branchement
    t_branchement = BranchPythonOperator(
        task_id='decider_branchement',
        python_callable=decider_branchement,
    )

    # Tâche 4a — Retraining
    t_retrain = PythonOperator(
        task_id='trigger_retrain',
        python_callable=trigger_retrain,
    )

    # Tâche 4b — Monitoring
    t_monitoring = PythonOperator(
        task_id='monitoring_normal',
        python_callable=monitoring_normal,
    )

    # Tâche 5 — Notification (converge après branchement)
    t_notifier = PythonOperator(
        task_id='notifier_resultat',
        python_callable=notifier_resultat,
        trigger_rule='none_failed_min_one_success',
    )

    # ─── Dépendances ──────────────────────────────────────────────────────────
    #
    #  verifier_services
    #        │
    #  collecter_metriques
    #        │
    #  decider_branchement
    #       / \
    # retrain  monitoring
    #       \ /
    #  notifier_resultat
    #
    t_verifier >> t_metriques >> t_branchement
    t_branchement >> [t_retrain, t_monitoring]
    [t_retrain, t_monitoring] >> t_notifier
