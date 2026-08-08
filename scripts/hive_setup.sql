-- ============================================================
-- hive_setup.sql — Tables et vues analytiques Hive pour Watt-SN
-- Équipe 06 | Master 2 DSGL | UADB Bambey 2025-2026
-- ============================================================
-- Usage :
--   docker exec -it watt_hive beeline -u jdbc:hive2://localhost:10000 \
--     -n hive -p hive123 -f /scripts/hive_setup.sql
-- ============================================================

-- ─── 1. BASE DE DONNÉES ───────────────────────────────────────
CREATE DATABASE IF NOT EXISTS watt_sn
COMMENT 'Base de donnees Watt-SN — Intelligence Energetique Senegal';

USE watt_sn;

-- ─── 2. TABLE PRINCIPALE — Consommation historique ────────────
-- Partitionnée par date et zone pour optimiser les requêtes analytiques
-- Format ORC + compression SNAPPY pour les performances
DROP TABLE IF EXISTS conso_historique;

CREATE EXTERNAL TABLE IF NOT EXISTS conso_historique (
    compteur_id       STRING    COMMENT 'Identifiant SHA-256 du compteur (anonymisé)',
    consommation_mw   DOUBLE    COMMENT 'Consommation en mégawatts',
    heure             INT       COMMENT 'Heure de la mesure (0-23)',
    timestamp_event   TIMESTAMP COMMENT 'Horodatage exact de la mesure',
    source            STRING    COMMENT 'Source des données (senelec_api, nifi, simulator)',
    statut_alerte     STRING    COMMENT 'Statut : ROUGE / ORANGE / VERT'
)
PARTITIONED BY (
    date_mesure       STRING    COMMENT 'Date de la mesure (YYYY-MM-DD)',
    zone              STRING    COMMENT 'Zone géographique (DAKAR_NORD, THIES, etc.)'
)
STORED AS ORC
TBLPROPERTIES (
    'orc.compress'              = 'SNAPPY',
    'orc.stripe.size'           = '67108864',
    'orc.row.index.stride'      = '10000',
    'transactional'             = 'false',
    'comment'                   = 'Historique consommation electrique par zone Senegal'
);

-- ─── 3. TABLE — Production solaire historique ─────────────────
DROP TABLE IF EXISTS solaire_historique;

CREATE EXTERNAL TABLE IF NOT EXISTS solaire_historique (
    zone                  STRING    COMMENT 'Zone géographique',
    production_mw         DOUBLE    COMMENT 'Production solaire en mégawatts',
    capacite_installee_mw DOUBLE    COMMENT 'Capacité installée en mégawatts',
    irradiance            DOUBLE    COMMENT 'Irradiance solaire (W/m²)',
    taux_utilisation      DOUBLE    COMMENT 'Taux utilisation capacité (0.0 à 1.0)',
    heure                 INT       COMMENT 'Heure de la mesure (0-23)',
    timestamp_event       TIMESTAMP COMMENT 'Horodatage exact'
)
PARTITIONED BY (
    date_mesure           STRING    COMMENT 'Date de la mesure (YYYY-MM-DD)',
    mois                  INT       COMMENT 'Mois (1-12) pour saisonnalité'
)
STORED AS ORC
TBLPROPERTIES (
    'orc.compress'  = 'SNAPPY',
    'transactional' = 'false',
    'comment'       = 'Historique production solaire par zone Senegal'
);

-- ─── 4. TABLE — Alertes historiques ───────────────────────────
DROP TABLE IF EXISTS alertes_historique;

CREATE EXTERNAL TABLE IF NOT EXISTS alertes_historique (
    zone              STRING    COMMENT 'Zone concernée par l alerte',
    niveau_alerte     STRING    COMMENT 'Niveau : ROUGE / ORANGE / VERT',
    consommation_mw   DOUBLE    COMMENT 'Consommation au moment de l alerte',
    seuil_mw          DOUBLE    COMMENT 'Seuil déclencheur de l alerte',
    depassement_mw    DOUBLE    COMMENT 'Dépassement par rapport au seuil',
    action_recommandee STRING   COMMENT 'Action recommandée par le système',
    timestamp_alerte  TIMESTAMP COMMENT 'Horodatage de l alerte',
    duree_minutes     INT       COMMENT 'Durée estimée du délestage en minutes'
)
PARTITIONED BY (
    date_alerte       STRING    COMMENT 'Date de l alerte (YYYY-MM-DD)',
    niveau            STRING    COMMENT 'Niveau alerte pour filtrage rapide'
)
STORED AS ORC
TBLPROPERTIES (
    'orc.compress'  = 'SNAPPY',
    'transactional' = 'false',
    'comment'       = 'Historique alertes delestage par zone Senegal'
);

-- ─── 5. VUE — Risque de délestage par zone ────────────────────
-- Agrège la consommation moyenne et identifie les zones à risque
-- Utilisée par le dashboard et le DAG Airflow
DROP VIEW IF EXISTS vue_risque_zone;

CREATE VIEW vue_risque_zone AS
SELECT
    zone,
    date_mesure,
    ROUND(AVG(consommation_mw), 2)                          AS conso_moyenne_mw,
    ROUND(MAX(consommation_mw), 2)                          AS conso_max_mw,
    ROUND(MIN(consommation_mw), 2)                          AS conso_min_mw,
    COUNT(*)                                                 AS nb_mesures,
    SUM(CASE WHEN statut_alerte = 'ROUGE'  THEN 1 ELSE 0 END) AS nb_alertes_rouge,
    SUM(CASE WHEN statut_alerte = 'ORANGE' THEN 1 ELSE 0 END) AS nb_alertes_orange,
    SUM(CASE WHEN statut_alerte = 'VERT'   THEN 1 ELSE 0 END) AS nb_alertes_vert,
    -- Niveau de risque global de la zone
    CASE
        WHEN AVG(consommation_mw) > 900 THEN 'CRITIQUE'
        WHEN AVG(consommation_mw) > 700 THEN 'ELEVE'
        WHEN AVG(consommation_mw) > 500 THEN 'MODERE'
        ELSE 'FAIBLE'
    END                                                      AS niveau_risque_global,
    -- Score de risque normalisé (0-100)
    ROUND(
        LEAST(100,
            (AVG(consommation_mw) / 1000.0) * 100 +
            (SUM(CASE WHEN statut_alerte = 'ROUGE' THEN 1 ELSE 0 END) * 5)
        ), 1
    )                                                        AS score_risque
FROM
    conso_historique
GROUP BY
    zone,
    date_mesure;

-- ─── 6. VUE — Optimisation injection solaire ──────────────────
-- Identifie les zones où l'injection solaire peut réduire le délestage
-- Croise consommation et production pour recommander l'injection
DROP VIEW IF EXISTS vue_solaire_optimisation;

CREATE VIEW vue_solaire_optimisation AS
SELECT
    s.zone,
    s.date_mesure,
    ROUND(AVG(s.production_mw), 2)                          AS production_moyenne_mw,
    ROUND(AVG(s.capacite_installee_mw), 2)                  AS capacite_installee_mw,
    ROUND(AVG(s.taux_utilisation) * 100, 1)                 AS taux_utilisation_pct,
    ROUND(AVG(c.consommation_mw), 2)                        AS conso_moyenne_mw,
    -- Déficit énergétique (positif = manque, négatif = surplus)
    ROUND(AVG(c.consommation_mw) - AVG(s.production_mw), 2) AS deficit_mw,
    -- Potentiel d'injection supplémentaire
    ROUND(AVG(s.capacite_installee_mw) - AVG(s.production_mw), 2) AS potentiel_injection_mw,
    -- Recommandation d'action
    CASE
        WHEN AVG(c.consommation_mw) > 900
             AND AVG(s.taux_utilisation) < 0.8
        THEN 'INJECTION_URGENTE — Zone critique, capacité solaire disponible'
        WHEN AVG(c.consommation_mw) > 700
             AND AVG(s.taux_utilisation) < 0.9
        THEN 'AUGMENTER_INJECTION — Zone tendue, optimiser solaire'
        WHEN AVG(s.taux_utilisation) > 0.95
        THEN 'CAPACITE_SATUREE — Envisager extension panneaux'
        ELSE 'NOMINAL — Production solaire suffisante'
    END                                                      AS recommandation,
    -- Économie CO2 estimée (kg/MWh solaire ≈ 820 kg évités vs charbon)
    ROUND(AVG(s.production_mw) * 0.82, 1)                   AS economie_co2_kg_h
FROM
    solaire_historique s
    JOIN conso_historique c
        ON s.zone = c.zone
        AND s.date_mesure = c.date_mesure
GROUP BY
    s.zone,
    s.date_mesure;

-- ─── 7. VUE — Délestage tournant équitable ────────────────────
-- Recommande un ordre de délestage équitable entre les zones
-- Basé sur la consommation relative et l'historique d'alertes
DROP VIEW IF EXISTS vue_delestage_tournant;

CREATE VIEW vue_delestage_tournant AS
SELECT
    zone,
    date_mesure,
    conso_moyenne_mw,
    nb_alertes_rouge,
    score_risque,
    -- Rang de priorité de délestage (1 = délester en premier)
    -- Les zones les plus consommatrices et à risque élevé sont délestées en priorité
    RANK() OVER (
        PARTITION BY date_mesure
        ORDER BY score_risque DESC, conso_moyenne_mw DESC
    )                                                        AS rang_priorite_delestage,
    -- Durée recommandée de délestage (en minutes)
    CASE
        WHEN score_risque > 80 THEN 120
        WHEN score_risque > 60 THEN 90
        WHEN score_risque > 40 THEN 60
        ELSE 30
    END                                                      AS duree_delestage_min,
    -- Plage horaire recommandée pour le délestage tournant
    CASE
        WHEN zone IN ('DAKAR_NORD', 'DAKAR_SUD') THEN '22h-00h (hors pic activité)'
        WHEN zone IN ('THIES', 'KAOLACK')        THEN '14h-16h (pic chaleur)'
        WHEN zone IN ('SAINT_LOUIS', 'ZIGUINCHOR', 'TAMBACOUNDA')
                                                 THEN '20h-22h (pic domestique)'
        ELSE '23h-01h'
    END                                                      AS plage_horaire_recommandee
FROM
    vue_risque_zone;

-- ─── 8. VÉRIFICATION ──────────────────────────────────────────
SHOW DATABASES;
USE watt_sn;
SHOW TABLES;
SHOW VIEWS;

SELECT 'hive_setup.sql execute avec succes !' AS statut;
