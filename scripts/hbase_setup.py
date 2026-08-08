import happybase
import sys

# ─── Configuration ────────────────────────────────────────────────────────────
HBASE_HOST = "localhost"
HBASE_PORT = 9090  # Thrift port HBase (pas le port UI 16010)
NAMESPACE = "watt"

# ─── Définition des tables et familles de colonnes ───────────────────────────
TABLES = {
    # Table 1 : Consommation temps réel
    # Famille 'info'    → métadonnées (zone, timestamp, source)
    # Famille 'conso'   → données de consommation (valeur_mw, statut)
    f"{NAMESPACE}:conso_temps_reel": {
        "info": {"max_versions": 1},
        "conso": {"max_versions": 5},
    },
    # Table 2 : Alertes de délestage
    # Famille 'info'    → métadonnées (zone, timestamp, niveau)
    # Famille 'alerte'  → détails alerte (seuil, valeur, message)
    # Famille 'action'  → actions recommandées (type, zone_cible, duree)
    f"{NAMESPACE}:alertes": {
        "info": {"max_versions": 1},
        "alerte": {"max_versions": 10},
        "action": {"max_versions": 5},
    },
    # Table 3 : Production solaire
    # Famille 'info'      → métadonnées (zone, timestamp, source)
    # Famille 'solaire'   → données production (puissance_kw, irradiance)
    # Famille 'prevision' → prévisions ML (valeur_24h, confiance)
    f"{NAMESPACE}:solaire": {
        "info": {"max_versions": 1},
        "solaire": {"max_versions": 5},
        "prevision": {"max_versions": 3},
    },
}


def creer_namespace(connection: happybase.Connection) -> None:
    """Crée le namespace 'watt' s'il n'existe pas déjà."""
    try:
        namespaces = connection.namespaces()
        if NAMESPACE.encode() not in namespaces:
            connection.create_namespace(NAMESPACE)
            print(f"✅ Namespace '{NAMESPACE}' créé.")
        else:
            print(f"ℹ️  Namespace '{NAMESPACE}' existe déjà.")
    except Exception as e:
        print(f"⚠️  Namespace non supporté (mode standalone) : {e}")


def creer_tables(connection: happybase.Connection) -> None:
    """Crée les tables HBase définies dans TABLES."""
    tables_existantes = [t.decode() for t in connection.tables()]

    for nom_table, familles in TABLES.items():
        if nom_table in tables_existantes:
            print(f"ℹ️  Table '{nom_table}' existe déjà — ignorée.")
            continue

        try:
            connection.create_table(nom_table, familles)
            print(
                f"✅ Table '{nom_table}' créée avec familles : {list(familles.keys())}"
            )
        except Exception as e:
            print(f"❌ Erreur création table '{nom_table}' : {e}")
            sys.exit(1)


def inserer_donnees_test(connection: happybase.Connection) -> None:
    """
    Insère des données de test pour valider les tables.
    Simule une alerte ROUGE sur Dakar.
    """
    print("\n📝 Insertion de données de test...")

    try:
        # Test table conso_temps_reel
        table_conso = connection.table(f"{NAMESPACE}:conso_temps_reel")
        table_conso.put(
            b"dakar#2025-01-01T00:00:00",
            {
                b"info:zone": b"Dakar",
                b"info:timestamp": b"2025-01-01T00:00:00",
                b"info:source": b"senelec_api",
                b"conso:valeur_mw": b"920.5",
                b"conso:statut": b"ROUGE",
            },
        )
        print(f"  ✅ Test insertion table conso_temps_reel OK")

        # Test table alertes
        table_alertes = connection.table(f"{NAMESPACE}:alertes")
        table_alertes.put(
            b"dakar#2025-01-01T00:00:00#ROUGE",
            {
                b"info:zone": b"Dakar",
                b"info:timestamp": b"2025-01-01T00:00:00",
                b"info:niveau": b"ROUGE",
                b"alerte:seuil": b"900",
                b"alerte:valeur": b"920.5",
                b"alerte:message": b"Consommation critique - delestage imminent",
                b"action:type": b"delestage_tournant",
                b"action:zone_cible": b"Dakar_Nord",
                b"action:duree": b"2h",
            },
        )
        print(f"  ✅ Test insertion table alertes OK")

        # Test table solaire
        table_solaire = connection.table(f"{NAMESPACE}:solaire")
        table_solaire.put(
            b"dakar#2025-01-01T00:00:00",
            {
                b"info:zone": b"Dakar",
                b"info:timestamp": b"2025-01-01T00:00:00",
                b"info:source": b"capteur_gis",
                b"solaire:puissance_kw": b"150.3",
                b"solaire:irradiance": b"850.0",
                b"prevision:valeur_24h": b"145.0",
                b"prevision:confiance": b"0.87",
            },
        )
        print(f"  ✅ Test insertion table solaire OK")

    except Exception as e:
        print(f"❌ Erreur insertion test : {e}")
        sys.exit(1)


def verifier_tables(connection: happybase.Connection) -> None:
    """Vérifie que les tables sont accessibles et affiche leur contenu de test."""
    print("\n🔍 Vérification des tables...")

    for nom_table in TABLES.keys():
        try:
            table = connection.table(nom_table)
            nb_lignes = sum(1 for _ in table.scan())
            print(f"  ✅ '{nom_table}' : {nb_lignes} ligne(s)")
        except Exception as e:
            print(f"  ❌ Erreur lecture '{nom_table}' : {e}")


def afficher_resume() -> None:
    """Affiche le résumé des tables créées."""
    print("\n" + "=" * 60)
    print("📊 RÉSUMÉ — Tables HBase Watt-SN")
    print("=" * 60)
    print(f"{'Table':<35} {'Familles':<25}")
    print("-" * 60)
    for nom_table, familles in TABLES.items():
        print(f"{nom_table:<35} {', '.join(familles.keys()):<25}")
    print("=" * 60)
    print("\n🔗 Interface HBase : http://localhost:16010")
    print("🔗 Table Details  : http://localhost:16010/table.jsp")


def main():
    """Point d'entrée principal."""
    print("⚡ Watt-SN — Initialisation HBase")
    print(f"   Host : {HBASE_HOST}:{HBASE_PORT}")
    print("-" * 40)

    # Connexion HBase via Thrift
    try:
        connection = happybase.Connection(
            host=HBASE_HOST,
            port=HBASE_PORT,
            timeout=10000,
            autoconnect=True,
            transport="buffered",
            protocol="binary",
        )
        print(f"✅ Connexion HBase établie sur {HBASE_HOST}:{HBASE_PORT}")
    except Exception as e:
        print(f"❌ Impossible de se connecter à HBase : {e}")
        print(f"   Vérifiez que HBase tourne : docker compose ps hbase")
        sys.exit(1)

    try:
        # Étape 1 : Namespace
        creer_namespace(connection)

        # Étape 2 : Tables
        print("\n📁 Création des tables...")
        creer_tables(connection)

        # Étape 3 : Données de test
        inserer_donnees_test(connection)

        # Étape 4 : Vérification
        verifier_tables(connection)

        # Résumé
        afficher_resume()

        print("\n✅ Setup HBase terminé avec succès !")

    finally:
        connection.close()


if __name__ == "__main__":
    main()
