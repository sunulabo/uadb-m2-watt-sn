"""
hbase_consumer.py — Consommateur Kafka → HBase
Lit les alertes depuis watt_alerts et les écrit dans HBase
Équipe 06 | Master 2 DSGL | UADB Bambey 2025-2026
"""

import json
import happybase
from kafka import KafkaConsumer
from datetime import datetime

KAFKA_BROKER = 'localhost:9092'
HBASE_HOST   = 'localhost'
HBASE_PORT   = 9090
TOPIC        = 'watt_alerts'

def ecrire_hbase(table, record):
    """Écrit une alerte dans HBase."""
    zone    = record.get('zone', 'INCONNU')
    niveau  = record.get('risque_delestage', 'VERT')

    if niveau == 'VERT':
        return  # On ne stocke pas les alertes vertes

    ts      = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    row_key = f"{zone}#{ts}#{niveau}".encode()
    seuil   = 10000.0 if niveau == 'ROUGE' else 5000.0

    table.put(row_key, {
        b'info:zone':         zone.encode(),
        b'info:timestamp':    ts.encode(),
        b'info:niveau':       niveau.encode(),
        b'alerte:seuil':      str(seuil).encode(),
        b'alerte:valeur':     str(record.get('conso_totale_mw', 0)).encode(),
        b'alerte:message':    f"Alerte {niveau} zone {zone}".encode(),
        b'action:type':       b'delestage_tournant',
        b'action:zone_cible': zone.encode(),
    })
    print(f"[HBase] ✅ Alerte {niveau} écrite — zone {zone}")

def main():
    print("🔌 Connexion HBase...")
    connection = happybase.Connection(
        host=HBASE_HOST,
        port=HBASE_PORT,
        timeout=10000,
        transport='buffered',
        protocol='binary',
    )
    table = connection.table('watt:alertes')
    print("✅ HBase connecté")

    print(f"📡 Écoute Kafka topic '{TOPIC}'...")
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='watt-hbase-writer'
    )

    print("🚀 Pipeline Kafka → HBase démarré...")
    for message in consumer:
        try:
            record = message.value
            ecrire_hbase(table, record)
        except Exception as e:
            print(f"[ERREUR] {e}")

if __name__ == '__main__':
    main()