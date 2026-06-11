"""
hbase_consumer.py — Consommateur Kafka → HBase
Lit les alertes depuis watt_alerts et les écrit dans HBase
Équipe 06 | Master 2 DSGL | UADB Bambey 2025-2026
"""

import json
import time
import happybase
from kafka import KafkaConsumer
from datetime import datetime

import os
KAFKA_BROKER  = os.environ.get('KAFKA_BROKER', 'localhost:9092')
HBASE_HOST    = os.environ.get('HBASE_HOST',   'localhost')
HBASE_PORT    = int(os.environ.get('HBASE_PORT', '9090'))
TOPIC         = 'watt_alerts'
WRITE_DELAY_S = float(os.environ.get('WRITE_DELAY_S', '0.05'))  # 50 ms entre chaque écriture

def ecrire_hbase(table, record):
    """Écrit une alerte dans HBase."""
    zone    = record.get('zone', 'INCONNU')
    niveau  = record.get('risque_delestage', 'VERT')

    if niveau == 'VERT':
        return  # On ne stocke pas les alertes vertes

    window  = record.get('window', {})
    ts      = window.get('start', datetime.utcnow().isoformat())
    if hasattr(ts, 'isoformat'):
        ts = ts.isoformat()
    ts = str(ts).replace(' ', 'T')[:19]
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
    time.sleep(WRITE_DELAY_S)

def connect_hbase():
    conn = happybase.Connection(
        host=HBASE_HOST,
        port=HBASE_PORT,
        timeout=10000,
        transport='buffered',
        protocol='binary',
    )
    return conn, conn.table('watt:alertes')

def main():
    print("🔌 Connexion HBase...")
    connection, table = connect_hbase()
    print("✅ HBase connecté")

    print(f"📡 Connexion Kafka {KAFKA_BROKER} topic '{TOPIC}'...")
    from kafka import TopicPartition
    tp = TopicPartition(TOPIC, 0)
    consumer = KafkaConsumer(
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    )
    consumer.assign([tp])
    consumer.seek_to_end(tp)
    print("🚀 Pipeline Kafka → HBase démarré (lecture des nouveaux messages)...")
    for message in consumer:
        try:
            record = message.value
            ecrire_hbase(table, record)
        except Exception as e:
            print(f"[ERREUR] {e} — reconnexion HBase dans 3s...")
            time.sleep(3)
            try:
                connection.close()
            except Exception:
                pass
            connection, table = connect_hbase()
            print("✅ HBase reconnecté")

if __name__ == '__main__':
    main()