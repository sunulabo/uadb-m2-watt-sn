# kafka_producer_watt.py — Simulateur consommation Senelec + production solaire
from kafka import KafkaProducer
import json, random, time, uuid
from datetime import datetime
import numpy as np
random.seed(42); np.random.seed(42)
ZONES =['DAKAR_NORD','DAKAR_SUD','THIES','SAINT_LOUIS','ZIGUINCHOR','KAOLACK','TAMBACOUNDA']
# Capacité installée par zone (MW) et profil solaire
CAPA_SOLAIRE = {'DAKAR_NORD':80,'DAKAR_SUD':60,'THIES':120,'SAINT_LOUIS':150,'ZIGUINCHOR':40,'KAOLACK':90,'TAMBACOUNDA':110}
CONSO_BASE= {'DAKAR_NORD':320,'DAKAR_SUD':280,'THIES':95,'SAINT_LOUIS':75,'ZIGUINCHOR':35,'KAOLACK':80,'TAMBACOUNDA':60}
producer = KafkaProducer(bootstrap_servers=['localhost:9092']
                        ,value_serializer=lambda v: json.dumps(v).encode())
def gen_conso(zone, heure):
    base = CONSO_BASE[zone]
    # Pic matin 7-9h et soir 18-22h
    facteur = 1.0 + 0.4*np.sin(np.pi*(heure-7)/12) if 7<=heure<=22 else 0.7
    conso= round(base * facteur * random.uniform(0.9,1.1), 2)
    return {'compteur_id': f'CPT_{zone}_{uuid.uuid4().hex[:8].upper()}',
            'zone':zone, 'consommation_mw':conso,
            'heure':heure, 'timestamp':datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}

def gen_solaire(zone, heure):
    m = datetime.utcnow().month
    # Production solaire : pic 10h-15h, nuit=0
    prod = max(0, CAPA_SOLAIRE[zone] * np.sin(np.pi*(heure-6)/12)
               * (0.8 + 0.2*np.sin(np.pi*m/6)) # saisonnalité
               * random.uniform(0.7,1.0))# ensoleillement aléatoire
    return {'zone':zone, 'production_mw':round(prod,2),
            'capacite_installee_mw':CAPA_SOLAIRE[zone],
            'heure':heure,'timestamp':datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}

if __name__ == '__main__':

    print('Simulateur Watt-SN démarré...')

    while True:

        h = datetime.utcnow().hour

        for zone in ZONES:

            conso = gen_conso(zone, h)
            solaire = gen_solaire(zone, h)

            print("Envoi :", zone)

            #producer.send('watt_conso_raw', conso)
            # future = producer.send('watt_conso_raw', gen_conso(zone, h))
            # print(future.get(timeout=10))
            # producer.send('watt_solaire_raw', solaire)
            future = producer.send('watt_conso_raw', gen_conso(zone, h))

            metadata = future.get(timeout=10)

            print(
                    f"topic={metadata.topic}, "
                    f"partition={metadata.partition}, "
                    f"offset={metadata.offset}")
        
        producer.flush()

        print("Batch envoyé")

        time.sleep(5)