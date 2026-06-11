"""
dashboard_watt_sn.py — Dashboard Web Watt-SN
Interface de surveillance énergétique temps réel — 7 zones du Sénégal
Équipe 06 | Master 2 DSGL | UADB Bambey 2025-2026
"""

from flask import Flask, render_template, jsonify
import happybase
from collections import Counter
from datetime import datetime

app = Flask(__name__)

HBASE_HOST = "localhost"
HBASE_PORT = 9090

ZONES_SENEGAL = [
    'DAKAR_NORD', 'DAKAR_SUD', 'THIES',
    'SAINT_LOUIS', 'ZIGUINCHOR', 'KAOLACK', 'TAMBACOUNDA'
]

SEUIL_ROUGE_MW  = 10000
SEUIL_ORANGE_MW = 5000


def charger_alertes():
    """
    Lit les alertes depuis HBase (watt:alertes).
    Retourne une liste de dicts {zone, niveau, valeur, timestamp}.
    En cas d'erreur HBase, retourne des données simulées pour la démo.
    """
    try:
        connection = happybase.Connection(
            host=HBASE_HOST,
            port=HBASE_PORT,
            timeout=5000,
            transport='buffered',
            protocol='binary',
        )
        table = connection.table("watt:alertes")
        alertes = []

        for key, data in table.scan():
            zone    = data.get(b'info:zone',    b'INCONNU').decode()
            niveau  = data.get(b'info:niveau',  b'VERT').decode()
            valeur  = data.get(b'alerte:valeur', b'0').decode()
            ts      = data.get(b'info:timestamp', b'').decode()
            alertes.append({
                'zone':      zone,
                'niveau':    niveau,
                'valeur':    float(valeur) if valeur else 0.0,
                'timestamp': ts,
            })

        connection.close()
        return alertes

    except Exception as e:
        print(f"[WARN] HBase indisponible ({e}) — données simulées")
        return [
            {'zone': 'DAKAR_NORD',  'niveau': 'ROUGE',  'valeur': 12500.0, 'timestamp': datetime.utcnow().isoformat()},
            {'zone': 'DAKAR_NORD',  'niveau': 'ROUGE',  'valeur': 11800.0, 'timestamp': datetime.utcnow().isoformat()},
            {'zone': 'DAKAR_SUD',   'niveau': 'ROUGE',  'valeur': 10900.0, 'timestamp': datetime.utcnow().isoformat()},
            {'zone': 'DAKAR_SUD',   'niveau': 'ROUGE',  'valeur': 10200.0, 'timestamp': datetime.utcnow().isoformat()},
            {'zone': 'THIES',       'niveau': 'ROUGE',  'valeur': 10050.0, 'timestamp': datetime.utcnow().isoformat()},
            {'zone': 'THIES',       'niveau': 'ROUGE',  'valeur': 10100.0, 'timestamp': datetime.utcnow().isoformat()},
            {'zone': 'SAINT_LOUIS', 'niveau': 'ORANGE', 'valeur': 7500.0,  'timestamp': datetime.utcnow().isoformat()},
            {'zone': 'SAINT_LOUIS', 'niveau': 'ORANGE', 'valeur': 7200.0,  'timestamp': datetime.utcnow().isoformat()},
            {'zone': 'KAOLACK',     'niveau': 'ORANGE', 'valeur': 6800.0,  'timestamp': datetime.utcnow().isoformat()},
            {'zone': 'ZIGUINCHOR',  'niveau': 'VERT',   'valeur': 3200.0,  'timestamp': datetime.utcnow().isoformat()},
            {'zone': 'TAMBACOUNDA', 'niveau': 'VERT',   'valeur': 2900.0,  'timestamp': datetime.utcnow().isoformat()},
        ]


def generer_recommandations(alertes):
    """
    Génère des recommandations de délestage tournant équitable par zone.
    ROUGE : action immédiate, ORANGE : surveillance renforcée.
    """
    zones_rouge  = list({a['zone'] for a in alertes if a['niveau'] == 'ROUGE'})
    zones_orange = list({a['zone'] for a in alertes if a['niveau'] == 'ORANGE'})

    recommandations = []

    for i, zone in enumerate(zones_rouge):
        recommandations.append({
            'zone':    zone,
            'niveau':  'ROUGE',
            'action':  'Délestage tournant immédiat — réduire 30% de la charge',
            'priorite': i + 1,
        })

    for i, zone in enumerate(zones_orange):
        recommandations.append({
            'zone':    zone,
            'niveau':  'ORANGE',
            'action':  'Réduire la charge de 20% — surveiller évolution',
            'priorite': len(zones_rouge) + i + 1,
        })

    return recommandations


@app.route("/")
def dashboard():
    """Route principale — rendu du dashboard."""
    alertes = charger_alertes()

    compteur_zones   = Counter([a['zone']   for a in alertes])
    compteur_risques = Counter([a['niveau'] for a in alertes])
    recommandations  = generer_recommandations(alertes)

    zones_rouge  = {a['zone'] for a in alertes if a['niveau'] == 'ROUGE'}
    zones_orange = {a['zone'] for a in alertes if a['niveau'] == 'ORANGE'}
    nb_rouge  = len(zones_rouge)
    nb_orange = len(zones_orange)
    # Les zones VERT ne sont pas stockées dans HBase — on les déduit
    nb_vert   = len(set(ZONES_SENEGAL) - zones_rouge - zones_orange)

    zones_data = []
    for zone in ZONES_SENEGAL:
        count  = compteur_zones.get(zone, 0)
        niveau = next((a['niveau'] for a in alertes if a['zone'] == zone), 'VERT')
        zones_data.append({'zone': zone, 'count': count, 'niveau': niveau})

    return render_template(
        "index.html",
        zones=zones_data,
        risques=compteur_risques,
        recommandations=recommandations,
        total_alertes=len(alertes),
        nb_rouge=nb_rouge,
        nb_orange=nb_orange,
        nb_vert=nb_vert,
        timestamp=datetime.utcnow().strftime('%d/%m/%Y %H:%M UTC'),
    )


@app.route("/api/alertes")
def api_alertes():
    """API JSON — données brutes des alertes pour rafraîchissement."""
    alertes = charger_alertes()
    return jsonify({
        'alertes':   alertes,
        'timestamp': datetime.utcnow().isoformat(),
        'total':     len(alertes),
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
