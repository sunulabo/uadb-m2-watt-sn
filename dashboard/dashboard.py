# dashboard.py
# ==========================================================
# Dashboard Watt-SN
# Visualisation des alertes HBase
# Equipe 06 | M2 DSGL | UADB Bambey
# ==========================================================

from flask import Flask, render_template
import happybase
from collections import Counter

app = Flask(__name__)

HBASE_HOST = "localhost"
HBASE_PORT = 9090

def charger_alertes():

    connection = happybase.Connection(
        host=HBASE_HOST,
        port=HBASE_PORT
    )

    table = connection.table("watt:alertes")

    alertes = []

    for key, data in table.scan():

        zone = data.get(
            b'info:zone',
            b'INCONNU'
        ).decode()

        niveau = data.get(
            b'info:niveau',
            b'VERT'
        ).decode()

        alertes.append({
            "zone": zone,
            "niveau": niveau
        })

    return alertes


@app.route("/")
def dashboard():

    alertes = charger_alertes()

    compteur_zones = Counter(
        [a["zone"] for a in alertes]
    )

    compteur_risques = Counter(
        [a["niveau"] for a in alertes]
    )

    recommandations = []

    for a in alertes:

        if a["niveau"] == "ROUGE":
            recommandations.append(
                f"{a['zone']} : délestage tournant immédiat"
            )

        elif a["niveau"] == "ORANGE":
            recommandations.append(
                f"{a['zone']} : réduire la charge de 20%"
            )

    return render_template(
        "index.html",
        zones=compteur_zones,
        risques=compteur_risques,
        recommandations=recommandations
    )


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=5000,
        debug=True
    )