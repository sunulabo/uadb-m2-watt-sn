# schema.py — Validation des données Watt-SN avec Pandera

import pandera as pa
from pandera import Column, Check
from pandera.typing import Series



# =========================================================
# Schéma validation consommation électrique
# =========================================================

conso_schema = pa.DataFrameSchema(
    {
        "compteur_id": Column(
            str,
            nullable=False,
            checks=[
                Check.str_matches(r"^CPT_[A-Z_]+_[A-F0-9]{8}$")
            ]
        ),

        "zone": Column(
            str,
            nullable=False,
            checks=[
                Check.isin([
                    "DAKAR_NORD",
                    "DAKAR_SUD",
                    "THIES",
                    "SAINT_LOUIS",
                    "ZIGUINCHOR",
                    "KAOLACK",
                    "TAMBACOUNDA"
                ])
            ]
        ),

        "consommation_mw": Column(
            float,
            nullable=False,
            checks=[
                Check.ge(0),
                Check.le(2000)
            ]
        ),

        "heure": Column(
            int,
            nullable=False,
            checks=[
                Check.ge(0),
                Check.le(23)
            ]
        ),

        "timestamp": Column(
            str,
            nullable=False,
            checks=[
                Check.str_matches(
                    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"
                )
            ]
        )
    },

    strict=True,
    coerce=True
)


# =========================================================
# Schéma validation production solaire
# =========================================================

solaire_schema = pa.DataFrameSchema(
    {
        "zone": Column(
            str,
            nullable=False,
            checks=[
                Check.isin([
                    "DAKAR_NORD",
                    "DAKAR_SUD",
                    "THIES",
                    "SAINT_LOUIS",
                    "ZIGUINCHOR",
                    "KAOLACK",
                    "TAMBACOUNDA"
                ])
            ]
        ),

        "production_mw": Column(
            float,
            nullable=False,
            checks=[
                Check.ge(0),
                Check.le(500)
            ]
        ),

        "capacite_installee_mw": Column(
            float,
            nullable=False,
            checks=[
                Check.ge(0),
                Check.le(500)
            ]
        ),

        "heure": Column(
            int,
            nullable=False,
            checks=[
                Check.ge(0),
                Check.le(23)
            ]
        ),

        "timestamp": Column(
            str,
            nullable=False,
            checks=[
                Check.str_matches(
                    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"
                )
            ]
        )
    },

    strict=True,
    coerce=True
)


# =========================================================
# Fonction utilitaire validation
# =========================================================

def validate_conso(df):
    """
    Validation des données de consommation.
    """
    return conso_schema.validate(df)


def validate_solaire(df):
    """
    Validation des données solaires.
    """
    return solaire_schema.validate(df)


# =========================================================
# Exemple de test local
# =========================================================

if __name__ == "__main__":

    import pandas as pd

    data = pd.DataFrame([
        {
            "compteur_id": "CPT_DAKAR_NORD_A1B2C3D4",
            "zone": "DAKAR_NORD",
            "consommation_mw": 350.5,
            "heure": 14,
            "timestamp": "2026-05-16T14:30:00Z"
        }
    ])

    try:
        validated = validate_conso(data)
        print("Validation réussie ✓")
        print(validated)

    except Exception as e:
        print("Erreur validation :", e)