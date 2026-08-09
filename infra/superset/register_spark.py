"""Register the Spark Connect (Unity Catalog) database in Superset (idempotent).

Configured for PER-USER identity -- there is intentionally NO shared credential:
  * impersonate_user=True  -> Superset calls the engine-spec's
    update_impersonation_config per user, which injects a UC token minted for
    the logged-in user (exchanged from their Keycloak token).
  * supports_oauth2 (engine-spec) + DATABASE_OAUTH2_CLIENTS (superset_config.py)
    -> Superset obtains/forwards each user's token via a transparent SSO dance.

Unity Catalog then enforces RBAC as that user. The Spark Connect endpoint and
the compute-edge token come from SPARK_REMOTE (env), not from this URI.
"""
import os

from superset.app import create_app

app = create_app()

with app.app_context():
    from superset import db
    from superset.models.core import Database

    catalog = os.environ.get("UC_CATALOG", "analytics")
    name = "Spark Connect (Unity Catalog)"
    uri = f"spark_connect_uc://spark/{catalog}"

    existing = db.session.query(Database).filter_by(database_name=name).first()
    database = existing or Database(database_name=name)
    database.set_sqlalchemy_uri(uri)
    database.impersonate_user = True
    # Let dashboards/SQL Lab run queries and browse metadata (all RBAC-scoped by UC).
    database.allow_dml = False
    database.expose_in_sqllab = True

    if existing is None:
        db.session.add(database)
        print(f"[superset] Registered database '{name}' (per-user OAuth -> Spark Connect -> UC)")
    else:
        print(f"[superset] Updated database '{name}'")

    db.session.commit()
