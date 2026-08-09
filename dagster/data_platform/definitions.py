"""Top-level Dagster Definitions wiring the full pipeline together.

  dlt ingest -> bronze (Spark/UC) -> silver/gold (dbt, UC-managed Delta)

The AUTOMATION plane ends at UC-governed `gold`. There is no separate serving
store: the HUMAN/BI plane (Superset -> Spark Connect -> Unity Catalog) reads
`gold` LIVE, per logged-in user, so UC enforces RBAC at query time. Both planes
run on Spark Connect -- automation as a team service account, BI as the user.
"""
from __future__ import annotations

from dagster import Definitions, ScheduleDefinition, define_asset_job

from .dbt import dbt_models, dbt_resource
from .ingestion import bronze_assets

# One job that materializes everything end-to-end. It carries NO team tag, so it
# runs as the default platform automation SA (UC_AUTOMATION_SA_CLIENT_ID) -- which
# is deliberately LEAST-PRIVILEGE (catalog visibility only, no data). An untagged
# run therefore cannot read/write data by design: tag your pipeline with a `team`
# (see analytics_team_job) so it runs as that team's builder SA. Kept mainly as a
# dev/smoke convenience and to make the "must tag a team" contract explicit.
medallion_job = define_asset_job(name="medallion_job", selection="*")

# Team-scoped DAG (Phase 6): the SAME assets, but tagged with a team. Its runs
# authenticate as that team's service account `sa-team-<name>` (CONVENTION), so
# Unity Catalog enforces the TEAM's grants. Add a team by declaring its
# `service_account.persona` in personas.yaml (the IAM reconciler auto-provisions
# the SA -- client + Vault secret + grants) and adding a team-tagged job here.
# There is no team->SA env map to maintain.
analytics_team_job = define_asset_job(
    name="analytics_team_job", selection="*", tags={"team": "analytics"}
)

daily_schedule = ScheduleDefinition(
    name="daily_medallion",
    job=analytics_team_job,
    cron_schedule="0 6 * * *",
)

defs = Definitions(
    assets=[*bronze_assets, dbt_models],
    resources={"dbt": dbt_resource},
    jobs=[medallion_job, analytics_team_job],
    schedules=[daily_schedule],
)
