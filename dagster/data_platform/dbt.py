"""dbt integration: expose the dbt project's models as Dagster assets.

The dbt `bronze` source tables are mapped to the same asset keys the ingestion
step produces (["bronze", "orders"], ["bronze", "customers"]), so Dagster wires
silver models downstream of ingestion automatically.
"""
# NOTE: intentionally NO `from __future__ import annotations` here. With PEP 563
# the `context: AssetExecutionContext` hint on the @dbt_assets fn becomes the
# STRING "AssetExecutionContext", which Dagster 1.11's _validate_context_type_hint
# rejects (it compares against the real class). Keeping real annotations avoids it.
import os
from typing import Any, Mapping

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import (
    DagsterDbtTranslator,
    DbtCliResource,
    DbtProject,
    dbt_assets,
)

DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/dagster/app/dbt")

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)
# Regenerate the manifest when running the local dev CLI; in containers the
# entrypoint runs `dbt parse` before Dagster loads.
dbt_project.prepare_if_dev()

dbt_resource = DbtCliResource(project_dir=dbt_project)


class MedallionDbtTranslator(DagsterDbtTranslator):
    """Align dbt source keys with the bronze ingestion asset keys."""

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        if dbt_resource_props.get("resource_type") == "source":
            return AssetKey(
                [dbt_resource_props["source_name"], dbt_resource_props["name"]]
            )
        return super().get_asset_key(dbt_resource_props)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=MedallionDbtTranslator(),
)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    # Run dbt as the platform automation SERVICE ACCOUNT (not admin): mint the
    # SA's UC token and hand it to dbt via DBT_UC_TOKEN, which profiles.yml
    # injects as the per-session Unity Catalog catalog token
    # (server_side_parameters). UC then enforces the SA's grants.
    from .uc_identity import sa_from_context, uc_token_for_sa

    # role='build' -> the team's builder SA (sa-team-<team>-build) that owns
    # silver/gold, falling back to the team's single SA if not provisioned.
    sa = sa_from_context(context, role="build")
    if sa:
        os.environ["DBT_UC_TOKEN"] = uc_token_for_sa(sa)
        context.log.info(f"dbt running as service account {sa!r} (UC-governed)")
    # `build` runs models + tests together (silver -> gold + data tests).
    yield from dbt.cli(["build"], context=context).stream()
