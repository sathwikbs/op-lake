#!/usr/bin/env python3
"""Generic Unity Catalog namespace model + grant expansion.

WHY THIS EXISTS
---------------
Enterprises organize a lakehouse along different axes -- environment
(dev/prod), business domain (finance/marketing), team, medallion layer
(bronze/silver/gold), tenant, region, ... . But Unity Catalog (like most
lakehouses) has a FIXED three-level namespace -- ``catalog.schema.table`` under
a per-region metastore. So every org is really just deciding WHICH axis lands
at WHICH level: 1-2 axes get folded into the CATALOG name, one into the SCHEMA
name. This module makes that mapping fully data-driven from personas.yaml's
``namespace:`` block instead of hard-coding a single ``catalog``.

    namespace:
      dimensions:                     # named axes + their allowed values
        domain: [analytics]
        layer:  [bronze, silver, gold]
      catalog_template: "{domain}"    # how a CATALOG name is composed
      schema_template:  "{layer}"     # how a SCHEMA name is composed
      primary_catalog:  analytics     # default catalog for non-data tooling

Grants are expressed against LEVELS (``catalog`` | ``schema``) with optional
dimension filters, and expanded over the cartesian product of the relevant
dimensions:

    grants:
      - {level: catalog, privilege: USE CATALOG}
      - {level: schema, where: {layer: [gold]}, privileges: [USE SCHEMA, SELECT]}

To switch topology you change ONLY the ``namespace:`` block -- personas/teams
and the reconciler are unchanged:

    single catalog        : catalog_template "{domain}",   dimensions.domain [analytics]
    catalog-per-env       : catalog_template "{env}",      dimensions.env [dev, prod]
    env x domain          : catalog_template "{env}_{domain}"
    catalog-per-team      : catalog_template "{team}"
    catalog-per-tenant    : catalog_template "{tenant}"
    medallion-as-catalog  : catalog_template "{layer}",    schema_template "{domain}"

The reconciler (sync_group_grants.py) and the SA provisioner
(provision_service_account.py) both import this so expansion is identical.
"""
from __future__ import annotations

import itertools
import re

_PLACEHOLDER = re.compile(r"\{(\w+)\}")


def _safe_sub(template: str, mapping: dict) -> str:
    """Substitute {name} placeholders from mapping; leave unknown ones intact.

    Safer than str.format (never raises KeyError/IndexError on stray braces),
    which matters because templates and literal names are user-authored YAML.
    """
    return _PLACEHOLDER.sub(lambda m: str(mapping.get(m.group(1), m.group(0))), template)


class Namespace:
    """Parsed ``namespace:`` model + grant/securable expansion."""

    def __init__(self, cfg: dict):
        cfg = cfg or {}
        ns = cfg.get("namespace") or {}
        self.dimensions: dict[str, list] = {
            k: list(v) for k, v in (ns.get("dimensions") or {}).items()
        }

        if not self.dimensions:
            # Back-compat: no namespace block -> synthesize a single-catalog /
            # medallion-layer model from the legacy scalar `catalog:`.
            cat = cfg.get("catalog", "main")
            self.dimensions = {"layer": ["bronze", "silver", "gold"]}
            self.catalog_template = cat            # literal, no placeholders
            self.schema_template = "{layer}"
            self.primary_catalog = cat
        else:
            first_dim = next(iter(self.dimensions))
            self.catalog_template = ns.get("catalog_template", "{%s}" % first_dim)
            self.schema_template = ns.get("schema_template", "{layer}")
            self.primary_catalog = ns.get("primary_catalog") or cfg.get("catalog")

        # Which dimensions form the catalog name vs the schema name.
        self.catalog_dims = _PLACEHOLDER.findall(self.catalog_template)
        self.schema_dims = _PLACEHOLDER.findall(self.schema_template)

        if not self.primary_catalog:
            # Derive from the FIRST value of each catalog-forming dimension.
            ctx = {d: self.dimensions.get(d, [""])[0] for d in self.catalog_dims}
            self.primary_catalog = _safe_sub(self.catalog_template, ctx)

    # ---- name composition ------------------------------------------------- #
    def compose_catalog(self, ctx: dict) -> str:
        return _safe_sub(self.catalog_template, ctx)

    def compose_schema(self, ctx: dict) -> str:
        return _safe_sub(self.schema_template, ctx)

    def _values(self, dim: str, scope: dict, where: dict) -> list:
        """Allowed values of `dim`, narrowed by entity `scope` then grant `where`."""
        vals = self.dimensions.get(dim, [])
        if scope and dim in scope:
            vals = [v for v in vals if v in scope[dim]]
        if where and dim in where:
            vals = [v for v in vals if v in where[dim]]
        return vals

    def _dims_for_level(self, level: str) -> list:
        if level == "catalog":
            return list(self.catalog_dims)
        # schema (and any deeper level) needs catalog-forming + schema-forming dims
        return list(self.catalog_dims) + [
            d for d in self.schema_dims if d not in self.catalog_dims
        ]

    # ---- grant expansion -------------------------------------------------- #
    def expand(self, grants: list, scope: dict | None = None,
               extra: dict | None = None) -> list[dict]:
        """Expand grant templates into concrete {securable,name,privilege} dicts.

        * ``level`` (alias ``securable``): "catalog" | "schema".
        * ``privilege`` or ``privileges: [..]`` -> one output grant per privilege.
        * ``where: {dim: [values]}`` narrows this grant to a subset of a dimension.
        * ``scope`` (entity-level) bounds ALL of a persona/team's grants.
        * ``name`` (optional) literal escape-hatch; supports {dim}/{catalog}/{team}.
        """
        scope = scope or {}
        extra = extra or {}
        out: dict[tuple, dict] = {}
        for g in grants or []:
            level = g.get("level") or g.get("securable")
            where = g.get("where") or {}
            privs = g.get("privileges") or [g["privilege"]]
            literal = g.get("name")
            dims = self._dims_for_level(level)
            value_lists = [self._values(d, scope, where) for d in dims]
            combos = itertools.product(*value_lists) if dims else [()]
            for combo in combos:
                ctx = dict(zip(dims, combo))
                if literal:
                    base = {"catalog": extra.get("catalog", self.primary_catalog)}
                    name = _safe_sub(literal, {**base, **extra, **ctx})
                elif level == "catalog":
                    name = self.compose_catalog(ctx)
                else:
                    name = f"{self.compose_catalog(ctx)}.{self.compose_schema(ctx)}"
                for p in privs:
                    out[(level, name, p)] = {
                        "securable": level, "name": name, "privilege": p,
                    }
        return list(out.values())

    # ---- bootstrap helpers ------------------------------------------------ #
    def declared_securables(self) -> tuple[list[str], list[tuple[str, str]]]:
        """The FULL declared namespace: every catalog + (catalog, schema) pair
        implied by the dimensions/templates. Used to bootstrap securables so
        USE-CATALOG/USE-SCHEMA grants always have a target."""
        catalogs: dict[str, None] = {}
        schemas: dict[tuple, None] = {}
        # catalogs = product over catalog-forming dims
        cat_lists = [self.dimensions.get(d, []) for d in self.catalog_dims]
        for combo in (itertools.product(*cat_lists) if self.catalog_dims else [()]):
            catalogs[self.compose_catalog(dict(zip(self.catalog_dims, combo)))] = None
        # schemas = product over catalog-forming + schema-forming dims
        all_dims = self._dims_for_level("schema")
        all_lists = [self.dimensions.get(d, []) for d in all_dims]
        for combo in (itertools.product(*all_lists) if all_dims else [()]):
            ctx = dict(zip(all_dims, combo))
            schemas[(self.compose_catalog(ctx), self.compose_schema(ctx))] = None
        return list(catalogs), list(schemas)

    @staticmethod
    def referenced_securables(grant_lists) -> tuple[set, set]:
        """(catalogs, {(catalog, schema)}) referenced by already-expanded grants."""
        catalogs, schemas = set(), set()
        for grants in grant_lists:
            for g in grants:
                if g["securable"] == "catalog":
                    catalogs.add(g["name"])
                elif g["securable"] == "schema":
                    cat, _, sch = g["name"].partition(".")
                    catalogs.add(cat)
                    if sch:
                        schemas.add((cat, sch))
        return catalogs, schemas
