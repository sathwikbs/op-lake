"""Superset DB engine spec for Spark Connect + Unity Catalog per-user RBAC.

Per-user flow (SIP-85 OAuth2 + impersonation):
  1. The DB is registered with ``impersonate_user=True`` and this spec declares
     ``supports_oauth2=True`` with a Keycloak client (DATABASE_OAUTH2_CLIENTS).
  2. When a user runs a query, Superset resolves that user's Keycloak token
     (obtaining it via a transparent SSO OAuth2 dance the first time) and calls
     ``update_impersonation_config`` with it as ``access_token``.
  3. We exchange it for a Unity Catalog token (RFC 8693) and put it in
     ``connect_args['uc_token']``; the dialect sets it as the per-session catalog
     token, so UC enforces THIS user's RBAC.
  4. If there is no token yet, the driver raises ``SparkConnectOAuth2Error`` and
     ``needs_oauth2`` maps it to a re-auth -- we never fall back to the admin
     identity (fail-closed).
"""
from __future__ import annotations

import logging
from typing import Any

from superset.db_engine_specs.base import BaseEngineSpec

from .dbapi import SparkConnectOAuth2Error
from .token_exchange import exchange_kc_for_uc

logger = logging.getLogger(__name__)


class SparkConnectUCEngineSpec(BaseEngineSpec):
    engine = "spark_connect_uc"
    engine_name = "Spark Connect (Unity Catalog)"
    drivers = {"connect": "Spark Connect gRPC with per-user Unity Catalog token"}
    default_driver = "connect"

    sqlalchemy_uri_placeholder = "spark_connect_uc://spark/analytics"

    # Unity Catalog is the authority; identity is per-user via OAuth2 + exchange.
    supports_oauth2 = True
    oauth2_scope = "openid email profile"
    oauth2_exception = SparkConnectOAuth2Error

    # Spark SQL time-grain expressions (enough for typical BI on gold tables).
    _time_grain_expressions = {
        None: "{col}",
        "PT1S": "date_trunc('second', {col})",
        "PT1M": "date_trunc('minute', {col})",
        "PT1H": "date_trunc('hour', {col})",
        "P1D": "date_trunc('day', {col})",
        "P1W": "date_trunc('week', {col})",
        "P1M": "date_trunc('month', {col})",
        "P3M": "date_trunc('quarter', {col})",
        "P1Y": "date_trunc('year', {col})",
    }

    @classmethod
    def epoch_to_dttm(cls) -> str:
        return "from_unixtime({col})"

    @classmethod
    def update_impersonation_config(
        cls,
        connect_args: dict[str, Any],
        uri: str,
        username: str | None,
        access_token: str | None,
    ) -> None:
        """Exchange the user's Keycloak token for a UC token and inject it.

        Leaving ``uc_token`` unset when there is no ``access_token`` is
        intentional: the driver will then raise ``SparkConnectOAuth2Error`` and
        Superset will start the OAuth2 dance instead of running as admin.
        """
        if not access_token:
            logger.info("spark_connect_uc: no access_token for %s; will trigger OAuth2", username)
            return
        try:
            connect_args["uc_token"] = exchange_kc_for_uc(access_token)
            logger.info("spark_connect_uc: injected per-user UC token for %s", username)
        except Exception as ex:  # noqa: BLE001
            # Surface as an OAuth2 need so the user can re-auth; never fall back.
            logger.warning("spark_connect_uc: UC token exchange failed for %s: %s", username, ex)
            raise SparkConnectOAuth2Error(f"UC token exchange failed: {ex}") from ex

    @classmethod
    def needs_oauth2(cls, ex: Exception) -> bool:
        """Recognize our OAuth2 trigger even when SQLAlchemy wraps it."""
        from flask import g
        from sqlalchemy.exc import DBAPIError

        original = ex
        if isinstance(ex, DBAPIError) and getattr(ex, "orig", None) is not None:
            original = ex.orig
        return bool(g and hasattr(g, "user")) and isinstance(original, cls.oauth2_exception)

    # ------------------------------------------------------------------
    # OAuth2 token exchange (SIP-85)
    #
    # Superset's base implementation POSTs the token request as JSON, but OIDC
    # token endpoints (Keycloak included) require application/x-www-form-urlencoded
    # per RFC 6749. With JSON, Keycloak returns an error body with no
    # ``expires_in``, and Superset core raises ``KeyError: 'expires_in'``. We
    # override both exchanges to send form-encoded data.
    # ------------------------------------------------------------------
    @classmethod
    def _token_request(cls, config: dict[str, Any], extra: dict[str, str]) -> dict[str, Any]:
        import requests
        from flask import current_app

        timeout = current_app.config["DATABASE_OAUTH2_TIMEOUT"].total_seconds()
        data = {
            "client_id": config["id"],
            "client_secret": config["secret"],
            **extra,
        }
        response = requests.post(
            config["token_request_uri"],
            data=data,  # form-encoded, not JSON
            timeout=timeout,
        )
        payload = response.json()
        if "access_token" not in payload:
            raise SparkConnectOAuth2Error(f"OAuth2 token request failed: {payload}")
        return payload

    @classmethod
    def get_oauth2_token(cls, config: dict[str, Any], code: str) -> dict[str, Any]:
        return cls._token_request(
            config,
            {
                "code": code,
                "redirect_uri": config["redirect_uri"],
                "grant_type": "authorization_code",
            },
        )

    @classmethod
    def get_oauth2_fresh_token(
        cls, config: dict[str, Any], refresh_token: str
    ) -> dict[str, Any]:
        return cls._token_request(
            config,
            {
                "refresh_token": refresh_token,
                "grant_type": "refresh_token",
            },
        )
