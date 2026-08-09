"""Dagster webserver entrypoint with server-side launcher-identity injection.

Dagster OSS does not record *who* launched a run, so a user who is past the
Keycloak/oauth2-proxy turnstile could otherwise trigger another team's pipeline
(which runs as that team's service account). This wrapper closes that gap
without forking Dagster:

  * oauth2-proxy (`dagster-auth`) authenticates every request and, with
    `--set-xauthrequest=true`, forwards the caller's identity as the
    `X-Auth-Request-Email` / `X-Auth-Request-Groups` headers.
  * A tiny ASGI middleware intercepts the GraphQL launch mutation and stamps the
    run's `executionMetadata.tags` with `launched_by` + `launched_by_groups`
    taken FROM THOSE HEADERS (any client-supplied values are stripped first, so
    the identity cannot be forged from the browser).
  * The execution-time guard in `data_platform.uc_identity.authorize_team_run`
    then fails closed unless the launcher holds the job's `team-<team>` role
    (or `platform-admin`).

Backend launches (schedule / daemon / CLI) never traverse this HTTP edge, carry
no `launched_by`, and are treated as trusted platform automation.
"""
from __future__ import annotations

import json

import dagster_webserver.app as _appmod
import dagster_webserver.cli as _climod

_LAUNCHED_BY = "launched_by"
_LAUNCHED_BY_GROUPS = "launched_by_groups"


def _inject_identity(body: bytes, email: str, groups: str) -> bytes:
    """Stamp launcher identity into any GraphQL op carrying executionParams."""
    try:
        payload = json.loads(body)
    except Exception:  # noqa: BLE001 - not JSON; leave untouched
        return body

    def stamp(op: object) -> None:
        if not isinstance(op, dict):
            return
        variables = op.get("variables")
        if not isinstance(variables, dict):
            return
        exec_params = variables.get("executionParams")
        if not isinstance(exec_params, dict):
            return
        meta = exec_params.get("executionMetadata")
        if not isinstance(meta, dict):
            meta = {}
        tags = meta.get("tags")
        if not isinstance(tags, list):
            tags = []
        # Drop any browser-supplied launched_by* tags, then set the trusted ones.
        tags = [
            t
            for t in tags
            if not (isinstance(t, dict) and str(t.get("key", "")).startswith("launched_by"))
        ]
        tags.append({"key": _LAUNCHED_BY, "value": email})
        tags.append({"key": _LAUNCHED_BY_GROUPS, "value": groups})
        meta["tags"] = tags
        exec_params["executionMetadata"] = meta

    if isinstance(payload, list):  # batched GraphQL
        for op in payload:
            stamp(op)
    else:
        stamp(payload)

    try:
        return json.dumps(payload).encode()
    except Exception:  # noqa: BLE001
        return body


class LauncherIdentityMiddleware:
    """Pure ASGI middleware: only rewrites POST /graphql bodies; passes through
    everything else (incl. websockets) untouched."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if (
            scope.get("type") != "http"
            or scope.get("method") != "POST"
            or scope.get("path") != "/graphql"
        ):
            return await self.app(scope, receive, send)

        headers = {k.decode().lower(): v.decode() for k, v in scope.get("headers", [])}
        email = (
            headers.get("x-auth-request-email")
            or headers.get("x-auth-request-preferred-username")
            or headers.get("x-auth-request-user")
        )
        if not email:
            # No authenticated identity on this request (should not happen behind
            # oauth2-proxy). Pass through; the fail-closed guard still applies.
            return await self.app(scope, receive, send)

        groups = headers.get("x-auth-request-groups", "")

        body = b""
        while True:
            message = await receive()
            body += message.get("body", b"")
            if not message.get("more_body", False):
                break

        new_body = _inject_identity(body, email, groups)

        new_headers = [
            (k, v) for (k, v) in scope.get("headers", []) if k.decode().lower() != "content-length"
        ]
        new_headers.append((b"content-length", str(len(new_body)).encode()))
        new_scope = dict(scope)
        new_scope["headers"] = new_headers

        sent = False

        async def receive_wrapped():
            nonlocal sent
            if not sent:
                sent = True
                return {"type": "http.request", "body": new_body, "more_body": False}
            return {"type": "http.request", "body": b"", "more_body": False}

        return await self.app(new_scope, receive_wrapped, send)


def _install_middleware():
    """Wrap Dagster's app factory so every app instance gets the middleware."""
    _orig = _appmod.create_app_from_workspace_process_context

    def patched(*args, **kwargs):
        app = _orig(*args, **kwargs)
        app.add_middleware(LauncherIdentityMiddleware)
        return app

    _appmod.create_app_from_workspace_process_context = patched
    # cli.py imported the symbol directly, so patch that reference too.
    _climod.create_app_from_workspace_process_context = patched


if __name__ == "__main__":
    _install_middleware()
    # Delegate to the stock CLI; click reads flags straight from sys.argv[1:]
    # (compose passes: -h 0.0.0.0 -p 3030 -w .../workspace.yaml).
    _climod.main()
