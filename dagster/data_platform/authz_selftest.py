"""Self-contained checks for team-launch authorization (no pytest required).

Run inside the Dagster container:
    python -m data_platform.authz_selftest
"""
from __future__ import annotations

from types import SimpleNamespace

from .uc_identity import TeamAuthorizationError, authorize_team_run, sa_from_context


def _ctx(run_tags=None, op_tags=None):
    run = SimpleNamespace(tags=run_tags or {})
    op_def = SimpleNamespace(tags=op_tags or {})
    return SimpleNamespace(run=run, op_def=op_def)


def _expect_ok(name, ctx):
    authorize_team_run(ctx)
    print(f"ALLOW  ok  : {name}")


def _expect_deny(name, ctx):
    try:
        authorize_team_run(ctx)
    except TeamAuthorizationError as e:
        print(f"DENY   ok  : {name}  -> {e}")
        return
    raise AssertionError(f"expected DENY but was ALLOWED: {name}")


def main() -> None:
    # Backend launches (schedule/daemon/CLI): no launched_by -> trusted.
    _expect_ok("schedule launches analytics job (no launcher)", _ctx(run_tags={"team": "analytics"}))
    _expect_ok("CLI launches untagged job (no launcher)", _ctx(run_tags={}))

    # Human, correct team role -> allowed.
    _expect_ok(
        "engineer (team-analytics) launches analytics job",
        _ctx(run_tags={"team": "analytics", "launched_by": "engineer@platform.local",
                       "launched_by_groups": "data-engineer,sa-creator,team-analytics"}),
    )

    # Human, missing team role -> denied (the core Gap 1 exploit).
    _expect_deny(
        "engineer2 (no team-analytics) launches analytics job",
        _ctx(run_tags={"team": "analytics", "launched_by": "engineer2@platform.local",
                       "launched_by_groups": "data-engineer"}),
    )

    # Platform admin -> may launch any team.
    _expect_ok(
        "platformadmin launches analytics job",
        _ctx(run_tags={"team": "analytics", "launched_by": "platformadmin@platform.local",
                       "launched_by_groups": "sa-creator,platform-admin"}),
    )

    # Human launches untagged job -> allowed (already passed persona gate).
    _expect_ok(
        "engineer2 launches untagged job",
        _ctx(run_tags={"launched_by": "engineer2@platform.local", "launched_by_groups": "data-engineer"}),
    )

    # Forgery attempt: a browser can't set launched_by (middleware strips it), but
    # even if it could, the groups must contain the real role. Simulate a user
    # claiming the team via tag without the role -> still denied.
    _expect_deny(
        "spoofed team tag without role",
        _ctx(run_tags={"team": "analytics", "launched_by": "attacker@evil",
                       "launched_by_groups": "data-engineer"}),
    )

    # sa_from_context must also fail closed (guard runs before SA resolution).
    try:
        sa_from_context(_ctx(run_tags={"team": "analytics", "launched_by": "engineer2@platform.local",
                                       "launched_by_groups": "data-engineer"}))
    except TeamAuthorizationError:
        print("DENY   ok  : sa_from_context fails closed for unauthorized team launch")
    else:
        raise AssertionError("sa_from_context did not fail closed")

    print("\nALL AUTHORIZATION CHECKS PASSED")


if __name__ == "__main__":
    main()
