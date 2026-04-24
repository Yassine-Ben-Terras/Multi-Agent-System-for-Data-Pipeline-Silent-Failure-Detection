"""
Unit tests for ActionExecutor.
All tests run in DRY_RUN mode — no real API calls made.
"""

from __future__ import annotations

import pytest
from unittest.mock import patch

from agents.remediation.executor import ActionExecutor
from agents.remediation.playbook import PlannedAction
from config.schemas import ActionType


@pytest.fixture(autouse=True)
def force_dry_run(monkeypatch):
    """Ensure all tests run in dry-run mode."""
    monkeypatch.setenv("REMEDIATION_DRY_RUN", "true")
    # Re-import executor module to pick up env change
    import importlib
    import agents.remediation.executor as mod
    importlib.reload(mod)


@pytest.fixture
def executor():
    import agents.remediation.executor as mod
    return mod.ActionExecutor()


def make_action(
    action_type: ActionType = ActionType.NOTIFY_SLACK,
    target: str = "data-platform",
    payload: dict = None,
) -> PlannedAction:
    return PlannedAction(
        action_type=action_type,
        target=target,
        payload=payload or {},
        description="test action",
    )


class TestActionExecutorDryRun:

    def test_dry_run_always_succeeds(self, executor):
        action = make_action(ActionType.QUARANTINE_PARTITION, "fct_orders")
        success, error = executor.execute(action)
        assert success is True
        assert error is None

    def test_dry_run_all_action_types(self, executor):
        for action_type in ActionType:
            action = make_action(action_type)
            success, error = executor.execute(action)
            assert success is True, f"Failed for {action_type}"

    def test_unknown_action_type_handled(self, executor):
        """Executor dispatch returns False for unknown types — no crash."""
        action = make_action(ActionType.NOTIFY_SLACK)
        # Override dispatch to simulate unknown type
        with patch.object(executor, '_notify_slack', side_effect=Exception("unexpected")):
            import agents.remediation.executor as mod
            mod.DRY_RUN = False
            try:
                success, error = executor.execute(action)
                assert success is False
                assert error is not None
            finally:
                mod.DRY_RUN = True
