"""HITL notifier links and Slack message body. Offline: Variables, config and webhooks are mocked."""
import types
from unittest.mock import patch

import pytest

pytest.importorskip("airflow.providers.standard.operators.hitl")
pytest.importorskip("airflow.providers.slack.hooks.slack_webhook")

from PanDAirflow import hitl_links, slack_hitl, teams_alert  # noqa: E402

BASE_URL = "https://devairflow.example:8080"
RUN_ID = "scheduled__2026-11-10T20:05:00+00:00"
# The review form URL the Airflow 3.2.2 UI itself uses for map index 2.
UI_MAPPED_URL = (f"{BASE_URL}/dags/Jail_OT_JOB_Counts/runs/{RUN_ID}"
                 f"/tasks/review_color/mapped/2/required_actions")


def context(map_index, body=None):
    ti = types.SimpleNamespace(dag_id="Jail_OT_JOB_Counts", run_id=RUN_ID, task_id="review_color",
                               map_index=map_index, start_date=None)
    task = types.SimpleNamespace(body=body, execution_timeout=None)
    return {"task_instance": ti, "task": task, "data_interval_end": "2026-11-10"}


@pytest.fixture
def base_url():
    with patch.object(hitl_links.conf, "get", return_value=BASE_URL):
        yield


def test_mapped_review_link_puts_map_index_in_path(base_url):
    # Bug: Airflow's link used ?map_index=2, which the UI ignores, so the button opened map index -1 (404).
    assert hitl_links.review_link(context(2)) == UI_MAPPED_URL


def test_mapped_review_link_keeps_run_id_unencoded(base_url):
    # Bug: encoding the run id's ':' and '+' (%3A, %2B) also broke the page.
    link = hitl_links.review_link(context(2))
    assert RUN_ID in link and "%3A" not in link and "%2B" not in link


def test_unmapped_review_link_uses_airflow_default():
    with patch.object(hitl_links.HITLOperator, "generate_link_to_ui_from_context",
                      return_value="AIRFLOW_LINK") as stock:
        assert hitl_links.review_link(context(-1)) == "AIRFLOW_LINK"
    stock.assert_called_once()


def test_missing_base_url_raises():
    with patch.object(hitl_links.conf, "get", return_value=None), pytest.raises(ValueError):
        hitl_links.review_link(context(0))


def slack_button(message):
    actions = [b for b in message["attachments"][0]["blocks"] if b["type"] == "actions"][0]
    return actions["elements"][0]["url"]


def test_slack_button_opens_mapped_review_form(base_url):
    # Guards that the Slack notifier uses review_link, not Airflow's broken link for mapped tasks.
    sent = {}
    with patch.object(slack_hitl.Variable, "get", return_value="TRUE"), \
            patch.object(slack_hitl, "SlackWebhookHook") as hook:
        hook.return_value.send.side_effect = lambda **message: sent.update(message)
        slack_hitl.SlackHITLNotifier(title="Review #FAC090").notify(context(2))
    assert slack_button(sent) == UI_MAPPED_URL


def test_teams_button_opens_mapped_review_form(base_url):
    # Same link bug in the Teams notifier.
    with patch.object(teams_alert.Variable, "get", side_effect=lambda key, default=None:
                      "TRUE" if key == "DEV_POWER_SWITCH" else "https://teams.example/webhook"), \
            patch.object(teams_alert.requests, "post") as post:
        teams_alert.TeamsHITLNotifier(variable_name="x_teams_secret").notify(context(2))
    card = post.call_args.kwargs["json"]["attachments"][0]["content"]
    assert card["actions"][0]["url"] == UI_MAPPED_URL


def test_slack_message_drops_markdown_images():
    # Bug: Markdown swatch images showed up in Slack as raw ![#FAC090](https://...) text.
    body = ("* 🟧 ![#FAC090](https://airflow.example/color-swatch/FAC090.svg) `#FAC090`: 7 cells\n"
            "* 🟧 ![#FAC191](https://airflow.example/color-swatch/FAC191.svg) `#FAC191`: 2 cells")
    message = slack_hitl.SlackHITLNotifier(title="Review", color="FAC090").build_message(
        context(0, body), "https://airflow.example/review")
    text = [b for b in message["attachments"][0]["blocks"] if b["type"] == "section" and "text" in b][0]
    assert "![" not in text["text"]["text"]
    assert text["text"]["text"].splitlines() == ["* 🟧 `#FAC090`: 7 cells", "* 🟧 `#FAC191`: 2 cells"]
    assert message["attachments"][0]["color"] == "#FAC090"
