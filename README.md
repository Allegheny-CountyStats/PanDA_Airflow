# CountyStat_Airflow

Python package used by CountyStat for Airflow operations:
* DevTest
* Slack Alert
* Teams HITL Alert

[Package building reference](https://packaging.python.org/en/latest/tutorials/packaging-projects/)

## Package Build/Update Steps

```shell
python -m build
python -m twine upload --verbose --repository testpypi dist/{VERSION}/*
```

## DevTest



## Slack Alert


## Teams HITL Alert

`TeamsHITLNotifier` (`teams_alert.py`) posts a Teams channel message when a HITL operator starts waiting. The message holds the HITL body and a **Review in Airflow** button that opens the Required Actions page. It is link-only: people still Accept/Reject in the Airflow UI.

Setup:
1. In the Teams channel: **... > Workflows > "Send webhook alerts to a channel"**, pick the team/channel, save, and copy the POST URL. Have a shared/team account own or co-own the flow.
2. Store the URL in an Airflow Variable whose name contains `secret` (e.g. `ACHD_teams_webhook_secret`) so Airflow masks it.
3. `AIRFLOW__API__BASE_URL` must be the URL staff use to open Airflow (the link is built from it).

```python
from PanDAirflow.teams_alert import TeamsHITLNotifier

HITLBranchOperator(..., notifiers=[TeamsHITLNotifier(variable_name='ACHD_teams_webhook_secret',
                                                     title='Approval needed')])
```

Posts only when `DEV_POWER_SWITCH` is `TRUE` (same rule as `dev_test`). A failed post is logged as a warning and does not fail the task.
