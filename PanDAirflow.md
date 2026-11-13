# PanDAirflow

Python package used by Allegheny County's Performance and Data Analytics division (PanDA) for Airflow operations. It replaces `CountyStatAirflow`: switch `from CountyStatAirflow.<module> import ...` to `from PanDAirflow.<module> import ...`, after checking the signatures below.

| Module | Import | Purpose |
|---|---|---|
| `dev_test_function` | `from PanDAirflow.dev_test_function import dev_test` | Returns `True` on prod (`DEV_POWER_SWITCH`), for use inside your own code |
| `short_circuits` | `from PanDAirflow.short_circuits import dev_test, tableau_check` | Short-circuit tasks: skip on dev servers, skip during the Tableau maintenance window |
| `slack_alert` | `from PanDAirflow.slack_alert import task_fail_slack_alert` | Slack message when a task fails |
| `slack_hitl` | `from PanDAirflow.slack_hitl import SlackHITLNotifier` | Slack message when a HITL task is waiting for review |
| `teams_alert` | `from PanDAirflow.teams_alert import TeamsHITLNotifier` | Teams message when a HITL task is waiting for review |
| `hitl_links` | `from PanDAirflow.hitl_links import review_link` | Link to a HITL task's Required Actions page (works for mapped tasks) |
| `validates` | `from PanDAirflow.validates import validate_task, data_validation` | Docker task(s) that check warehouse tables against a column schema |
| `file_checkit` | `from PanDAirflow.file_checkit import check_moveit` | Sensor that waits for a file to land in MOVEit |

## DevTest

Dev and prod servers run the same DAGs. The Airflow Variable `DEV_POWER_SWITCH` is `TRUE` only on prod, and these helpers read it so dev servers don't run prod loads or send alerts. There are two helpers with the same name:

* `dev_test_function.dev_test()`: plain function that returns `True` when `DEV_POWER_SWITCH` is `TRUE`, else `False`. Use it inside your own Python logic.
* `short_circuits.dev_test()`: a `@task.short_circuit` task (task_id `dev_ShortCircuit`) that skips every downstream task when `DEV_POWER_SWITCH` isn't `TRUE`.

Neither takes arguments. DAGs moved from `CountyStatAirflow` that pass `op_kwargs={'host_n': ...}` must drop them.

```python
from PanDAirflow.short_circuits import dev_test

dev_check = dev_test()
dev_check >> extract >> load
```

## Tableau Window Check

`short_circuits.tableau_check()` is a `@task.short_circuit` task (task_id `tableau_check`) that skips downstream tasks during the Tableau maintenance window: the 3rd Tuesday of the month (day 15–21) between 20:00 and 23:00, server local time (`pendulum.now()`). It lets them run at any other time. It takes no arguments.

```python
from PanDAirflow.short_circuits import tableau_check

tableau_check() >> refresh_extract
```

## Slack Alert

`slack_alert.task_fail_slack_alert(context)` is an `on_failure_callback`. It posts the task, DAG, `data_interval_start` and log URL to Slack through the Incoming Webhook connection `slack` (`SLACK_CONN_ID`).

```python
from PanDAirflow.slack_alert import task_fail_slack_alert

default_args = {'on_failure_callback': task_fail_slack_alert}
```

## Slack HITL Alert

`SlackHITLNotifier` (`slack_hitl.py`) posts a Slack message when a HITL operator starts waiting. The message has the title, DAG, run, a "Respond by" time (when the task has an `execution_timeout`), the HITL body and a **Review in Airflow** button. Like the Teams notifier, it is link-only: people still respond in the Airflow UI.

* `slack_conn_id` (default `'slack'`): the Slack Incoming Webhook connection, the same one `task_fail_slack_alert` uses.
* `title` (templated): message heading.
* `color` (templated, optional): `'#RRGGBB'` for the message's side bar, e.g. a color being reviewed. Values that aren't 6-digit hex are ignored.
* Markdown images in the body are removed, because Slack can't show them or fetch internal URLs.

```python
from PanDAirflow.slack_hitl import SlackHITLNotifier

HITLBranchOperator(..., notifiers=[SlackHITLNotifier(title='Approval needed',
                                                     color='{{ params.color }}')])
```

Posts only when `DEV_POWER_SWITCH` is `TRUE` on that server. A failed post is logged as a warning with the exception type only, never the webhook URL, and does not fail the task.

## Teams HITL Alert

`TeamsHITLNotifier` (`teams_alert.py`) posts a Teams channel or chat message when a HITL operator starts waiting. The message holds the HITL body and a **Review in Airflow** button that opens the Required Actions page. It is link-only: people still Accept/Reject in the Airflow UI.

### Adding a new chat or channel target
Each target is a Teams Workflows webhook URL stored in an Airflow Variable. Variables are stored separately on each Airflow server, so the same Variable name can point to different targets on dev and prod. For example, dev posts to your personal chat and prod to the team channel.

1. **Create the webhook in Teams** (Apps > **Workflows** > **Create**):
   * **Team channel:** in the channel, **... > Workflows > "Send webhook alerts to a channel"**, pick the team and channel.
   * **Personal or group chat:** **Notify a chat > "Post to a chat when a webhook request is received"**. Set **Post as** to **Flow bot**, and **Post in** to **Chat with Flow bot** (your personal chat) or **Group chat**.
   * Save and copy the HTTP POST URL. **The URL is a secret**: anyone holding it can post to that chat. For team targets, have a shared/team account own or co-own the flow so it keeps working when one person leaves.
2. **Store the URL in an Airflow Variable** whose name contains `secret` (e.g. `ACHD_teams_webhook_secret`), so Airflow masks it. Set it on each server separately, in **Admin > Variables** or from a terminal on that server, without leaving the URL in shell history:
   ```console
   read -rs URL && airflow variables set ACHD_teams_webhook_secret "$URL"; unset URL
   ```
3. **Test the webhook from the server.** This reads the URL from the Variable, never prints it, and posts a test card using the system CA bundle, which is needed behind the county TLS inspection. `202` means the card was delivered:
   ```console
   airflow variables get ACHD_teams_webhook_secret | "$(dirname "$(which airflow)")/python" -c "import sys, requests; r = requests.post(sys.stdin.read().strip(), json={'type': 'message', 'attachments': [{'contentType': 'application/vnd.microsoft.card.adaptive', 'content': {'type': 'AdaptiveCard', 'version': '1.4', 'body': [{'type': 'TextBlock', 'text': 'Airflow webhook test'}]}}]}, timeout=30, verify='/etc/ssl/certs/ca-certificates.crt'); print(r.status_code)"
   ```
4. **Use the Variable name** in the DAG (below). Different DAGs or tables can use different Variables to reach different targets.

Server requirements:
* `AIRFLOW__API__BASE_URL` must be the URL staff use to open that server's Airflow. The **Review in Airflow** link is built from it, so messages from dev open the dev UI.
* The server must trust the county TLS inspection certificate in `/etc/ssl/certs/ca-certificates.crt`. The notifier uses that bundle, or `REQUESTS_CA_BUNDLE` if set.

```python
from PanDAirflow.teams_alert import TeamsHITLNotifier

HITLBranchOperator(..., notifiers=[TeamsHITLNotifier(variable_name='ACHD_teams_webhook_secret',
                                                     title='Approval needed')])
```

Posts only when `DEV_POWER_SWITCH` is `TRUE` on that server (same rule as `dev_test`); if a run logs "skipping Teams notification", that's why. A failed post is logged as a warning with its cause, never the URL, and does not fail the task.

## HITL Review Links

`hitl_links.review_link(context, base_url=None)` returns the URL of a HITL task's Required Actions form. Both HITL notifiers use it to build their **Review in Airflow** button, and custom notifiers or callbacks can call it too.

It fixes the stock `HITLOperator.generate_link_to_ui_from_context` for mapped tasks. The stock link ends `.../required_actions?map_index=N`, but the Airflow UI (checked on 3.2.2) reads the map index only from the path, so the link opened "not found". Mapped tasks get `.../tasks/<task>/mapped/<N>/required_actions`; unmapped tasks keep the stock link.

The base URL comes from `base_url` or `[api] base_url` (`AIRFLOW__API__BASE_URL`). If neither is set, it raises `ValueError`.

## Data Validation

`validates.py` checks warehouse tables against a stored column schema by running the `countystats/data-validate:r` image (`Rscript schema-validate.R`) on a Docker host.

* `validate_task(variable, department_name, schema, connection_name, pool='Remote_Pool', url_var='docker_remote')` returns one `DockerOperator` (task_id `Validate_<variable>`) that validates table `variable`.
* `data_validation(table_list, department_name, schema, connection_name, pool, url_var)` is a `@task_group` (group_id `data_validations`) with one `validate_task` per table in `table_list`. All arguments are required here.

Arguments:
* `variable` / `table_list`: table name(s).
* `department_name`: passed to the container as `DEPT`.
* `schema`: the warehouse schema holding the table, passed as `SOURCE`.
* `connection_name`: an Airflow `Connection` object, not a connection id (e.g. `BaseHook.get_connection('warehouse')`). Its host, schema, login and password are passed as `WH_HOST`, `WH_DB`, `WH_USER` and `WH_PASS`.
* `pool`: an existing Airflow pool.
* `url_var`: name of the Variable holding the Docker host URL.

Airflow Variables it reads:
* `docker_api_version`
* `<table>_schema`: the expected column schema, one Variable per table
* the Variable named by `url_var` (default `docker_remote`)

```python
from airflow.hooks.base import BaseHook
from PanDAirflow.validates import data_validation

wh_conn = BaseHook.get_connection('warehouse')
validations = data_validation(table_list=['Jail_Daily', 'Jail_Bookings'], department_name='Jail',
                              schema='Staging', connection_name=wh_conn,
                              pool='Remote_Pool', url_var='docker_remote')
load >> validations
```

## MOVEit File Sensor

`file_checkit.check_moveit(file_name, folder_path=None, file_types=None)` is a `@task.sensor` that waits for a file to be uploaded to MOVEit. It runs in `reschedule` mode, checks every 12 hours, and fails after 7 days.

On each check it lists the available MOVEit files and filters them:
1. `folder_path`: keep files whose path starts with it (all files if `None`).
2. `file_name`: keep files whose name contains it.
3. `file_types`: keep files whose name ends with one of these extensions, e.g. `['.csv', '.xlsx']` (all types if `None`).

When it finishes:
* If the folder filter leaves exactly one file, any match finishes the sensor.
* If it leaves more than one, exactly one file must match. Several matches (ambiguous naming or repeated uploads) keep it waiting.

The matched file name is returned as the task's XCom value, so a downstream task can pick that file.

Requirements: the `moveitAPI` package (`auth_move_it`, `available_files`) installed on the worker, and the Airflow Variables `moveit_username` and `moveit_password`.

```python
from PanDAirflow.file_checkit import check_moveit

file_name = check_moveit(file_name='Monthly_Calls', folder_path='/Home/CountyStat', file_types=['.csv'])
download_file(file_name)
```
