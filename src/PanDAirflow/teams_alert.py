import logging
import os

import requests
from airflow.sdk import Variable
from airflow.sdk.bases.notifier import BaseNotifier
from .hitl_links import review_link

log = logging.getLogger(__name__)

# County TLS inspection re-signs outbound HTTPS with the Allegheny CA, which is in the OS bundle but not in certifi
SYSTEM_CA_BUNDLE = '/etc/ssl/certs/ca-certificates.crt'


def ca_bundle():
    if os.environ.get('REQUESTS_CA_BUNDLE'):
        return os.environ['REQUESTS_CA_BUNDLE']
    return SYSTEM_CA_BUNDLE if os.path.exists(SYSTEM_CA_BUNDLE) else True


def safe_reason(exc):
    """Innermost cause (e.g. 'certificate verify failed: ...') without the request URL, which holds the webhook signature."""
    while getattr(exc, 'reason', None) is not None or (exc.args and isinstance(exc.args[0], Exception)):
        exc = exc.reason if getattr(exc, 'reason', None) is not None else exc.args[0]
    msg = str(getattr(exc, 'verify_message', '') or getattr(exc, 'strerror', '') or '')
    return f'{type(exc).__name__}: {msg}' if msg and 'http' not in msg.lower() else type(exc).__name__


class TeamsHITLNotifier(BaseNotifier):
    """
    Posts a Teams channel message (Adaptive Card) when a HITL operator starts waiting,
    with a button linking to the Airflow Required Actions page. Responses still happen in Airflow.

    :param variable_name: Airflow Variable holding the Teams Workflows webhook URL
        (include "secret" in the name so Airflow masks it).
    :param title: card heading.
    """

    def __init__(self, variable_name, title='Airflow approval needed'):
        super().__init__()
        self.variable_name = variable_name
        self.title = title

    def build_card(self, context, link):
        ti = context['task_instance']
        task = context['task']
        facts = [
            {'title': 'DAG', 'value': ti.dag_id},
            {'title': 'Run', 'value': str(context.get('data_interval_end') or ti.run_id)},
        ]
        if task.execution_timeout and ti.start_date:
            respond_by = ti.start_date + task.execution_timeout
            facts.append({'title': 'Respond by', 'value': respond_by.strftime('%Y-%m-%d %H:%M %Z')})
        return {
            'type': 'message',
            'attachments': [{
                'contentType': 'application/vnd.microsoft.card.adaptive',
                'content': {
                    '$schema': 'http://adaptivecards.io/schemas/adaptive-card.json',
                    'type': 'AdaptiveCard',
                    'version': '1.4',
                    'body': [
                        {'type': 'TextBlock', 'text': self.title, 'weight': 'Bolder', 'size': 'Medium', 'wrap': True},
                        {'type': 'FactSet', 'facts': facts},
                        {'type': 'TextBlock', 'text': getattr(task, 'body', None) or '', 'wrap': True},
                    ],
                    'actions': [{'type': 'Action.OpenUrl', 'title': 'Review in Airflow', 'url': link}],
                },
            }],
        }

    def notify(self, context):
        # Prod only: same rule as short_circuits.dev_test
        if Variable.get('DEV_POWER_SWITCH', default='FALSE') != 'TRUE':
            log.info('DEV_POWER_SWITCH is not TRUE; skipping Teams notification.')
            return
        # A failed post must not fail the HITL task; the request is still answerable in the UI
        try:
            link = review_link(context)
            resp = requests.post(Variable.get(self.variable_name), json=self.build_card(context, link),
                                 timeout=30, verify=ca_bundle())
            resp.raise_for_status()
        except Exception as e:
            # Never log str(e): requests errors include the webhook URL, which is a secret
            status = getattr(getattr(e, 'response', None), 'status_code', None)
            log.warning('Teams notification failed (%s%s; cause: %s).', type(e).__name__,
                        f', HTTP {status}' if status else '', safe_reason(e))