import logging

import requests
from airflow.sdk import Variable
from airflow.sdk.bases.notifier import BaseNotifier
from airflow.providers.standard.operators.hitl import HITLOperator

log = logging.getLogger(__name__)


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
            link = HITLOperator.generate_link_to_ui_from_context(context=context)
            resp = requests.post(Variable.get(self.variable_name), json=self.build_card(context, link), timeout=30)
            resp.raise_for_status()
        except Exception as e:
            # Log only the exception type and status; the webhook URL is a secret
            status = getattr(getattr(e, 'response', None), 'status_code', None)
            log.warning('Teams notification failed (%s%s).', type(e).__name__, f', HTTP {status}' if status else '')