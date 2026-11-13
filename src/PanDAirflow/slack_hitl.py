import logging
import re

from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.providers.standard.operators.hitl import HITLOperator
from airflow.sdk import Variable
from airflow.sdk.bases.notifier import BaseNotifier

log = logging.getLogger(__name__)

HEX_COLOR = re.compile(r'^#?[0-9A-Fa-f]{6}$')
# Markdown image plus one leading space, e.g. ' ![#FAC090](https://airflow/color-swatch/FAC090.svg)'
MD_IMAGE = re.compile(r' ?!\[[^\]]*\]\([^)\s]*\)')


class SlackHITLNotifier(BaseNotifier):
    """
    Posts a Slack message through an Incoming Webhook connection when a HITL operator starts waiting,
    with a button linking to the Airflow Required Actions page. Responses still happen in Airflow.

    :param slack_conn_id: Slack Incoming Webhook connection (same one task_fail_slack_alert uses by default).
    :param title: message heading (templated).
    :param color: optional '#RRGGBB' for the message's side bar, e.g. a color being reviewed (templated).
    """

    template_fields = ('title', 'color')

    def __init__(self, slack_conn_id='slack', title='Airflow approval needed', color=None):
        super().__init__()
        self.slack_conn_id = slack_conn_id
        self.title = title
        self.color = color

    def build_message(self, context, link):
        ti = context['task_instance']
        task = context['task']
        fields = [f'*DAG:* {ti.dag_id}', f"*Run:* {context.get('data_interval_end') or ti.run_id}"]
        if task.execution_timeout and ti.start_date:
            respond_by = ti.start_date + task.execution_timeout
            fields.append(f"*Respond by:* {respond_by.strftime('%Y-%m-%d %H:%M %Z')}")
        blocks = [
            {'type': 'header', 'text': {'type': 'plain_text', 'text': self.title[:150]}},
            {'type': 'section', 'fields': [{'type': 'mrkdwn', 'text': f} for f in fields]},
        ]
        body = getattr(task, 'body', None)
        if body:
            # Slack mrkdwn can't show Markdown images, and Slack can't fetch internal URLs; drop them.
            body = MD_IMAGE.sub('', body)
            blocks.append({'type': 'section', 'text': {'type': 'mrkdwn', 'text': body[:3000]}})
        blocks.append({'type': 'actions', 'elements': [
            {'type': 'button', 'text': {'type': 'plain_text', 'text': 'Review in Airflow'}, 'url': link,
             'style': 'primary'}]})
        attachment = {'blocks': blocks}
        if self.color and HEX_COLOR.match(str(self.color)):
            attachment['color'] = '#' + str(self.color).lstrip('#')
        return {'text': self.title, 'attachments': [attachment]}

    def notify(self, context):
        # Prod only: same rule as short_circuits.dev_test and TeamsHITLNotifier
        if Variable.get('DEV_POWER_SWITCH', default='FALSE') != 'TRUE':
            log.info('DEV_POWER_SWITCH is not TRUE; skipping Slack notification.')
            return
        # A failed post must not fail the HITL task; the request is still answerable in the UI
        try:
            link = HITLOperator.generate_link_to_ui_from_context(context=context)
            SlackWebhookHook(slack_webhook_conn_id=self.slack_conn_id).send(**self.build_message(context, link))
        except Exception as e:
            # Never log str(e): webhook errors can include the webhook URL, which is a secret
            log.warning('Slack HITL notification failed (%s).', type(e).__name__)