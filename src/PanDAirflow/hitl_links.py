"""Links to a Human-in-the-loop (HITL) task's review form in the Airflow UI."""
from urllib.parse import urlparse, urlunparse

from airflow.configuration import conf
from airflow.providers.standard.operators.hitl import HITLOperator


def review_link(context, base_url=None):
    """Like HITLOperator.generate_link_to_ui_from_context, but correct for mapped HITL tasks.

    The stock link is .../tasks/<task>/required_actions?map_index=N, but the Airflow UI (checked on 3.2.2)
    reads the map index only from the path, so for a mapped task it opens map index -1 ("not found").
    Mapped tasks get the UI's own form, .../tasks/<task>/mapped/<N>/required_actions. The run id stays
    unencoded, as in the UI's links: encoding its ':' and '+' breaks the page.
    """
    ti = context['task_instance']
    map_index = getattr(ti, 'map_index', -1)
    if map_index is None or map_index < 0:
        return HITLOperator.generate_link_to_ui_from_context(context=context, base_url=base_url)
    if not (base_url := base_url or conf.get('api', 'base_url', fallback=None)):
        raise ValueError('Not able to retrieve base_url')
    parsed = urlparse(base_url)
    path = f'/dags/{ti.dag_id}/runs/{ti.run_id}/tasks/{ti.task_id}/mapped/{map_index}/required_actions'
    return urlunparse((parsed.scheme, parsed.netloc, path, '', '', ''))
