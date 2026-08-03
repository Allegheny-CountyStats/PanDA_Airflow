from airflow import DAG
from datetime import datetime, timedelta

# Import your dev sensor module inside DAGs folder
from dev_PanDAirflow.file_checkit import check_moveit

with DAG(
    dag_id="test_file_checkit",
    start_date=datetime.now() - timedelta(days=1),
    schedule=None,
    catchup=False
):
    check_moveit(
        file_name="testfile",
        folder_path="/sample/path",
        file_types=[".csv"]
    )
