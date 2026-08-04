
#!/usr/bin/env bash
set -e

VM="mgradmin@MGRAFDv3"
TASK_ID="check_moveit"
DAG_ID="test_file_checkit"

ssh $VM "cd /home/mgradmin/airflow/airflow_env && \
         source bin/activate && \
         airflow tasks test $DAG_ID $TASK_ID 2024-01-01"
