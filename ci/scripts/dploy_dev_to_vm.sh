#!/usr/bin/env bash
set -e

VM="mgradmin@MGRAFDv3"
DAG_DIR="/home/mgradmin/airflow/dags/dev_PanDAirflow"

echo "Deploying dev PanDAirflow folder to Airflow VM..."

ssh $VM "mkdir -p $DAG_DIR"

rsync -av --delete src/PanDAirflow/ $VM:$DAG_DIR/

echo "Deployment complete."
