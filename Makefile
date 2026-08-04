test:
	pytest tests/unit

package:
	python -m build

lint:
	flake8 src/PanDAirflow

deploy-dev:
	ci/scripts/deploy_dev_to_vm.sh