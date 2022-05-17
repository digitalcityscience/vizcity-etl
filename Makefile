help:
	@echo "for now, you are on your own"

init:
	python3 -m venv --prompt iot-parser .venv
	source .venv/bin/activate

install:
	pip install -r requirements-dev.txt

test: test_*.py
	python -m pytest -ra

update-snapshot:
	python -m pytest --snapshot-update