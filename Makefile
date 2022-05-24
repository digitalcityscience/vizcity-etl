TAG=ghcr.io/digitalcityscience/vizcity-etl

help:
	@echo "for now, you are on your own"

init:
	python3 -m venv --prompt vizcity-etl .venv
	source .venv/bin/activate

install:
	pip install -r requirements-dev.txt

test: test_*.py
	python -m pytest -ra

update-snapshot:
	python -m pytest --snapshot-update

docker:
	docker build --network=host -t ${TAG}:latest .

run-docker: 
	docker run --rm ${TAG}