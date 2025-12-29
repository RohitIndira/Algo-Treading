.PHONY: init
## init: install requirements
init:
	pip install -r requirements.txt
	pip install -r requirements-dev.txt

.PHONY: check
## check: check if everything's okay
check:
	isort --recursive --check-only pycloudrestapi tests
	pylint pycloudrestapi

.PHONY: format
## format: format files
format:
	isort -rc -y pycloudrestapi tests

.PHONY: test
## test: run tests
test:
	python -m pytest

.PHONY: coverage
## coverage: run tests with coverage
coverage:
	python -m pytest --cov pycloudrestapi --cov-report term --cov-report xml

.PHONY: htmlcov
## htmlcov: run tests with coverage and create coverage report HTML files
htmlcov:
	python -m pytest --cov pycloudrestapi --cov-report html
	rm -rf /tmp/htmlcov && mv htmlcov /tmp/
	open /tmp/htmlcov/index.html

.PHONY: help
## help: prints this help message
help:
	@echo "Usage: \n"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':'
