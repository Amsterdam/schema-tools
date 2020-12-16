.PHONY: install
install:
	pip install -e .
	pip install pre-commit
	pre-commit install

test:
	pytest -v tests/ django_tests

retest:
	pytest -vv --lf tests/ django_tests/

.PHONY: coverage
coverage:
	pytest -vv --cov --cov-report=term-missing tests/ django_tests/

.PHONY: format
format:
	pre-commit run -a
