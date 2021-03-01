.PHONY: install
install:
	pip install -e '.[tests]'
	pip install pre-commit
	pre-commit install

test:
	pytest -v tests/ tests_django/

retest:
	pytest -vv --lf tests/ tests_django/

.PHONY: coverage
coverage:
	pytest -vv --cov --cov-report=term-missing tests/ tests_django/

.PHONY: format
format:
	pre-commit run -a
