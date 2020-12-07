.PHONY: install
install:
	pip install -e .
	pip install pre-commit
	pre-commit install

test:
	pytest -v tests/

retest:
	pytest -vv --lf tests/

.PHONY: coverage
coverage:
	pytest -vv --cov --cov-report=term-missing tests/

.PHONY: format
format:
	pre-commit run -a
