.PHONY: install
install:
	pip install -e '.[tests,django]'
	pip install pre-commit
	pre-commit install

test:
	pytest -v

retest:
	pytest -vv --lf

.PHONY: coverage
coverage:
	pytest -vv --cov --cov-report=term-missing

.PHONY: format
format:
	pre-commit run -a
