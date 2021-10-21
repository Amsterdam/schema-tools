.PHONY: install
install:
	pip install -e '.[dev,tests,django]'
	pip install pre-commit
	pre-commit install

.PHONY: test
test:
	pytest -v

.PHONY: restest
retest:
	pytest -vv --lf

.PHONY: coverage
coverage:
	pytest -vv --cov --cov-report=term-missing

.PHONY: format
format:
	pre-commit run -a

.PHONY: clean
clean:
	find . -type d -name __pycache__ -exec rm -r {} \+
	rm -rf build dist

.PHONY: build
build: clean
	python -m build --sdist --wheel .

.PHONY: upload
upload: build
	python -m twine upload --repository-url https://upload.pypi.org/legacy/ --username datapunt dist/*
