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

version := $(shell awk '/^version = / {print $$3}' setup.cfg)

.PHONY: upload
upload: build
	[ "$$(head -n 1 CHANGES.md)" = "# $$(date +%Y-%m-%d) (${version})" ]
	python -m twine upload dist/*
