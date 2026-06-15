.PHONY: install
install:
	if ! command -v uv >/dev/null 2>&1; \
	then echo "uv not found, installing..." \
	&& curl -LsSf https://astral.sh/uv/install.sh | sh; \
	fi
	uv sync --extra django
	uv run pre-commit install

.PHONY: test
test:
	uv run pytest -v

.PHONY: restest
retest:
	uv run pytest -vv --lf

.PHONY: coverage
coverage:
	uv run pytest -vv --cov --cov-report=term-missing

.PHONY: format
format:
	uv run ruff check --fix-only .
	uv run pre-commit run -a

.PHONY: lint
lint:
	uv run ruff check .

.PHONY: typecheck
typecheck:
	uv run ty check
