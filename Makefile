.PHONY: docs

init:
	poetry install

test:
	poetry run pytest tests

publish:
	poetry build
	poetry publish

docs:
	cd docs && make html
	@echo "\033[95m\n\nBuild successful! View the docs homepage at docs/_build/html/index.html.\n\033[0m"
