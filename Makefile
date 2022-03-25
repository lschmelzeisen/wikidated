# See: https://blog.thapaliya.com/posts/well-documented-makefiles/
help: ##- Show this help message.
	@awk 'BEGIN {FS = ":.*##-"; printf "usage: make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z0-9_-]+:.*##-/ { printf "  \033[36m%-29s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
.PHONY: help

# ------------------------------------------------------------------------------

devinstall: ##- Install the project in editable mode with all test and dev dependencies (in a virtual environment).
	@poetry env use 3.7
	@poetry install -E test -E build -E analysis
.PHONY: devinstall

# ------------------------------------------------------------------------------

test: test-pytest ##- Run all tests and report test coverage.
.PHONY: test

test-pytest: ##- Run all tests in the currently active environment.
	@poetry run pytest --cov --cov-report= --cov-context test --html tests-report.html --self-contained-html
	@poetry run coverage html --dir tests-coverage
	@poetry run coverage report
.PHONY: test-pytest

test-nox: ##- Run all tests against all supported Python versions (in separate environments).
	@poetry run coverage erase
	@poetry run nox
	@poetry run coverage html --dir tests-coverage
	@poetry run coverage report
.PHONY: test-nox

# ------------------------------------------------------------------------------

check: check-flake8 check-mypy check-vulture check-isort check-black ##- Run linters and perform static type-checking.
.PHONY: check

check-flake8: ##- Run linters.
	@poetry run flake8 scripts src tests noxfile.py
.PHONY: check-flake8

check-mypy: ##- Run static type-checking.
	@poetry run mypy scripts src tests noxfile.py
.PHONY: check-mypy

check-vulture: ##- Check for unused code.
	@poetry run vulture scripts src tests noxfile.py vulture-whitelist.py
.PHONY: check-vulture

check-isort: ##- Check if imports are sorted correctly.
	@poetry run isort --check-only --quiet scripts src tests noxfile.py
.PHONY: check-isort

check-black: ##- Check if code is formatted correctly.
	@poetry run black --check scripts src tests noxfile.py
.PHONY: check-black

# ------------------------------------------------------------------------------

format: format-licenseheaders format-isort format-black ##- Auto format all code.
.PHONY: format

format-licenseheaders: ##- Prepend license headers to all code files.
	@poetry run licenseheaders --tmpl LICENSE.header --years 2021-2022 --owner "Lukas Schmelzeisen" --dir scripts
	@poetry run licenseheaders --tmpl LICENSE.header --years 2021-2022 --owner "Lukas Schmelzeisen" --dir src
	@poetry run licenseheaders --tmpl LICENSE.header --years 2021-2022 --owner "Lukas Schmelzeisen" --dir tests
	@poetry run licenseheaders --tmpl LICENSE.header --years 2021-2022 --owner "Lukas Schmelzeisen" -f noxfile.py
.PHONY: format-licenseheaders

format-isort: ##- Sort all imports.
	@poetry run isort --quiet scripts src tests noxfile.py
.PHONY: format-isort

format-black: ##- Format all code.
	@poetry run black scripts src tests noxfile.py
.PHONY: format-black

# ------------------------------------------------------------------------------

clean: ##- Remove all created cache/build files, test/coverage reports, and virtual environments.
	@rm -rf __pycache__ .coverage* .eggs .mypy_cache .pytest_cache .nox .venv dist src/*.egg-info tests-coverage tests-report.html
	@find scripts src tests -type d -name __pycache__ -exec rm -r {} +
	@rm -rf wikidata-toolkit/{.gradle,jars}
.PHONY: clean

# ------------------------------------------------------------------------------

build-vulture-whitelistpy: ##- Regenerate vulture whitelist (list of currently seemingly unused code that will not be reported).
	@poetry run vulture scripts src tests *.py --make-whitelist > vulture-whitelist.py || true
.PHONY: build-vulture-whitelistpy
