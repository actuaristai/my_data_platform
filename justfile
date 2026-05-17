set shell := ["pwsh", "-c"]

PROJECT_NAME := "my_data_platform"
REMOTE_REPO := "git@github.com:actuaristai/my_data_platform.git"
DESCRIPTION := "Opinionated guide to best practices set up of open source data platform with best data engineering practices"

POWERSHELL_SHEBANG := if os() == 'windows' {
  'pwsh.exe'
} else {
  '/usr/bin/env pwsh'
}

# List available commands
help:
    just --list --unsorted

# --- Pipeline ---

# Run pipeline locally (DuckLake on disk, incremental)
run:
    New-Item -ItemType Directory -Force -Path data/storage, data/01_raw, data/pins_board | Out-Null
    uv run sqlmesh --gateway local_gateway plan --auto-apply

# Run pipeline on MotherDuck dev environment (isolated *__dev schemas)
stage:
    uv run sqlmesh --gateway motherduck plan dev --auto-apply

# Promote dev to production on MotherDuck
deploy:
    uv run sqlmesh --gateway motherduck plan --auto-apply

# Download raw ATP/WTA tennis CSVs to data/01_raw/
ingest:
    uv run python -m my_data_platform.ingest

# Run pointblank validation checks across all layers
validate:
    uv run python -m my_data_platform.validate

# Publish gold tables to pins board
publish:
    uv run python -m my_data_platform.publish

# --- Quality ---

# Upgrade ruff, commitizen, autopep8 and sync pre-commit hook revs
sync-hooks:
    uv add --upgrade ruff commitizen autopep8
    uv run pre-commit autoupdate

# Lint using ruff + sqlmesh
lint:
    uv run --only-group lint ruff check src/{{PROJECT_NAME}} --fix
    uv run --only-group lint ruff check tests --fix
    uv run sqlmesh lint

# Run pytest + sqlmesh tests
test:
    uv run --only-group test pytest --cov-report term-missing --cov={{PROJECT_NAME}} -v -p no:faulthandler -W ignore::DeprecationWarning --verbose --doctest-modules
    uv run sqlmesh test

# --- Docs ---

_docs-build:
    uv run quartodoc build
    uv run quarto render

docs: _docs-build
    uv run quarto preview

# --- Init (run once after cloning) ---

init-project: init-env init-pre-commit
    New-Item -ItemType Directory -Force -Path data/01_raw, data/02_bronze, data/03_silver, data/04_gold, data/storage, data/pins_board | Out-Null

init-env:
    uv sync
    uv run python -m ipykernel install --user --name {{PROJECT_NAME}}

init-pre-commit:
    uvx pre-commit install --hook-type pre-commit --hook-type commit-msg
    uvx pre-commit autoupdate
    uvx pre-commit run --all-files

# --- CD ---

cd-publish:
    $env:PRE_COMMIT_ALLOW_NO_CONFIG = "1"; uv run quarto publish gh-pages

cd-release VERSION:
    git checkout -b release-{{VERSION}} develop
    uv run python bump_version.py {{VERSION}}
    uv sync
    uv run cz changelog --incremental
    git commit -a -m "chore: Bumped version number to {{VERSION}}"
    git checkout main
    git merge --no-ff release-{{VERSION}}
    git push
    git tag -a {{VERSION}} -m "add version tag"
    git push origin {{VERSION}}
    git checkout develop
    git merge --no-ff main
    git branch -d release-{{VERSION}}
    git push

update-template *COPIER_OPTIONS:
    uvx copier update --trust --skip-tasks --skip-answered

clean:
    #!{{POWERSHELL_SHEBANG}}
    Remove-Item -Path "_freeze" -Recurse -Confirm -Erroraction 'silentlycontinue'
    Remove-Item -Path ".pytest_cache" -Recurse -Confirm -Erroraction 'silentlycontinue'
    Remove-Item -Path ".ruff_cache" -Recurse -Confirm -Erroraction 'silentlycontinue'
    Remove-Item -Path "__pycache__" -Recurse -Confirm -Erroraction 'silentlycontinue'
    Remove-Item -Path ".quarto" -Recurse -Confirm -Erroraction 'silentlycontinue'
    Get-ChildItem -Path . -Filter "__pycache__" -Recurse -Directory | Remove-Item -Recurse -Force

duckdb:
    #!{{POWERSHELL_SHEBANG}}
    $tmp = New-TemporaryFile
    "ATTACH 'ducklake:data/catalog.ducklake' AS my_lakehouse;" | Set-Content $tmp
    uvx --from duckdb-cli duckdb.exe -ui -init $tmp
    Remove-Item $tmp
