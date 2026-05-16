# Description

Opinionated guide to best practices set up of open source data platform with best data engineering practices


# How to use

1. `just ingest`: to download all the files locally
2. `just run`: to set up ducklake locally

# Duckdb ui
````
uvx --from duckdb-cli duckdb.exe -ui
ATTACH 'ducklake:data/catalog.ducklake' AS my_ducklake;
```