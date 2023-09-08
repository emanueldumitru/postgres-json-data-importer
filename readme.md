# Python data importer json to postgres

How to build it 

```
docker build -t data-importer .
```

How to run it

```
docker run --rm \
  -v /Users/emanueldumitru/Desktop/work/onyxia/workspace-code/data-importer/input:/tmp/json_input_path \
  -e POSTGRES_OLAP_HOST=localhost \
  -e POSTGRES_OLAP_PORT=5471 \
  -e POSTGRES_OLAP_DATABASE='onyxia_olap' \
  -e POSTGRES_OLAP_USERNAME=postgres \
  -e POSTGRES_OLAP_PASSWORD=postgres \
  data-importer \
  --table_name=google_workspace \
  --schema=owenco \
  --stream=assets \
  --json_files_path=/tmp/json_input_path
```