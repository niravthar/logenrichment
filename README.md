# logenrichment

To run on local,

Update input and output args in main.py

Create bq table
bq mk --table --expiration 3600 --description "Enriched Logs Table" --label organization:development  jp-poc-platform:enriched_logs.logs1   logs1_table.json

open gitbash
export PYTHON_PATH=<BASE_DIR>/enrichlogs/logs

cd <BASE_DIR>
python -m logs.main
