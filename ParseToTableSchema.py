from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
import json
#from apache_beam.io.gcp.internal.clients import bigquery
from google.cloud import bigquery

def _get_field_schema(**kwargs):
    field_schema = bigquery.TableFieldSchema()
    field_schema.name = kwargs['name']
    field_schema.type = kwargs.get('type', 'STRING')
    field_schema.mode = kwargs.get('mode', 'NULLABLE')
    fields = kwargs.get('fields')
    if fields:
        for field in fields:
            field_schema.fields.append(_get_field_schema(**field))
    return field_schema


def _inject_fields(fields, table_schema):
    for field in fields:
        table_schema.fields.append(_get_field_schema(**field))


def parse_bq_json_schema(schema):
    table_schema = bigquery.TableSchema()
    _inject_fields(schema['fields'], table_schema)
    return table_schema


# with open('bq_table.json') as f:
#     schema_string = f.read()
# print(schema_string)
# # table_schema = parse_table_schema_from_json(schema_string)
# table_schema = parse_bq_json_schema(schema_string)



client = bigquery.Client()
table_schema = client.schema_from_json('bq_table.json')

print(table_schema)

