import argparse
import json
import logging
import sys
import time

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions
from bigquery_schema_generator.generate_schema import SchemaGenerator, read_existing_schema_from_file
from google.cloud import bigquery
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json


class JobOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--project",
            type=str,
            help="project ID of GCP project",
            default="jp-poc-platform",
        )
        parser.add_argument(
            "--input_subscription",
            type=str,
            help="The Cloud Pub/Sub topic to read from.\n"
                 '"projects/<PROJECT_NAME>/subscriptions/<SUBSCRIPTION_NAME>".',
            default="projects/jp-poc-platform/subscriptions/df-enriched-source-sub",
        )
        parser.add_argument(
            "--bq_window_size",
            type=float,
            help="Error file's window size in number of minutes.",
            default=0.1,
        )
        parser.add_argument(
            "--bq_dataset",
            type=str,
            help="Bigquery Dataset to write raw salesforce data",
            default='enriched_logs',
        )
        parser.add_argument(
            "--bq_table",
            type=str,
            help="Bigquery Table to write raw salesforce data",
            default='logs1',
        )


class GroupWindowsIntoBatches(beam.PTransform):
    def __init__(self, window_size):
        # Convert minutes into seconds.
        self.window_size = int(window_size * 60)

    def expand(self, pcoll):
        return (pcoll
                | 'Add Timestamps' >> beam.Map(lambda x: beam.window.TimestampedValue(x, time.time()))
                | "Window into Fixed Intervals" >> beam.WindowInto(window.FixedWindows(self.window_size))
                | "Groupby" >> beam.GroupByKey()
                | "Abandon Dummy Key" >> beam.MapTuple(lambda _, val: val)
                )


class ConvertDataToJson(beam.DoFn):
    def process(self, pubsub_message):
        # If we use with_attributes=True on beam.io.ReadFromPubSub we need to transform the message to JSON
        # If we use with_attributes=False, the message is already going to be JSON and we can skip this step

        attributes = dict(pubsub_message.attributes)
        data = json.loads(pubsub_message.data.decode("utf-8"))

        # If we want to keep the Pubsub Message attributes we can do it here
        # e.g. data['attribute_x'] = attributes['x']

        yield data


class HandleBadRows(beam.DoFn):

    def __init__(self, bq_dataset, bq_table):
        self.bq_dataset = bq_dataset
        self.bq_table = bq_table

    def start_bundle(self):
        self.client = bigquery.Client()

    def process(self, batch):
        logging.info(f"Got {len(batch)} bad rows")
        logging.info(f"Batch is of type {type(batch)}")
        logging.info(f"Batch 1st record is of type {type(batch[0])}")
        table_id = f"{self.bq_dataset}.{self.bq_table}"

        generator = SchemaGenerator(input_format='dict', quoted_values_are_strings=True, keep_nulls=True)

        # Get original schema to assist the deduce_schema function.
        # If the table doesn't exist
        # proceed with empty original_schema_map
        try:
            table_file_name = f"original_schema_{self.bq_table}.json"
            table = self.client.get_table(table_id)
            self.client.schema_to_json(table.schema, table_file_name)
            original_schema_map = read_existing_schema_from_file(table_file_name)
        except Exception:
            logging.info(f"{table_id} table not exists. Proceed without getting schema")
            original_schema_map = {}

        # generate the new schema
        schema_map, error_logs = generator.deduce_schema(
            input_data=batch,
            schema_map=original_schema_map)
        schema = generator.flatten_schema(schema_map)

        job_config = bigquery.LoadJobConfig(
            source_format=
            bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            ],
            write_disposition=
            bigquery.WriteDisposition.WRITE_APPEND,
            schema=schema
        )

        try:
            load_job = self.client.load_table_from_json(
                batch,
                table_id,
                job_config=job_config,
            )  # Make an API request.

            load_job.result()  # Waits for the job to complete.
            if load_job.errors:
                logging.info(f"error_result =  {load_job.error_result}")
                logging.info(f"errors =  {load_job.errors}")
            else:
                logging.info(f'Loaded {len(batch)} rows.')

        except Exception as error:
            logging.info(f'Error: {error} with loading dataframe')


def run(argv):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True, streaming=True)
    options = pipeline_options.view_as(JobOptions)

    #bq show --schema --format=prettyjson jp-poc-platform:enriched_logs.cloudaudit_googleapis_com_data_access > bq_table.json
    schema_json_file = 'logs1_table.json'
    with open(schema_json_file) as f:
        data = f.read()
        # Wrapping the schema in fields is required for the BigQuery API.
        schema_str = '{"fields": ' + data + '}'

    table_schema = parse_table_schema_from_json(schema_str)

    logging.info(f"Table Schema is .... {table_schema}")

    with beam.Pipeline(options=pipeline_options) as pipeline:
        realtime_data = (
                pipeline
                | "Read PubSub Messages" >> beam.io.ReadFromPubSub(subscription=options.input_subscription, with_attributes=True)
                | "Convert Messages to JSON" >> beam.ParDo(ConvertDataToJson())
                | f"Write to {options.bq_table}" >> beam.io.WriteToBigQuery(
            table=f"{options.bq_dataset}.{options.bq_table}",
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            insert_retry_strategy=beam.io.gcp.bigquery_tools.RetryStrategy.RETRY_NEVER
        )
        )
        #
        # (
        #     realtime_data[beam.io.gcp.bigquery.BigQueryWriteFn.FAILED_ROWS]
        #     | f"Window" >> GroupWindowsIntoBatches(window_size=options.bq_window_size)
        #     | f"Failed Rows for {options.bq_table}" >> beam.ParDo(HandleBadRows(options.bq_dataset, options.bq_table))
        # )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run(sys.argv)
