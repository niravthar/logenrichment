import argparse
import logging
import sys
import json
from google.cloud import resource_manager
from google.cloud import logging
from typing import Dict
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.utils import shared

def run_command(self, description, command):
    import subprocess
    logger = logging

    try:
        status = subprocess.call(command)
    except Exception as e:
        raise Exception(description + ' caught exception: ' + str(e))
    if status == 0:
        logger.debug(description + ': `' + ' '.join(command) +
                     '` completed successfully')
        return status
    else:
        raise Exception(description + ' failed with signal ' +
                        str(status))


def start_bundle(self):
    logger = logging
    logger.debug('start_bundle firing')

    try:
        update_status = self.run_command('apt-get update',
                                         ['sudo', 'apt-get', 'update'])
        gcs_status = self.run_command('pull from gcs bucket pypi packages',
                                      ['gsutil', 'cp', 'gs://dataflow_python_dependencies/enrichlogs-0.0.1.tar.gz',
                                       '.'])
        pip_status = self.run_command('pip install',
                                      ['pip', 'install', '--user', 'enrichlogs-0.0.1.tar.gz'])

    except Exception as e:
        raise e

    if update_status == 0 and \
            pip_status == 0 and \
            gcs_status == 0:
        logger.debug('start_bundle completed successfully')
        return True
    else:
        return False


class JobOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # parser.add_argument(
        #     "--project",
        #     type=str,
        #     help="project ID of GCP project",
        #     default="jp-poc-platform",
        # )
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

class ConvertDataToJson(beam.DoFn):
    def process(self, pubsub_message):
        # If we use with_attributes=True on beam.io.ReadFromPubSub we need to transform the message to JSON
        # If we use with_attributes=False, the message is already going to be JSON and we can skip this step

        attributes = dict(pubsub_message.attributes)
        data = json.loads(pubsub_message.data.decode("utf-8"))

        # If we want to keep the Pubsub Message attributes we can do it here
        # e.g. data['attribute_x'] = attributes['x']

        yield data

class EnrichLabels(beam.DoFn):
    def __init__(self, shared_handle: shared.Shared):
        self._shared_handle = shared_handle
        self.sideinput_data = None
        self.sideinput_project_labels = None

    def start_bundle(self):
        self.sideinput_data = None

    # [START logging_write_log_entry]
    def write_entry(self, logger_entry):
        """Writes log entries to the given logger."""
        logging_client = logging.Client()

        # This log can be found in the Cloud Logging console under 'Custom Logs'.
        logger = logging_client.logger("enriched_log")

        # Struct log. The struct can be any JSON-serializable dictionary.
        logger.log_struct(logger_entry)

        print("Wrote logs to {}.".format(logger.name))
        # [END logging_write_log_entry]

    def process(self, log_payload):
        def get_sideinput(shared_handle=None):

            def load_sideinput():
                # logging.getLogger().info("Inside load-sideinput")
                # Container to hold the various side input details. Key represents side input name and value represents side input data
                class _SideInputContainer(Dict):
                    pass

                sideinput_container = _SideInputContainer()
                # Uncomment to use Application Default Credentials (ADC)
                client = resource_manager.Client()
                # Uncomment to use Service Account Credentials in Json format
                # client = resource_manager.Client.from_service_account_json(
                #     "C:\\Thar\\GCP\\enrichlogs\\advance-block-255313-c3a07d4b6ede.json")
                sideinput_container['project_labels'] = {}
                for project in client.list_projects():
                    sideinput_dict = sideinput_container['project_labels']
                    sideinput_dict[project.project_id] = project.labels

                return sideinput_container

            sideinput_data = shared_handle.acquire(load_sideinput)
            # TODO:: Refresh cache only when data changes
            return sideinput_data

        if self.sideinput_data is None:  # Create a local reference to shared object at start of each bundle
            self.sideinput_data = get_sideinput(self._shared_handle)
            self.sideinput_project_labels = self.sideinput_data.get("project_labels")

        # logging.getLogger().info("Enrich labels")
        log_payload_labels = log_payload['resource']['labels']
        project_id = log_payload_labels['project_id']
        log_payload_labels.update(self.sideinput_project_labels.get(project_id))
        prj_labels = {'seal_id': 'seal999', 'env': 'enrich'}
        log_payload_labels.update(prj_labels)
        self.write_entry(log_payload)
        yield log_payload


def run(argv):
    """Main entry point; defines and runs the enrichlogs pipeline."""
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True, streaming=True)
    pipeline_args.extend([
        # CHANGE 2/6: (OPTIONAL) Change this to DataflowRunner to
        # run your pipeline on the Google Cloud Dataflow Service.
        '--runner=DataflowRunner',
        # CHANGE 3/6: (OPTIONAL) Your project ID is required in order to
        # run your pipeline on the Google Cloud Dataflow Service.
        # '--project=jp-poc-platform',
        # CHANGE 4/6: (OPTIONAL) The Google Cloud region (e.g. us-central1)
        # is required in order to run your pipeline on the Google Cloud
        # Dataflow Service.
        '--region=us-east4',
        # CHANGE 5/6: Your Google Cloud Storage path is required for staging local
        # files.
        '--staging_location=gs://jp-dataflow-demo/staging/',
        # '--staging_location=gs://lab_log_export/stage/',
        # '--staging_location=gs://dataflow_python_dependencies/',
        # CHANGE 6/6: Your Google Cloud Storage path is required for temporary
        # files.
        '--temp_location=gs://jp-dataflow-demo/tmp/',
        # '--temp_location=gs://lab_log_export/tmp/',
        # '--temp_location=gs://dataflow_python_dependencies/',
        '--worker_machine_type=n2-standard-2',
        '--job_name=enrich-logs-demo',
        # '--extra_package=C:\\Thar\\GCP\\enrichlogs\\dist\\enrichlogs-0.0.1.tar.gz',
        # '--no_use_public_ips'
        '--requirements_file=C:\\Thar\\logenrichment\\requirements.txt',
        # '--setup_file=C:\\Thar\\logenrichment\\setup.py'
    ])
    options = pipeline_options.view_as(JobOptions)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Create Shared handles
        sideinput_shared_handle = shared.Shared()
        (pipeline
        | "Read PubSub Messages" >> beam.io.ReadFromPubSub(subscription=options.input_subscription, with_attributes=True)
        | "Convert Messages to JSON" >> beam.ParDo(ConvertDataToJson())
        | 'Get Labels' >> beam.ParDo(EnrichLabels(sideinput_shared_handle)))

        # logging.info("Completed enriching logs")

if __name__ == '__main__':
    #logging.getLogger().setLevel(logging.INFO)
    run(sys.argv)
