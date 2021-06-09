import argparse
import logging
import json
from google.cloud import resource_manager
from typing import Dict
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.utils import shared


def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the enrichlogs pipeline."""

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      #default='gs://lab_log_export/cloudaudit.googleapis.com/activity/2021/06/04/*.json',
      #default='gs://lab_log_export/cloudaudit.googleapis.com/data_access/2021/06/04/*.json',
      default='gs://lab_log_export/cloudaudit.googleapis.com/system_event/2021/06/04/*.json',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      # CHANGE 1/6: The Google Cloud Storage path is required
      # for outputting the results.
      #default='gs://YOUR_OUTPUT_BUCKET/AND_OUTPUT_PREFIX',
      default='/Thar/GCP/enrichlogs/tmp',
      #default='gs://lab_log_export/enriched_logs/cloudaudit.googleapis.com/activity/2021/06/04/enrich',
      #default='gs://lab_log_export/enriched_logs/cloudaudit.googleapis.com/data_access/2021/06/04/enrich',
      #default='gs://lab_log_export/enriched_logs/cloudaudit.googleapis.com/system_event/2021/06/04/enrich',
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_args.extend([
      # CHANGE 2/6: (OPTIONAL) Change this to DataflowRunner to
      # run your pipeline on the Google Cloud Dataflow Service.
      '--runner=dataflow',
      # CHANGE 3/6: (OPTIONAL) Your project ID is required in order to
      # run your pipeline on the Google Cloud Dataflow Service.
      '--project=advance-block-255313',
      # CHANGE 4/6: (OPTIONAL) The Google Cloud region (e.g. us-central1)
      # is required in order to run your pipeline on the Google Cloud
      # Dataflow Service.
      '--region=us-east4',
      # CHANGE 5/6: Your Google Cloud Storage path is required for staging local
      # files.
      '--staging_location=gs://lab_log_export/stage/',
      # CHANGE 6/6: Your Google Cloud Storage path is required for temporary
      # files.
      '--temp_location=gs://lab_log_export/tmp/',
      '--worker_machine_type=n2-standard-2',
      '--job_name=enrich-logs-demo',
      #'--extra_package=C:\\Thar\\GCP\\enrichlogs\\Lib\\site-packages\\google.zip'
      #'--requirements_file=C:\\Thar\\GCP\\enrichlogs\\requirements.txt',
      '--setup_file=C:\\Thar\\GCP\\enrichlogs\\setup.py'
  ])

  class EnrichLabels(beam.DoFn):
      def __init__(self, shared_handle: shared.Shared):
          self._shared_handle = shared_handle
          self.sideinput_data = None
          self.sideinput_project_labels = None

      def start_bundle(self):
          self.sideinput_data = None

      def process(self, log_payload):
          def get_sideinput(shared_handle=None):
              def load_sideinput():
                  logging.getLogger().info("Inside load-sideinput")
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

          logging.getLogger().info("Enrich labels")
          log_payload_labels = log_payload['resource']['labels']
          project_id = log_payload_labels['project_id']

          log_payload_labels.update(self.sideinput_project_labels.get(project_id))
          yield log_payload

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  with beam.Pipeline(options=pipeline_options) as p:
    # Create Shared handles
    sideinput_shared_handle = shared.Shared()
    # Read the text file[pattern] into a PCollection.
    logging.getLogger().info("Starting to read from GCS")
    lines = p | ReadFromText(known_args.input)
    logging.getLogger().info("Completed reading from GCS")
    # Count the occurrences of each word.
    output = (
        lines
          | 'Json To Dict' >> beam.Map(lambda x: json.loads(x))
          | 'Get Labels' >> beam.ParDo(EnrichLabels(sideinput_shared_handle))
    )

    logging.getLogger().info("Completed enriching logs")

    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    output | WriteToText(known_args.output)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()