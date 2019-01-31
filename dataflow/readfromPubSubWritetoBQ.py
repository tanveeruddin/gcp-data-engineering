# This example reads from pubsub and writes into Bigquery whatever messages it has collected in fixed 10 seconds windows.

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.pipeline import PipelineOptions
#from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions, StandardOptions

# You use PipelineOptions to configure how and where your pipeline executes and what resources it uses.
TABLE_SCHEMA = ('message:STRING, '
                'window_start:TIMESTAMP, window_end:TIMESTAMP')
class FormatDoFn(beam.DoFn):
  def process(self, element, window=beam.DoFn.WindowParam):
    ts_format = '%Y-%m-%d %H:%M:%S.%f UTC'
    window_start = window.start.to_utc_datetime().strftime(ts_format)
    window_end = window.end.to_utc_datetime().strftime(ts_format)
    return [{'message': element,
             'window_start':window_start,
             'window_end':window_end}]
             
if __name__ == '__main__':
   options = PipelineOptions()
   google_cloud_options = options.view_as(GoogleCloudOptions)
   google_cloud_options.project = 'august-button-203520'
   google_cloud_options.job_name = 'mypipe'
   google_cloud_options.staging_location = 'gs://test-gcp-bucket-tu/staging'
   google_cloud_options.temp_location = 'gs://test-gcp-bucket-tu/temp'
   options.view_as(StandardOptions).runner = 'DirectRunner' #'DataflowRunner'
   options.view_as(StandardOptions).streaming = True
   output = 'gs://test-gcp-bucket-tu/output'
   p = beam.Pipeline(options=options)
   
   #GoogleCloudOptions vs StandardOptions: if you are using GCP services e.g. BQ, GCS, you might need to set certain GCP project and credential options. In such cases, you should use options.view_as(GoogleCloudOptions).project to set your Google Cloud Project ID. 
   
   (p
      | 'ReadPubSub' >> beam.io.ReadFromPubSub(subscription='projects/august-button-203520/subscriptions/mySub1') #you can read from topic too. 
      | 'window' >> beam.WindowInto(beam.window.FixedWindows(10))
      | 'Format' >> beam.ParDo(FormatDoFn())
      | 'Write' >> beam.io.WriteToBigQuery('august-button-203520:mytesting.pubsubmessage',schema=TABLE_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
      #| 'write' >> beam.io.WriteToText(output, num_shards=1)
   )

   p.run().wait_until_finish()

'''in a separate window, create pubsub topic, subscription and start publishing message   
gcloud pubsub topics create sandiego
gcloud pubsub subscriptions create --topic sandiego mySub1
gcloud pubsub topics publish sandiego --message "hello1" --message "hello2" --message "hello3" 
gcloud pubsub subscriptions pull --auto-ack mySub1

gcloud pubsub topics publish sandiego --message "hello8"
gcloud pubsub topics publish sandiego --message "hello9"
