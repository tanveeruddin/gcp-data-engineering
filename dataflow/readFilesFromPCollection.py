#I wrote this code to answer stackoverflow question https://stackoverflow.com/questions/53801600/create-pcollection-of-gcs-objects-in-google-cloud-dataflow-apache-beam
#rather than passing a wildcard parameter of file location, it reads from PCollection of file names

from __future__ import absolute_import

import argparse
import logging
import re

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText, ReadAllFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp import gcsio


class WordExtractingDoFn(beam.DoFn):
  """Parse each line of input text into words."""

  def __init__(self):
    super(WordExtractingDoFn, self).__init__()
  def process(self, element):  
    text_line = element.strip()
    return text_line


def run(argv=None):
  """Main entry point; defines and runs the wordcount pipeline."""
  output_prefix = 'C:\\pythonVirtual\\Mycodes\\output'
  p = beam.Pipeline(options=PipelineOptions())

  # Read the text file[pattern] into a PCollection.
  elements =  ['C:\\pythonVirtual\\Mycodes\\aggregate.csv',
              'C:\\pythonVirtual\\Mycodes\\aggregate3.csv']

  books = p | beam.Create((elements))
  
  lines = books | 'read' >> ReadAllFromText()

  counts = (lines
            | 'split' >> (beam.ParDo(WordExtractingDoFn())
                          .with_output_types(unicode)))

  output = counts | 'write' >> beam.io.WriteToText(output_prefix)

  result = p.run()
  result.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()