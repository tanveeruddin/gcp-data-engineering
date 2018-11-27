# I wrote this code to answer Stackoverflow question (https://stackoverflow.com/questions/53285726/conditional-statement-python-apache-beam-pipeline/53355910?noredirect=1#comment93617454_53355910)
import apache_beam as beam
from apache_beam import pvalue
import sys

class Split(beam.DoFn):

    # These tags will be used to tag the outputs of this DoFn.
    OUTPUT_TAG_BQ = 'BigQuery'
    OUTPUT_TAG_PS1 = 'pubsub topic1'
    OUTPUT_TAG_PS2 = 'pubsub topic2'

    def process(self, element):
        """
        tags the input as it processes the orignal PCollection
        """
        print element
        if "BigQuery" in element:
            yield pvalue.TaggedOutput(self.OUTPUT_TAG_BQ, element)
            print 'found bq'
        elif "pubsub topic1" in element:
            yield pvalue.TaggedOutput(self.OUTPUT_TAG_PS1, element)
        elif "pubsub topic2" in element:
            yield pvalue.TaggedOutput(self.OUTPUT_TAG_PS2, element)
        

if __name__ == '__main__':
    output_prefix = 'C:\\pythonVirtual\\Mycodes\\output'
    p = beam.Pipeline(argv=sys.argv)
    lines = (p
            | beam.Create([
               'this line is for BigQuery',
               'this line for pubsub topic1',
               'this line for pubsub topic2']))

    # with_outputs allows accessing the explicitly tagged outputs of a DoFn.
    tagged_lines_result = (lines
                          | beam.ParDo(Split()).with_outputs(
                              Split.OUTPUT_TAG_BQ,
                              Split.OUTPUT_TAG_PS1,
                              Split.OUTPUT_TAG_PS2))

    # tagged_lines_result is an object of type DoOutputsTuple. It supports
    # accessing result in alternative ways.
    bq_records = tagged_lines_result[Split.OUTPUT_TAG_BQ]| "write BQ" >> beam.io.WriteToText(output_prefix + 'bq')
    ps1_records = tagged_lines_result[Split.OUTPUT_TAG_PS1] | "write PS1" >> beam.io.WriteToText(output_prefix + 'ps1')
    ps2_records = tagged_lines_result[Split.OUTPUT_TAG_PS2] | "write PS2" >> beam.io.WriteToText(output_prefix + 'ps2')

    p.run().wait_until_finish()
