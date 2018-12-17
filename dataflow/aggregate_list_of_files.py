#I wrote this code to answer stackoverflow question https://stackoverflow.com/questions/53801600/create-pcollection-of-gcs-objects-in-google-cloud-dataflow-apache-beam
#rather than passing a wildcard parameter of file location, it reads from PCollection of file names

import apache_beam as beam
import sys
import logging


class Split(beam.DoFn):

    def process(self, element):
        """
        Splits each row on commas and returns a tuple representing the row to process
        """
        customer_id, customer_name, transction_amount, transaction_type = element.split(",")
        return [
            (customer_id +","+customer_name+","+transaction_type, float(transction_amount))
        ]

class ProcessFile(beam.DoFn):

    def process(self, element):
        """
        Splits each row on commas and returns a tuple representing the row to process
        """
        print(element)
        return [element]
        
if __name__ == '__main__':
   p = beam.Pipeline(argv=sys.argv)
   
   input = beam.Create([
               'C:\\pythonVirtual\\Mycodes\\aggregate.csv',
              'C:\\pythonVirtual\\Mycodes\\aggregate3.csv'])
   
   output_prefix = 'C:\\pythonVirtual\\Mycodes\\output'
   
   (p
      | 'ListOfFiles' >> beam.Create(['C:\\pythonVirtual\\Mycodes\\aggregate.csv','C:\\pythonVirtual\\Mycodes\\aggregate3.csv'])
      | 'ReadFile' >> beam.io.ReadAllFromText ()
      #| 'ParseFiles' >> beam.ParDo(ProcessFile())
      #| 'ReadFile' >> beam.io.ReadAllFromText (input)
      #| 'ReadFile' >> beam.Map(lambda input: (beam.io.ReadAllFromText(input)))
      #| 'fdg' >> beam.io.ReadAllFromText(input)
      | 'parse' >> beam.ParDo(Split())
	  | 'sum' >> beam.CombinePerKey(sum)
	  | 'convertToString' >>beam.Map(lambda (combined_key, total_balance): '%s,%s,%s,%s' % (combined_key.split(",")[0], combined_key.split(",")[1],total_balance,combined_key.split(",")[2]))
      | 'write' >> beam.io.WriteToText(output_prefix) 
   )

   p.run().wait_until_finish()
