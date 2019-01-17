#I wrote this code to answer stackoverflow question https://stackoverflow.com/questions/53808388/how-to-filter-and-get-folder-with-latest-date-in-google-dataflow
#aggregates by key and writes into a gcs bucket


import apache_beam as beam
import sys
import datetime

class Split(beam.DoFn):

    def process(self, element):
        """
        Splits each row on commas and returns a tuple representing the row to process
        """
        customer_id, customer_name, transction_amount, transaction_type = element.split(",")
        return [
            (customer_id +","+customer_name+","+transaction_type, float(transction_amount))
        ]

if __name__ == '__main__':
   p = beam.Pipeline(argv=sys.argv)
   print 
   input = 'C:\\myGit\\gcp-data-engineering\dataflow\\data-' + datetime.datetime.today().strftime('%Y-%m-%d') + '*\*'
   output_prefix = 'C:\\myGit\\gcp-data-engineering\output'
   
   (p
      | 'ReadFile' >> beam.io.ReadFromText(input)
      | 'parse' >> beam.ParDo(Split())
	  | 'sum' >> beam.CombinePerKey(sum)
	  | 'convertToString' >>beam.Map(lambda (combined_key, total_balance): '%s,%s,%s,%s' % (combined_key.split(",")[0], combined_key.split(",")[1],total_balance,combined_key.split(",")[2]))
      | 'write' >> beam.io.WriteToText(output_prefix)
   )

   p.run().wait_until_finish()
