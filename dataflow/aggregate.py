#I wrote this code to answer stackoverflow question https://stackoverflow.com/questions/53261781/how-to-aggregate-data-using-apache-beam-api-with-multiple-keys/53333866#53333866
#aggregates by key and writes into a gcs bucket
'''
after runing the command "python aggregate.py", I got the error "ImportError: No module named apache_beam"
then ran the command "pip2 install --upgrade apache_beam google-cloud-dataflow"
then I got the error "google-cloud-dataflow 2.5.0 has requirement apache-beam[gcp]==2.5.0, but you'll have apache-beam 2.8.0 which is incompatible"
Here I have used CombinePerKey as per Lak, it could be done by using GroupByKey too. But combinePerKey is faster.

sudo apt-get install python-pip
sudo pip install google-cloud-dataflow oauth2client==3.0.0 
sudo pip install --force six==1.10  # downgrade as 1.11 breaks apitools
sudo pip install -U pip

But still error

mkdir myvirtualpython
cd myvirtualpython
virtualenv env --python=/usr/bin/python
which python
source env/bin/activate
pip install apache-beam[gcp]


python ./aggregate.py \
	--project=august-button-203520 \
	--job_name=myjob \
	--staging_location= \
	--temp_location= \
	--runner=DirectRunner


-- to remove virtual env: sudo rm -rf venv	
'''

''' how I made it work in PowerShell
https://beam.apache.org/get-started/quickstart-py/

'''

'''
windows
python -m apache_beam.examples.wordcount --input /path/to/inputfile --output /path/to/write/counts
'''

import apache_beam as beam
import sys

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
   input = 'aggregate.csv'
   output_prefix = 'C:\\pythonVirtual\\Mycodes\\output'
   
   (p
      | 'ReadFile' >> beam.io.ReadFromText(input)
      | 'parse' >> beam.ParDo(Split())
	  | 'sum' >> beam.CombinePerKey(sum)
	  | 'convertToString' >>beam.Map(lambda (combined_key, total_balance): '%s,%s,%s,%s' % (combined_key.split(",")[0], combined_key.split(",")[1],total_balance,combined_key.split(",")[2]))
      | 'write' >> beam.io.WriteToText(output_prefix)
   )

   p.run().wait_until_finish()
