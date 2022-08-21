import apache_beam as beam
from apache_beam.io import ReadFromText
import datetime
import os


input_file = 'gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv'
#input_file = 'C:\\virgin_media_test\\virgin_media_test\\input\\transactions.csv'
output_prefix = os. getcwd() + '\\output\\results.csv'


class Split(beam.DoFn):
    def process(self, element):
        timestamp,origin,destination,transaction_amount = element.split(',')
        return [{
            'date': timestamp,
            'transaction_amount': float(transaction_amount),
        }]

class filter_amt(beam.DoFn):
    
    def process(self, element):
        results = []
        if element['transaction_amount'] > 20:
            results.append(element)
        return results

class filter_year(beam.DoFn):
    
    def process(self, element):
        results = []
        bool_val = (datetime.datetime.strptime(element['date'] , '%Y-%m-%d %H:%M:%S %Z').year > 2010)
        if bool_val:
            results.append(element)
        return results

class get_tuple(beam.DoFn):
    
    def process(self, element):
        results = []
        keyval= list((element['date'], element['transaction_amount']))
        results.append(keyval)
        return results
      
class join_list(beam.DoFn):

    def process(self, element):
        temp = []
        join_list = list(element)
        temp.append(join_list)
        #print(amount)
        #print(','.join(join_list))
        
        return temp





class compute_transforms_tranctions(beam.PTransform):
  def expand(self, pcollect):
    # Transform logic goes here.
    return (
        pcollect 
    | 'split data' >> beam.ParDo(Split())
    | 'filter amount greater than 20' >> beam.ParDo(filter_amt())
    | 'filter years later than 2010' >> beam.ParDo(filter_year())
    | 'convert data from grouping' >> beam.ParDo(get_tuple())
    | 'sum ammount for each year' >> beam.CombinePerKey(sum)
    | 'join final output for writing to csv' >> beam.ParDo(join_list())
    )


class write_list(beam.DoFn):

    def setup(self):
        exist = os.path.isfile(output_prefix)
        if exist:
            os.remove(output_prefix)

    def process(self,element):
        result_list = [str(element[0]),str(element[1])]
        result_string = ','.join(result_list)
        exist = os.path.isfile(output_prefix)
        if exist:
            mode = 'a'
        else:
            with open(output_prefix, 'w') as f:
                f.write('date, Total_amount' + '\n')
                mode = 'a'
        with open(output_prefix, mode) as f:
            f.write(result_string + '\n')

        #print(element)



p = beam.Pipeline()
transactions_data = p | ReadFromText(input_file, skip_header_lines=1)
results = transactions_data | compute_transforms_tranctions()

output = results | 'out ' >> beam.io.WriteToText(file_path_prefix=output_prefix, file_name_suffix='.jsonl.gz', header=['date', 'Total_amount'])
output1 = results | 'out1' >> beam.ParDo(write_list())
#output1 = output | beam.Map(_to_dictionary)

print(output)
p.run()