# virgin_media_test

This project aims to demonstrate the use of Apache - beam
Using python sdk. 

<h3> Input </h3>

The input data is taken from gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv
It is written in varible input_file in transactions.py to
make it easy to run the pipeline directly from IDE instead of 
Providing commandline arguments since that was the dataset which was 
Supposed to be considered.

<h3> Transforms </h3>

All transforms are performed using a single composite transform.

<h3> Output </h3>


The output is generated in two formats. One is jsonl.gz and other is csv format 
for increased readability.

The output path is ./output/results.csv


<h3> Unit Test </h3>

Unit tests are written in tests_transactions.py The whole composite transform is tested.


<h3> Running the pipeline </h3>

Steps:

1. Navigate to src folder
2. Run unittest: `python -m unittest discover`
3. Run transactions.py from IDE or enter command: `python transactions.py`
4. Run both together: `python -m unittest discover && python transactions.py`
5. Check output in output folder in project directory. (results.csv)
