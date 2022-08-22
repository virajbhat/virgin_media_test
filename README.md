# virgin_media_test

This project aims to demonstrate the use of Apache - beam
Using python sdk. 

Input:

The input data is taken from 
It is written in varible input_file in transactions.py to
make it easy to run the pipeline directly from IDE instead of 
Providing commandline arguments since that was the dataset which was 
Supposed to be considered.

Transforms:

All transforms are performed using a single composite transform.

Output:

The output is generated in two formats. One is jsonl.gz and other is csv format 
for increased readability.

The output path is ./output/results.csv

