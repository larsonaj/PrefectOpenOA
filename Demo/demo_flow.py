'''
Moves files from source folder to sink folder, to run this correctly, CD to the directory where the flow is located (./Demo/)
and execute python demo_flow.py
'''

import os
from os import walk
import csv
import datetime

from prefect import task, Flow, Parameter
from prefect.schedules import IntervalSchedule


@task(max_retries=1, retry_delay=datetime.timedelta(seconds=5))
def extract(path):
    print(path)
    with open(path, "r") as f:
        text = f.readline().strip()
    data = [int(i) for i in text.split(",")]
    print(data)
    return data

@task
def transform(data):
    tdata = [i+1 for i in data]
    return tdata

@task
def load(data, path):
    with open(path, "w" ) as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(data)
    return 


with Flow("my_etl") as flow:
    extract_path = Parameter(name="extract_path", required=True)
    load_path = Parameter(name="load_path", required=True)
    data = extract(extract_path)
    tdata = transform(data)
    load(tdata, load_path)
    