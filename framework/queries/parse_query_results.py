#!/usr/bin/env python3

import subprocess
import json
import pandas as pd
import os
import glob
import re
import fire
import csv
import ntpath

def query_name(path):
    head, tail = ntpath.split(path)
    return ntpath.splitext(tail)[0] or ntpath.basename(head)


def parse(file_name):

    with open(file_name) as f:
        data = f.readlines()

    # Split and parse system results
    metrics = {'query' : query_name(file_name)}
    seen = False

    for line in data:

        if line in ['\n', '\r\n']:
            continue

        elif "Performance counter stats for" in line:
            seen = True
            continue

        elif seen == False:
            continue

        # Split the string by #
        metricsList = line.split("#")
        metricsList = [string.strip() for string in metricsList]

        for metric in metricsList:
            indv_metric = metric.split(" ", 1)
            val, key = [string.strip() for string in indv_metric]
            metrics[key] = val

    return metrics


def main( path = '/tmp/sf1',
          pattern = '*.log',
          csv_file = '/tmp/sf1/perf_results.csv' ):

    # Ask the user before continuing
    print("****************************")
    print("path:     " + path)
    print("pattern:  " + pattern)
    print("csv_file: " + csv_file)
    print("****************************")

    if input("continue? (y/n)") != "y":
        exit()

    with open(csv_file, 'w', newline='') as csvfile:

        writer = csv.writer(csvfile)
        header = True

        for f in glob.glob(path + '/' + pattern):

            print("Parse {}".format(f))
            metrics = parse(f)

            if header == True:
                keys = list(metrics.keys())
                writer.writerow(keys)
                header = False

            values = list(metrics.values())
            writer.writerow(values)


if __name__ == "__main__":
    fire.Fire(main)

