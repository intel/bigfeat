#!/usr/bin/env python3

import subprocess
import json
import pandas as pd
import os
import glob
import re
import fire
import csv

def parse(file_name):
  with open(file_name) as f:
    data = f.readlines()

  # Split and parse system results
  metrics = {'query':file_name}
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
      if len(metric) == 2:
        raise Exception('List {} size != 2'.format(metric))
      indv_metric = metric.split(" ", 1)
      val, key = [string.strip() for string in indv_metric]
      metrics[key] = val

  return metrics


def main(path = '/tmp', pattern = '*.log', csv_file = 'perf_results.csv'):
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

