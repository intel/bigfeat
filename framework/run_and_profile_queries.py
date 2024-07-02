#!/usr/bin/env python3

import fire
import subprocess
import glob
import os
import re
import shutil

"""
# TODO: Move prepare.sh script to Python
# TODO: Minio rb functionality is not working right now, figure out what is going on there.
"""

"""
Given a query_file run the query on the presto docker container and profile it 
using perf. The ouput gets stored in output_path.
"""
def run_and_profile_query( query_path,
                           output_path,
                           timeout_in_secs=12000,
                           presto_container="coordinator"):

    perf_command = "perf stat -d" 
    docker_command = "docker exec -it " + presto_container
    presto_command = "presto-cli --file"

    perf_command = perf_command.split(" ")
    docker_command = docker_command.split(" ")
    presto_command = presto_command.split(" ")

    command = perf_command + docker_command + presto_command + [query_path]

    print("Gathering stats for ", query_path)

    with open(output_path + ".out", "w") as out, open(output_path+".log", "w") as err:
        proc = subprocess.run(command, 
                              encoding='utf-8', 
                              stderr=err, 
                              stdout=out, 
                              timeout=timeout_in_secs)

    if proc.returncode != 0:
        return 1

    return 0

"""
Use the prepare script to generat queries for the given scale_factor
Returns the list of queries generated
"""
def prepare(scale_factor):
    proc = subprocess.run(["./prepare.sh", str(scale_factor)])
    queries = [os.path.basename(x) for x in glob.glob('./generated/q*.sql')]
    queries.sort()
    return queries

"""
Remove files with a specified pattern from a specified path
"""
def delete_files(path, pattern = '*'):
    for i in glob.glob(os.path.join(path, pattern)):
        if os.path.isdir(i):
            shutil.rmtree(i)
        else:
            os.remove(i)

"""
Run and profile queries with scale_factor and store the output in output_dir
"""
def run( scale_factor = 1,
         output_dir_pattern = '/tmp/sf',
         query_pattern = 'bf*.sql',
         reload_db = True,
         clean_output_dir = True ):

    # Ask the user before continuing
    print("****************************")
    print("scale_factor:       " + str(scale_factor))
    print("output_dir_pattern: " + output_dir_pattern)
    print("query_pattern:        " + query_pattern)
    # print("reload_db:          " + str(query_pattern))
    print("clean_output_dir:   " + str(clean_output_dir))
    print("****************************")

    if input("continue? (y/n)") != "y":
        exit()

    # Create the ouput directory if it does not exist
    output_dir = output_dir_pattern + str(scale_factor)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Clean the output directory
    if clean_output_dir:
        # Purge all output files
        delete_files(output_dir, '*.out')

        # Purge all perf stat files
        delete_files(output_dir, '*.log')

    # WASAY: I am commenting this out for now as the pipeline is not robust to handle this correctly

    # # Run drop_tables query
    # if reload_db:
    #     # Drop the tables
    #     drop_query = "drop_tables.sql"
    #     run_and_profile_query("./framework/sf" + str(scale_factor) + "/" + drop_query, 
    #                               output_dir + "/" + drop_query.split('.')[0])

    #     command = "docker exec minio rm -rf data/tpcds-sf" + str(scale_factor) + \
    #                               "-partitioned-dsdgen-parquet"
    #     print(command)
    #     proc = subprocess.run(command.split(" "))

    # # Run prepare to set up bucket and schema for the provided scale factor
    # queries = prepare(scale_factor)

    # # Run create_tables query
    # if reload_db:
    #     create_query = "create_tables.sql"
    #     run_and_profile_query("./framework/sf" + str(scale_factor) + "/" + create_query, \
    #                               output_dir + "/" + create_query.split('.')[0])

    # Run queries
    
    query_files = glob.glob("./feature_engineering/queries/"+query_pattern)
    print(query_files)

    for query_file in query_files:
        
        query_file = query_file.split('/')[-1]
        
        return_code = run_and_profile_query("./framework/sf" + str(scale_factor) + "/" + query_file, output_dir + "/" +query_file.replace('.sql',''))

    # print("*******************")
    # print("scale_factor: " + str(scale_factor))
    # print("output_dir:   " + output_dir)
    # print("query_range:  " + query_range)
    # print("*******************")


if __name__ == '__main__':
    fire.Fire(run)

