#!/usr/bin/env python3

import fire
import subprocess
import glob
import os
import re

"""
# TODO: Move prepare.sh script to Python
# TODO: Minio rb functionality is not working right now, figure out what is going on there.
"""

"""
Given a query_file run the query on the presto docker container and profile it 
using perf. The ouput gets stored in output_path.
"""
def run_and_profile_query(query_path, 
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

    with open(output_path+".out", "w") as out, open(output_path+".log", "w") as err:
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
Run and profile queries with scale_factor and store the output in output_dir
"""

# TODO Set both drop and clean flags to default value
def run(scale_factor=1,
        output_dir="/tmp/sf1/",
        query_range=range(1,2),
        drop=True,
        clean_output_dir=True):

    # Ask the user before continuing
    print("*******************")
    print("scale factor: " + str(scale_factor))
    print("Output_directory: " + output_dir)
    print("query_range: " + str(query_range))
    print("drop: " + str(drop))
    print("clean_output_dir: " + str(clean_output_dir))
    print("*******************")

    if input("continue? (y/n)") != "y":
        exit()


    # Check that the ouput directory exists
    if not os.path.isdir(output_dir):
        print("output directory does not exist, or the path is incorrect")
        exit()
    
    # Clean the output directory
    if clean_output_dir:
        command = "rm -rf "+output_dir+"*"
        subprocess.run(command.split(" "))    
    
    # Drop the tables
    if drop:
        drop_query = "drop_tables.sql"
        run_and_profile_query("./tpcds/sf" + str(scale_factor) + "/"+ drop_query, 
                                  output_dir+drop_query.split('.')[0])

        command = "docker exec minio rm -rf data/tpcds-sf"+str(scale_factor)+"-partitioned-dsdgen-parquet"
        print(command)
        proc = subprocess.run(command.split(" "))

    # Run prepare to set up bucket and schema for the provided scale factor
    queries = prepare(scale_factor)

    
    # Run create_tables query
    create_query = "create_tables.sql"
    run_and_profile_query("./tpcds/sf" + str(scale_factor) + "/"+ create_query, 
                                  output_dir+create_query.split('.')[0])

    # Run queries
    for query in queries:
        if int(re.findall(r'\d+', query)[-1]) in query_range:
            return_code = run_and_profile_query("./tpcds/sf" + str(scale_factor) + "/"+ query, 
                                  output_dir+query.split('.')[0])

    print("*******************")
    print("scale factor: " + str(scale_factor))
    print("Output_directory: " + output_dir)
    print("query_range: " + str(query_range))
    print("*******************")

   
if __name__ == '__main__':
  fire.Fire(run)