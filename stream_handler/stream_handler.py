from pyspark.sql import SparkSession
from pyspark import SparkContext
import json
import importlib
import argparse


def _parse_arguments():
    """ Parse arguments provided by spark-submit commend"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", required=True)
    return parser.parse_args()

def main():

    args = _parse_arguments()

    with open("config.json","r") as config_file:
        config = json.load(config_file)

    spark = SparkSession. \
        builder. \
        appName(config.get("app_name")). \
        getOrCreate() \
        
    spark.sparkContext.setLogLevel("Error")

    job_module = importlib.import_module(f"jobs.{args.job}")
    job_module.run_job(spark,config)


if __name__=="__main__":
    main()

 