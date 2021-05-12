from pyspark.sql.functions import explode,split,concat,length,lit
from shared.udfs import uuidDf

def _extract_data(spark,config):
        return spark. \
        readStream. \
        format(config.get("source")). \
        option(config.get("source_servers"),config.get("source_host")). \
        option(config.get("source_option"),config.get("topic")). \
        load(). \
        selectExpr("CAST(value AS STRING)")

def _transform_data(raw_df):
    df = raw_df.select(split(raw_df.value,' (?=(?:[^"]|"[^"]*")*$)').alias('col'))
    new_df =  df.select(
        concat(df.col[3],lit(' '),df.col[4]).alias('timestamp'),
        df.col[0].alias('ip'),
        df.col[5].alias('request'),
        df.col[6].alias('status'),
        df.col[7].alias('resp_size'),
        df.col[9].alias('ua')
    )
    return new_df.withColumn("uuid",uuidDf())

def _load_data(config,transformedDf):
    query = transformedDf.writeStream. \
        trigger(processingTime="5 seconds"). \
        outputMode('append'). \
        format(config.get("target")). \
        options(table=config.get("table"),keyspace=config.get("keyspace")). \
        option("checkpointLocation",config.get("checkpointLocation")). \
        start()
    query.awaitTermination()

def run_job(spark,config):
    _load_data(config, _transform_data(_extract_data(spark,config)))