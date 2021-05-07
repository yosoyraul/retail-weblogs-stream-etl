from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,split,concat,length,lit
from pyspark import SparkContext




def main():
    spark = SparkSession. \
        builder. \
        appName("spark-handler"). \
        getOrCreate() \
        
    spark.sparkContext.setLogLevel("Error")

    lines = spark. \
        readStream. \
        format("kafka"). \
        option("kafka.bootstrap.servers","localhost:9092"). \
        option("subscribe","test"). \
        load(). \
        selectExpr("CAST(value AS STRING)")


    col = lines.select(split(lines.value,' (?=(?:[^"]|"[^"]*")*$)').alias('col'))
    cols = col.select(
        concat(col.col[3],lit(' '),col.col[4]).alias('timestamp'),
        col.col[0].alias('ip'),
        col.col[5].alias('request'),
        col.col[6].alias('status'),
        col.col[7].alias('resp_size'),
        col.col[9].alias('ua')
    )
    query = cols.writeStream.outputMode('update').format('console').start()

    query.awaitTermination()


if __name__=="__main__":
    main()

 