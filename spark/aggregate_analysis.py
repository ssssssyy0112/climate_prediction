# This file use sparkml to do some aggregate computation.
# The core is about `pyspark.sql.functions`, which provided
# max, min, avg etc., and this file use avg to do later statistics.

from pyspark.sql import SparkSession,Row
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from elasticsearch import Elasticsearch,helpers
from datetime import datetime,timedelta

spark = SparkSession.builder.appName("aggregate").getOrCreate()
es = Elasticsearch('http://localhost:9200')
schema = StructType([
    StructField("STAID", IntegerType(), True),
    StructField("region_code", StringType(), False),
    StructField("TG", IntegerType(), False),    # avg
    StructField("TX", IntegerType(), False),     # high
    StructField("TN", IntegerType(), False),     # low
    StructField("RR", IntegerType(), True),     # rain
    StructField("FG", IntegerType(), True),     # avg wind speed
    StructField("SS", IntegerType(), True),     # sunshine
])



dates = [19901001]

for date in dates:
    df = spark.createDataFrame([],schema)
    for staid in range(1,26374,100):
        query = {
            "query": {
                "bool":{
                    "must":[
                        {
                        "terms":{
                            "DATE": [date]
                        }},
                        {
                            "terms":{
                                "STAID":[i for i in range(staid,staid+100)]
                            }
                        }
                    ]
                }
            },
        }
        result = es.search(index="weathers",body=query)
        for hit in result["hits"]["hits"]:
            source = hit["_source"]
            row = Row(STAID=int(source['STAID']), region_code = str(source['region_code']), TG=int(source['TG']), TX=int(source['TX']), TN=int(source['TN']), 
                      RR=None if source['RR'] is None else int(source['RR']),
                      FG=None if source['FG'] is None else int(source['FG']), 
                      SS=None if source['SS'] is None else int(source['SS']),)
            df = df.union(spark.createDataFrame([row], schema))

df_grouped = df.groupBy("region_code").agg(
    F.avg("TX").alias("avg_high"),
    F.avg("TG").alias("avg_mean"),
    F.avg("TN").alias("avg_low"),
    F.avg("RR").alias("avg_rain"),
    F.avg("SS").alias("avg_sunshine"),
    F.avg("FG").alias("avg_speed")
)

df_grouped.write.format('csv').option('header',True).mode('overwrite').save('intermediate_data/aggregate_19901001.csv')