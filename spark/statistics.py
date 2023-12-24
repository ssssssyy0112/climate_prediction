# This file uses sparkml funcions to do statistics work
# max, min, avg, var and None_count are used.

from pyspark.sql import SparkSession,Row
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField,IntegerType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from elasticsearch import Elasticsearch,helpers
from datetime import datetime,timedelta

spark = SparkSession.builder.appName("statistics").getOrCreate()
es = Elasticsearch('http://localhost:9200')

schema = StructType([
    StructField("STAID", IntegerType(), True),
    StructField("TG", IntegerType(), True),
    StructField("TX", IntegerType(), True),
    StructField("TN", IntegerType(), True),
    StructField("RR", IntegerType(), True),
    StructField("HU", IntegerType(), True),
    StructField("SS", IntegerType(), True),
    StructField("FG", IntegerType(), True),
    StructField("PP", IntegerType(), True),
    StructField("CC", IntegerType(), True),
    StructField("SD", IntegerType(), True),
    StructField("QQ", IntegerType(), True),
    StructField("FX", IntegerType(), True),
    StructField("DD", IntegerType(), True),
])

dates = [19900101]

dic = {}

for date in dates:
    df = spark.createDataFrame([],schema=schema)
    for staid in range(1,26374,1000):
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
                                "STAID":[i for i in range(staid,staid+1000)]
                            }
                        }
                    ]
                }
            },
            "size":1000
        }
        result = es.search(index="weathers",body=query)
        for hit in result["hits"]["hits"]:
            source = hit["_source"]
            row = Row(STAID=int(source['STAID']), TG=int(source['TG']),
                       TX=int(source['TX']), TN=int(source['TN']), 
                       RR=None if source['RR'] is None else int(source['RR']),
                       PP=None if source['PP'] is None else int(source['PP']),
                       CC=None if source['CC'] is None else int(source['CC']),
                       HU=None if source['HU'] is None else int(source['HU']),
                       SD=None if source['SD'] is None else int(source['SD']),
                       SS=None if source['SS'] is None else int(source['SS']),
                       QQ=None if source['QQ'] is None else int(source['QQ']),
                       FG=None if source['FG'] is None else int(source['FG']),
                       FX=None if source['FX'] is None else int(source['FX']),
                       DD=None if source['DD'] is None else int(source['DD']),
                       )
            df = df.union(spark.createDataFrame([row],schema))
    print(1)
    tp_dict = {}
    tp_dict["Total"] = df.count()
    result = df.select(F.max(df.TG),F.min(df.TG)).collect()
    tp_dict["TG"] = [result[0][i] for i in range(2)]
    result = df.select(F.max(df.TX),F.min(df.TX),F.avg(df.TX),F.var_pop(df.TX),F.count(F.when(F.col("TX").isNull(),1))).collect()
    tp_dict["TX"] = [result[0][i] for i in range(5)]
    result = df.select(F.max(df.TN),F.min(df.TN),F.avg(df.TN),F.var_pop(df.TN),F.count(F.when(F.col("TN").isNull(),1))).collect()
    tp_dict["TN"] = [result[0][i] for i in range(5)]
    result = df.select(F.max(df.RR),F.min(df.RR),F.avg(df.RR),F.var_pop(df.RR),F.count(F.when(F.col("RR").isNull(),1))).collect()
    tp_dict["RR"] = [result[0][i] for i in range(5)]
    result = df.select(F.max(df.PP),F.min(df.PP),F.avg(df.PP),F.var_pop(df.PP),F.count(F.when(F.col("PP").isNull(),1))).collect()
    tp_dict["PP"] = [result[0][i] for i in range(5)]
    result = df.select(F.max(df.CC),F.min(df.CC),F.avg(df.CC),F.var_pop(df.CC),F.count(F.when(F.col("CC").isNull(),1))).collect()
    tp_dict["CC"] = [result[0][i] for i in range(5)]
    result = df.select(F.max(df.HU),F.min(df.HU),F.avg(df.HU),F.var_pop(df.HU),F.count(F.when(F.col("HU").isNull(),1))).collect()
    tp_dict["HU"] = [result[0][i] for i in range(5)]
    result = df.select(F.max(df.SD),F.min(df.SD),F.avg(df.SD),F.var_pop(df.SD),F.count(F.when(F.col("SD").isNull(),1))).collect()
    tp_dict["SD"] = [result[0][i] for i in range(5)]
    result = df.select(F.max(df.SS),F.min(df.SS),F.avg(df.SS),F.var_pop(df.SS),F.count(F.when(F.col("SS").isNull(),1))).collect()
    tp_dict["SS"] = [result[0][i] for i in range(5)]
    result = df.select(F.max(df.QQ),F.min(df.QQ),F.avg(df.QQ),F.var_pop(df.QQ),F.count(F.when(F.col("QQ").isNull(),1))).collect()
    tp_dict["QQ"] = [result[0][i] for i in range(5)]
    result = df.select(F.max(df.FG),F.min(df.FG),F.avg(df.FG),F.var_pop(df.FG),F.count(F.when(F.col("FG").isNull(),1))).collect()
    tp_dict["FG"] = [result[0][i] for i in range(5)]
    result = df.select(F.max(df.FX),F.min(df.FX),F.avg(df.FX),F.var_pop(df.FX),F.count(F.when(F.col("FX").isNull(),1))).collect()
    tp_dict["FX"] = [result[0][i] for i in range(5)]
    result = df.select(F.max(df.DD),F.min(df.DD),F.avg(df.DD),F.var_pop(df.DD),F.count(F.when(F.col("DD").isNull(),1))).collect()
    tp_dict["DD"] = [result[0][i] for i in range(5)]
    dic[date]=tp_dict

f = open("intermediate_data/statistics_1","w+",encoding="utf-8")
f.write(str(dic))
f.close()