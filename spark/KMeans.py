# This file use sparkml clustering KMeans, and do some KMeans computation
# It uses temperature(TG/TX/TN), rain(RR), humidity(HU) and sunshine(SS)
# as features, trying to divide data into 4 clusters.
# Also, the wind speed(FG) can be added into the features.
# We hope to classify the weather into "sunning" "windy" "rainy" "cloudy" "cold" etc.
 
from pyspark.sql import SparkSession,Row
from pyspark.sql.types import StructType,StructField,IntegerType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from elasticsearch import Elasticsearch,helpers
from datetime import datetime,timedelta

spark = SparkSession.builder.appName("Kmeans").getOrCreate()
es = Elasticsearch('http://localhost:9200')
schema = StructType([
    StructField("STAID", IntegerType(), True),
    StructField("TG", IntegerType(), True),
    StructField("TX", IntegerType(), True),
    StructField("TN", IntegerType(), True),
    StructField("RR", IntegerType(), True),
    StructField("HU", IntegerType(), True),
    StructField("SS", IntegerType(), True),
    # StructField("FG", IntegerType(), True)
])

start_date = datetime(1990,1,1)
end_date = datetime(1990,12,31)
dates=[]
current_date = start_date
while current_date <= end_date:
    dates.append(current_date.strftime("%Y%m%d"))
    current_date+=timedelta(days=1)

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
            if source["RR"]==None or source["HU"]==None or source["SS"]==None:
                continue
            row = Row(STAID=int(source['STAID']), TG=int(source['TG']), TX=int(source['TX']), TN=int(source['TN']), RR=int(source['RR']), HU=int(source['HU']), SS=int(source['SS']))
            df = df.union(spark.createDataFrame([row], schema))
    features = ["TG","TX","TN","RR","HU","SS"]
    vec_assmbler = VectorAssembler(inputCols=features,outputCol="features")
    df_kmeans = vec_assmbler.transform(df).select("features")
    kmeans = KMeans(k=4,seed=1)
    model = kmeans.fit(df_kmeans.select("features"))

    centers = model.clusterCenters()
    f = open("intermediate_data/cluster_"+str(date),"w+",encoding="utf-8")
    f.write("Cluster centers, Date:"+str(date)+"\n")
    f.write("TG\t\tTX\t\tTN\t\tRR\t\tHU\t\tSS\t\t\n")
    for center in centers:
        f.write(str(center))
        f.write("\n")
    f.close()