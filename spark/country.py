# This file add region_code and altitude of stations into data.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from elasticsearch import Elasticsearch,helpers
from elasticsearch.helpers import bulk

# SparkSession
spark = SparkSession.builder.appName("txtToES").getOrCreate()

# files
file_paths = ["station_positions/cc_stations.txt","station_positions/dd_stations.txt","station_positions/fg_stations.txt","station_positions/fx_stations.txt",
              "station_positions/hu_stations.txt","station_positions/pp_stations.txt","station_positions/qq_stations.txt","station_positions/rr_stations.txt",
              "station_positions/sd_stations.txt","station_positions/ss_stations.txt","station_positions/tg_stations.txt","station_positions/tn_stations.txt","station_positions/tx_stations.txt"]

df = spark.read.text(file_paths)

# schema
schema = "id INT, station STRING, region_code STRING, latitude STRING, longitude STRING, altitude INT"

df = df.selectExpr(f"split(value, ',') as row").selectExpr(f"row[0] as id", "row[1] as station", "row[2] as region_code", "row[3] as latitude", "row[4] as longitude", "row[5] as altitude")

df = df.dropDuplicates()


data = [row.asDict() for row in df.collect()]


    


# es = Elasticsearch(
#         ['http://es-cn-x0r3hvdmx000ka9ud.public.elasticsearch.aliyuncs.com:9200'],
#         basic_auth=('elastic', 'ZhouWuShangWuDaShuJuXiaoZu4')
#     )

es = Elasticsearch(
    ["http://localhost:9200"]
)


# def gen_data(data):
#     for item in data:
#         yield {
#             "_index": "station-location",
#             "_source": item
#         }


# def write_to_es(data):
#     try:
#         helpers.bulk(es, gen_data(data))
#     except helpers.BulkIndexError as e:
#         for i, error in enumerate(e.errors):
#             print(f"Document {i} failed with error: {error}")

# write_to_es(data)


cnt = 0
for item in data:
    cnt+=1
    print(cnt)
    staid = int(item['id'].strip())
    cc = item['region_code'].strip()
    altitude = int(item['altitude'])
    body = {
    "script": {
        "source": "ctx._source.region_code = params.region_code; ctx._source.altitude = params.altitude",
        "params": {
            "region_code":cc,
            "altitude":altitude
        }
    },
    "query": {
        "term": {
            "STAID": staid
            }
        }
    }
    res = es.update_by_query(index='weathers', body=body)
    
