# This file add geo_point of each station into data, for the purpose of
# deploying heat map in kibana.
# It's similar with `country.py`, but writing different fields.

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


def parse_lat_and_lon(val:str):
    neg=False
    if "-" in val:
        neg=True
    degree,minute,second = map(int,val.strip().strip("+").split(":"))
    dec = degree+minute/60+second/3600
    if dec>180:
        dec-=360
    if neg:
        dec = -dec
    return dec
    


es = Elasticsearch(
        ['http://es-cn-x0r3hvdmx000ka9ud.public.elasticsearch.aliyuncs.com:9200'],
        basic_auth=('elastic', 'ZhouWuShangWuDaShuJuXiaoZu4')
    )

# es = Elasticsearch(
#     ["http://localhost:9200"]
# )


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
    latitude = parse_lat_and_lon(item['latitude'])
    longitude = parse_lat_and_lon(item['longitude'])


    body = {
    "script": {
        "source": "if (ctx._source.geo == null) { ctx._source.geo = new HashMap(); } ctx._source.geo.coordinates = params.coordinates",
        "params": {
            "coordinates": {
                "lat": latitude,
                "lon": longitude
            }
        }
    },
    "query": {
        "term": {
            "STAID": staid
            }
        }
    }
    res = es.update_by_query(index='weathers', body=body)
    
