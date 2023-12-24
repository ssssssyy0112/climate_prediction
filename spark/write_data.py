# This file reads data from remote FileSystem, using RESTful api
# Then, it uses spark to read/rename/filter and aggregate data into pieces, 
# and write it into elasticsearch.

from pyspark import SparkContext, SparkConf
import urllib.request
from io import StringIO
import pandas as pd
from urllib.error import HTTPError
from elasticsearch import Elasticsearch, helpers
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col

es = Elasticsearch(
        ['http://es-cn-x0r3hvdmx000ka9ud.public.elasticsearch.aliyuncs.com:9200'],
        basic_auth=('elastic', 'ZhouWuShangWuDaShuJuXiaoZu4')
    )

def gen_data(weather_data):
    for item in weather_data:
        yield {
            "_index": "weather_1980-1989",  # different index for different decade
            "_source": item
        }


def write_to_es(data):
    try:
        helpers.bulk(es, gen_data(data))
    except helpers.BulkIndexError as e:
        for i, error in enumerate(e.errors):
            print(f"Document {i} failed with error: {error}")
    

def main():
    conf = SparkConf().setAppName("write_data")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc).builder.getOrCreate()
    prefixes = ["CC","DD","FG","FX","HU","PP","QQ","RR","SD","SS","TG","TN","TX"]

    for i in range(1, 10, 10):
        df = spark.createDataFrame([], schema="STAID INT, DATE INT, TX INT, TG INT, TN INT, CC INT, DD INT, FG INT, FX INT, HU INT, PP INT, QQ INT, RR INT, SS INT, SD INT")        
        for j in range(i, i + 10):
            for prefix in prefixes:
                idx = prefix+"_STAID"+str(j).zfill(6)+".txt"
                url = "http://47.95.13.142:8888/"+idx   # this is the seaweedfs's url
                try:
                    response = urllib.request.urlopen(url)
                except HTTPError:
                    print(f"{idx} 文件不存在")
                    continue
                
                tp_data = response.read().decode("utf-8")
                pdf = pd.read_csv(StringIO(tp_data),header=None)
                print(pdf)
                temp_df = spark.createDataFrame(pdf)
                print(temp_df.columns)
                temp_df = temp_df.withColumnRenamed("0", "STAID")\
                .withColumnRenamed("2", "DATE")\
                .withColumnRenamed("3", prefix)\
                .withColumnRenamed("4", "VALID")\
                .drop("1")
                # only validated data are saved
                temp_df = temp_df.filter(col("VALID") == 0)
                temp_df = temp_df.filter(temp_df['DATE'].between(19800101, 19891231))
                temp_df = temp_df.drop("VALID")

                df = df.join(temp_df,on=['STAID','DATE',prefix],how="outer")
        
        df = df.groupBy("STAID", "DATE")\
           .agg(F.first("TN", ignorenulls=True).alias("TN"),
                F.first("TG", ignorenulls=True).alias("TG"),
                F.first("TX", ignorenulls=True).alias("TX"),
                F.first("CC", ignorenulls=True).alias("CC"),
                F.first("DD", ignorenulls=True).alias("DD"),
                F.first("FG", ignorenulls=True).alias("FG"),
                F.first("FX", ignorenulls=True).alias("FX"),
                F.first("HU", ignorenulls=True).alias("HU"),
                F.first("PP", ignorenulls=True).alias("PP"),
                F.first("QQ", ignorenulls=True).alias("QQ"),
                F.first("RR", ignorenulls=True).alias("RR"),
                F.first("SD", ignorenulls=True).alias("SD"),
                F.first("SS", ignorenulls=True).alias("SS"))
    
        # only containing all tx、tn、tg's data are saved 
        df = df.filter((df['TX'].isNotNull()) & (df['TG'].isNotNull()) & (df['TN'].isNotNull()))  
        # df.show(5)

        weather_data = [row.asDict() for row in df.collect()]
        if len(weather_data)==0:
            print("empty")
            continue 
        
        write_to_es(weather_data)    
       

if __name__ == "__main__":
    main()
