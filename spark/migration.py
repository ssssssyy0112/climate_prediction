# This file is a helper function which can migrate huge data from 
# one es to another. Because the size is so large, we use `bulk`
# to speed up the migration.
# `es1` is the source elasticsearch, and `es2` is the destination, 
# so you can replace it with your own es url.

from elasticsearch import Elasticsearch, helpers

def reindex(es1, es2, index, staid):
    print(staid)
    result = es1.search(
        body={"query": {"term": {"STAID": staid}},"size":3700},
        index=index
    )

    actions = [
        {
            "_index": index,
            "_id": hit['_id'],
            "_source": hit['_source']
        }
        for hit in result['hits']['hits']
    ]

    helpers.bulk(es2, actions)


es1 = Elasticsearch(
        ['http://es-cn-x0r3hvdmx000ka9ud.public.elasticsearch.aliyuncs.com:9200'],
        basic_auth=('elastic', 'ZhouWuShangWuDaShuJuXiaoZu4')
    )
es2 = Elasticsearch('http://localhost:9200')

for staid in range(1, 26374):
    reindex(es1, es2, 'weathers', staid)
