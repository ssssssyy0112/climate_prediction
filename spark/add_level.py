# This file add `level` for each climate metric.
# The temperature, sunshine duration and rain are generally 
# divided int 8 to 10 levels for later use.

from elasticsearch import Elasticsearch, helpers

def add_level_tx(es, staid):
    print(staid)
    body = {
        "query": {
            "term": {
            "STAID": {
                "value": staid
                }
            }
        },
        "script": {
            "source": """
            double x = Double.parseDouble(ctx._source.TX.trim()) / 10;
            if (x < -15) ctx._source.TX_LEVEL = 0;
            else if (x >= -15 && x < -10) ctx._source.TX_LEVEL = 1;
            else if (x >= -10 && x < -5) ctx._source.TX_LEVEL = 2;
            else if (x >= -5 && x < 0) ctx._source.TX_LEVEL = 3;
            else if (x >= 0 && x < 5) ctx._source.TX_LEVEL = 4;
            else if (x >= 5 && x < 10) ctx._source.TX_LEVEL = 5;
            else if (x >= 10 && x < 15) ctx._source.TX_LEVEL = 6;
            else if (x >= 15 && x < 20) ctx._source.TX_LEVEL = 7;
            else if (x >= 20 && x < 25) ctx._source.TX_LEVEL = 8;
            else if (x >= 25) ctx._source.TX_LEVEL = 9;
            """
        }
    }
    res = es.update_by_query(index='weathers', body=body)


def add_level_tn(es, staid):
    print(staid)
    body = {
        "query": {
            "term": {
            "STAID": {
                "value": staid
                }
            }
        },
        "script": {
            "source": """
            double x = Double.parseDouble(ctx._source.TN.trim()) / 10;
            if (x < -25) ctx._source.TN_LEVEL = 0;
            else if (x >= -25 && x < -20) ctx._source.TN_LEVEL = 1;
            else if (x >= -20 && x < -15) ctx._source.TN_LEVEL = 2;
            else if (x >= -15 && x < -10) ctx._source.TN_LEVEL = 3;
            else if (x >= -10 && x < -5) ctx._source.TN_LEVEL = 4;
            else if (x >= -5 && x < 0) ctx._source.TN_LEVEL = 5;
            else if (x >= 0 && x < 5) ctx._source.TN_LEVEL = 6;
            else if (x >= 5 && x < 10) ctx._source.TN_LEVEL = 7;
            else if (x >= 10 && x < 15) ctx._source.TN_LEVEL = 8;
            else if (x >= 15) ctx._source.TN_LEVEL = 9;
            """
        }
    }
    res = es.update_by_query(index='weathers', body=body)


def add_level_tg(es, staid):
    print(staid)
    body = {
        "query": {
            "term": {
            "STAID": {
                "value": staid
                }
            }
        },
        "script": {
            "source": """
            double x = Double.parseDouble(ctx._source.TG.trim()) / 10;
            if (x < -20) ctx._source.TG_LEVEL = 0;
            else if (x >= -20 && x < -15) ctx._source.TG_LEVEL = 1;
            else if (x >= -15 && x < -10) ctx._source.TG_LEVEL = 2;
            else if (x >= -10 && x < -5) ctx._source.TG_LEVEL = 3;
            else if (x >= -5 && x < 0) ctx._source.TG_LEVEL = 4;
            else if (x >= 0 && x < 5) ctx._source.TG_LEVEL = 5;
            else if (x >= 5 && x < 10) ctx._source.TG_LEVEL = 6;
            else if (x >= 10 && x < 15) ctx._source.TG_LEVEL = 7;
            else if (x >= 15 && x < 20) ctx._source.TG_LEVEL = 8;
            else if (x >= 20) ctx._source.TG_LEVEL = 9;
            """
        }
    }

    res = es.update_by_query(index='weathers', body=body)


def add_level_other(es, staid, field):
    print(staid,field)
    body = {
        "query": {
            "term": {
            "STAID": {
                "value": staid
                }
            }
        },
        "script": {
            "source": """
            int x = ctx._source.%s == null ? -1 : Integer.parseInt(ctx._source.%s.trim());
            ctx._source.%s_NUMBER = x;""" % (field, field, field)
        }
    }

    res = es.update_by_query(index='weathers', body=body)



es = Elasticsearch(
    ["http://localhost:9200"]
)

for staid in range(1,26374):
    add_level_tx(es,staid)

for staid in range(1,26374):
    add_level_tn(es,staid)

for staid in range(1,26374):
    add_level_tg(es,staid)

for field in ["RR","PP","CC","HU","SD","SS","QQ","FG","FX","DD"]:
    for staid in range(1,26374):
        add_level_other(es,staid,field)