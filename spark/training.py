# This file includes training, using different sparkml functions.
# It uses several regression and decision tree with vector assembler

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch
import pyspark.pandas as ps

num_stats = 10000

es = Elasticsearch(
  ['http://es-cn-x0r3hvdmx000ka9ud.public.elasticsearch.aliyuncs.com:9200'],
  basic_auth=('elastic', 'ZhouWuShangWuDaShuJuXiaoZu4')
)

query = {
  "query": {
    "terms": {
       "STAID": [1]
    }
  },
  "from": 0, 
  "size": num_stats, 
}

response = es.search(index="weathers", body=query)


print("查询结果的条数：", response["hits"]["total"]["value"])
print("example:", response["hits"]["hits"][0])
weathers = []
requests = []
idx = 0
rints = [128,
        8341,
        5503,
        3288,
        3225,
        4402,
        10903,
        3204,
        24173,
        8548,
        4180,
        19046,
        11354,
        395,
        2600,
        24426,
        4121,
        2139,
        4788,
        2611,]
import random
for i in range(20):
    query["query"]["terms"]["STAID"] = [rints[i]]
    response = es.search(index="weathers", body=query)
    for hit in response["hits"]["hits"]:
        item = hit["_source"]
        item["idx"] = idx
        idx += 1
        weathers.append(item)
        requests.append(hit)
print("All:", len(weathers))
data = pd.DataFrame(weathers)
del data['geo']
# del data['location']
data = data.sort_values(by= ["STAID", "DATE"], ascending=[True, True])
data = data.reset_index(drop=True)
print(data)

#处理数据
def getyear(date):
    date = str(date)
    date = date[0:4]
    date = int(date)
    return date

def getmonth(date):
    date = str(date)
    date = date[4:6]
    date = int(date)
    return date

def getday(date):
    date = str(date)
    date = date[6:]
    date = int(date)
    return date

def toint(date):
    date = int(date)
    return date

def cloth(temperature):
    if temperature < -150:
        return 0
    if temperature < 50:
        return 1
    if temperature < 150:
        return 2
    if temperature < 200:
        return 3
    if temperature < 250:
        return 4
    if temperature < 280:
        return 5
    if temperature < 330:
        return 6
    return 7


data["YEAR"] = data.apply(lambda x: getyear(x['DATE']), axis = 1)
data["MONTH"] = data.apply(lambda x: getmonth(x['DATE']), axis = 1)
data["DAY"] = data.apply(lambda x: getday(x['DATE']), axis = 1)
data["TX"] = data.apply(lambda x: toint(x['TX']), axis = 1)
data["TG"] = data.apply(lambda x: toint(x['TG']), axis = 1)
data["TN"] = data.apply(lambda x: toint(x['TN']), axis = 1)
data["CLOTH"] = data.apply(lambda x: cloth(x['TG']), axis = 1)

prev_days = 2
length = len(data)
for i in range(1, prev_days + 1):
    days_ago = "TX_" + str(i) + "D_AGO"
    data[days_ago] = 0
    days_ago = "TG_" + str(i) + "D_AGO"
    data[days_ago] = 0
    days_ago = "TN_" + str(i) + "D_AGO"
    data[days_ago] = 0
data["AVG_TX_DAY"] = 0
data["AVG_TG_DAY"] = 0
data["AVG_TN_DAY"] = 0
print(len(data))
for i in range(length):
    for j in range(1, min(i + 1, prev_days + 1)):
        days_ago = "TX_" + str(j) + "D_AGO"
        data.loc[i,days_ago] = data.iloc[i - j]["TX"]
        days_ago = "TG_" + str(j) + "D_AGO"
        data.loc[i,days_ago] = data.iloc[i - j]["TG"]
        days_ago = "TN_" + str(j) + "D_AGO"
        data.loc[i,days_ago] = data.iloc[i - j]["TN"]
    cur = data.iloc[i]
    m = cur["MONTH"]
    d = cur["DAY"]
    bf = data[(data["MONTH"] == m) & (data["DAY"] == d)]
    data.loc[i,"AVG_TX_DAY"] = bf["TX"].mean()
    data.loc[i,"AVG_TG_DAY"] = bf["TG"].mean()
    data.loc[i,"AVG_TN_DAY"] = bf["TN"].mean()

print(data)

input_cols = ["AVG_TX_DAY", "AVG_TG_DAY", "AVG_TN_DAY"] 
for i in range(1, prev_days + 1):
    days_ago = "TX_" + str(i) + "D_AGO"
    input_cols.append(days_ago)
    days_ago = "TG_" + str(i) + "D_AGO"
    input_cols.append(days_ago)
    days_ago = "TN_" + str(i) + "D_AGO"
    input_cols.append(days_ago)
all_cols = input_cols.copy()
all_cols.append("TG")
all_cols.append("CLOTH")
all_cols.append("idx")
Xs = data[all_cols].loc[prev_days:]
Xs

Ys = data["TG"].loc[prev_days:]
Ys


ps_data = ps.from_pandas(Xs)
Xs = ps_data.to_spark()
print("Spark:", Xs)
Xs.show()
Xs.printSchema()

# 用VectorAssembler合并所有特性
 
from pyspark.ml.feature import VectorAssembler
 
print(input_cols)
assembler = VectorAssembler(
            inputCols= input_cols,
            outputCol='features')
 
transformed_data = assembler.transform(Xs)
transformed_data.show()

(training_data, test_data) = transformed_data.randomSplit([0.8,0.2], seed = 42)
print("训练数据集总数: " + str(training_data.count()))
print("测试数据集总数: " + str(test_data.count()))

from pyspark.ml.regression import DecisionTreeRegressor, LinearRegression, RandomForestRegressor, \
                                  FMRegressor, IsotonicRegression, GeneralizedLinearRegression, \
                                  GBTRegressor,  AFTSurvivalRegression

from pyspark.ml.evaluation import RegressionEvaluator

#气温预测

#Decision Tree
rf = DecisionTreeRegressor(labelCol='TG', 
                           featuresCol='features',
                           maxDepth=5)
model = rf.fit(training_data)
rf_predictions = model.transform(test_data)
 
multi_evaluator = RegressionEvaluator(
      labelCol = 'TG', metricName = "rmse")
print('DecisionTree Rmse:', multi_evaluator.evaluate(rf_predictions))


#LinearRegression
rf = LinearRegression(labelCol='TG', 
                      featuresCol='features')
model = rf.fit(training_data)
rf_predictions = model.transform(test_data)
 
multi_evaluator = RegressionEvaluator(
      labelCol = 'TG', metricName = "rmse")
print('LinearRegression Rmse:', multi_evaluator.evaluate(rf_predictions))

#RandomForest
rf = RandomForestRegressor(labelCol='TG', 
                           featuresCol='features',
                           maxDepth=5)
model = rf.fit(training_data)
rf_predictions = model.transform(test_data)
 
multi_evaluator = RegressionEvaluator(
      labelCol = 'TG', metricName = "rmse")
print('RandomForestRegressor Rmse:', multi_evaluator.evaluate(rf_predictions))

#FMRegressor
rf = FMRegressor(labelCol='TG', 
                 featuresCol='features')
model = rf.fit(training_data)
rf_predictions = model.transform(test_data)
 
multi_evaluator = RegressionEvaluator(
      labelCol = 'TG', metricName = "rmse")
print('FMRegressor Rmse:', multi_evaluator.evaluate(rf_predictions))

#IsotonicRegression
rf = IsotonicRegression(labelCol='TG', 
                        featuresCol='features')
model = rf.fit(training_data)
rf_predictions = model.transform(test_data)
 
multi_evaluator = RegressionEvaluator(
      labelCol = 'TG', metricName = "rmse")
print('IsotonicRegression Rmse:', multi_evaluator.evaluate(rf_predictions))

#GBTRegressor
rf = GBTRegressor(labelCol='TG', 
                  featuresCol='features',
                  maxDepth=5)
model = rf.fit(training_data)
rf_predictions = model.transform(test_data)
 
multi_evaluator = RegressionEvaluator(
      labelCol = 'TG', metricName = "rmse")
print('GBTRegressor Rmse:', multi_evaluator.evaluate(rf_predictions))

#GeneralizedLinearRegression
rf = GeneralizedLinearRegression(labelCol='TG', 
                                 featuresCol='features')
model = rf.fit(training_data)
rf_predictions = model.transform(test_data)
 
multi_evaluator = RegressionEvaluator(
      labelCol = 'TG', metricName = "rmse")
print('GeneralizedLinearRegression Rmse:', multi_evaluator.evaluate(rf_predictions))
test_data.show()
rf_predictions.show()
rf_predictions = rf_predictions.to_pandas_on_spark().to_pandas() 
final = rf_predictions[['prediction', 'TG']]
final['diff'] = abs(rf_predictions['TG'] - rf_predictions['prediction'])
final['wrong1'] = final['diff'] < 10
final['wrong2'] = final['diff'] < 20
final['wrong3'] = final['diff'] < 30
final['wrong5'] = final['diff'] < 50
print(final[:30])
print("wrong sum1:", final['wrong1'].sum() / len(final))
print("wrong sum2:", final['wrong2'].sum() / len(final))
print("wrong sum3:", final['wrong3'].sum() / len(final))
print("wrong sum5:", final['wrong5'].sum() / len(final))
print("mean diff", final['diff'].mean())

#穿衣指数
from pyspark.ml.classification import DecisionTreeClassifier, RandomForestClassifier, \
                                      LinearSVC, GBTClassifier, MultilayerPerceptronClassifier

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

rf = DecisionTreeClassifier(labelCol='CLOTH', 
                            featuresCol='features',
                            maxDepth=5)
model = rf.fit(training_data)
rf_predictions = model.transform(test_data)
 
multi_evaluator = MulticlassClassificationEvaluator(
      labelCol = 'CLOTH', metricName = "accuracy")
print('DecisionTree accuracy:', multi_evaluator.evaluate(rf_predictions))

rf = RandomForestClassifier(labelCol='CLOTH', 
                            featuresCol='features',
                            maxDepth=5)
model = rf.fit(training_data)
rf_predictions = model.transform(test_data)
 
multi_evaluator = MulticlassClassificationEvaluator(
      labelCol = 'CLOTH', metricName = "accuracy")
print('RandomForestClassifier accuracy:', multi_evaluator.evaluate(rf_predictions))

print(type(rf_predictions))
rf_predictions = rf_predictions.to_pandas_on_spark().to_pandas() 
print(rf_predictions[['prediction', 'idx']])
out_pred = rf_predictions[['prediction', 'idx']]
length = len(out_pred)
ppp = 0
doc = []
for i in range(length):
    # print("_____________________")
    # print(out_pred.loc[i, 'prediction'], " ", out_pred.loc[i, 'idx'])
    p_idx = out_pred.loc[i, 'idx']
    weathers[p_idx]['CLOTH'] = out_pred.loc[i, 'prediction']
    requests[p_idx]['_source']['CLOTH'] = out_pred.loc[i, 'prediction']
    requests[p_idx]['_source'].pop('idx')
    # print(requests[p_idx])
    info = {}
    info["update"] = {}
    info["update"]["_index"] = requests[p_idx]["_index"]
    info["update"]["_id"] = requests[p_idx]["_id"]
    doc.append(info)
    info = {}
    info["doc"] = requests[p_idx]["_source"]
    doc.append(info)
print("doc:", len(doc))
es.bulk(index="weathers", body=doc)
print("rint")
print("final")

layers = [len(input_cols), 50, 50, 8]
rf = MultilayerPerceptronClassifier(featuresCol='features',
                                    labelCol='CLOTH', 
                                    layers = layers,
                                    solver = "l-bfgs")
model = rf.fit(training_data)
rf_predictions = model.transform(test_data)
 
multi_evaluator = MulticlassClassificationEvaluator(
      labelCol = 'CLOTH', metricName = "accuracy")
print('l-bfgs-Based MultilayerPerceptronClassificationModel accuracy:', multi_evaluator.evaluate(rf_predictions))

rf = MultilayerPerceptronClassifier(featuresCol='features',
                                    labelCol='CLOTH', 
                                    layers = layers,
                                    solver = "gd")
model = rf.fit(training_data)
rf_predictions = model.transform(test_data)
 
multi_evaluator = MulticlassClassificationEvaluator(
      labelCol = 'CLOTH', metricName = "accuracy")
print('gd-Based MultilayerPerceptronClassificationModel accuracy:', multi_evaluator.evaluate(rf_predictions))

test_data.show()
rf_predictions.show()
