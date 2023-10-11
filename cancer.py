#import SparkSession
from time import sleep
from cassandra.cluster import Cluster
import pandas as pd
from pyspark.shell import spark
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier


def readFromCassandra():
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect('breastcancer', wait_for_all_pools=True)
    session.execute('USE breastcancer')
    rows = session.execute("SELECT * FROM datacancer ")
    df = pd.DataFrame(list(rows))
    print(df.head())
    return df

df = readFromCassandra()
df = spark.createDataFrame(df)
features_col = ['barenuclei', 'blandchromatin', 'clumpthickness', 'marginaladhesion', 'mitoses', 'normalnucleoli', 'singleepithelialcellsize', 'uniformitycellshape', 'uniformitycellsize']
assembler = VectorAssembler(inputCols=features_col,outputCol="features")
data = assembler.transform(df)

train_df, test_df = data.randomSplit([0.80,0.20])
model = LogisticRegression(labelCol='classes')
#model = RandomForestClassifier(labelCol="classes", featuresCol="features", numTrees=30)
trained_model = model.fit(train_df)

test_predictions = trained_model.evaluate(test_df)
print("Accuracy: ", test_predictions.accuracy)
sleep(100)