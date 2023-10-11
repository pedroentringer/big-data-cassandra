import pandas as pd
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
from time import sleep

# Inicializar a sessao do Spark
spark = SparkSession.builder.appName("KS_NASA").getOrCreate()


def create_cassandra_session():
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect('ks_nasa', wait_for_all_pools=True)
    session.execute('USE ks_nasa;')
    return session;


def fetch_data(session, type):
    query = "SELECT * FROM tbl_sensors WHERE file_type = '{}' ALLOW FILTERING;".format(type)

    rows = session.execute(query)

    # Converter as linhas em um RDD
    rdd = spark.sparkContext.parallelize(rows)

    # Criar um DataFrame a partir do RDD
    df = spark.createDataFrame(rdd)

    # retorna o DataFrame como Pandas
    return df.toPandas()

def main():
    session = create_cassandra_session()

    jet_data = fetch_data(session, "train")
    test_data = fetch_data(session, "test")

    print(jet_data.shape)
    print(test_data.shape)

if __name__ == "__main__":
    main()
    sleep(100000)
