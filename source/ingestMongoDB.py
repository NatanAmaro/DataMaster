#TODO Arquivo unutilizado estrategia alterada.
import pymongo_spark
import findspark
from constants import *

findspark.init()
from pyspark.sql import SparkSession
import os


def run():
    pymongo_spark.activate()

    MONGO_CONN_STRING = os.getenv('MONGO_CONN_STRING')

    spark = SparkSession.builder \
        .enableHiveSupport() \
        .getOrCreate()

    df = spark.sql("""SELECT * FROM GOV.CURSOS LIMIT 1000""")

    rdd = df.rdd.map(tuple)

    rdd.saveToMongoDB(MONGO_CONN_STRING + '/admin.cursos')


if __name__ == '__main__':
    run()
