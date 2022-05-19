from constants import *
import findspark

findspark.init()
from pyspark.sql import SparkSession


def run():
    spark = SparkSession.builder.getOrCreate()

    spark.sql("""
    INSERT INTO CURSOS_HBASE SELECT * FROM CURSOS;
    """).show()


if __name__ == '__main__':
    run()
