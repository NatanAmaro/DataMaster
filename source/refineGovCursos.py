from constants import *
import findspark

findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def run():
    df = readDf()
    dfRefined = refineDf(df)
    writeHiveTable(dfRefined)


def readDf():
    return spark.read.csv('/data/ingestion/file.csv', sep=';', header=True)


def refineDf(df):
    return df.withColumn('QTD_VAGAS', df['QTD_VAGAS'].cast('int')) \
        .withColumn('QTD_INSCRITOS', df['QTD_INSCRITOS'].cast('int')) \
        .withColumn('QTD_MATRICULAS', df['QTD_MATRICULAS'].cast('int'))


def writeHiveTable(df):
    df.write.insertInto('GOV.CURSOS')


if __name__ == '__main__':
    run()
