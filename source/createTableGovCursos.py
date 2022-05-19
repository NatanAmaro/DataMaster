from constants import *
import findspark

findspark.init()
from pyspark.sql import SparkSession


def run():
    spark = SparkSession.builder.getOrCreate()

    spark.sql("""
    CREATE TABLE GOV.CURSOS(
    MUNICIPIO string,
    NM_SUBTIPO_CURSO string,
    CO_CURSO string,
    NM_CURSO string,
    CH_MINIMA string,
    OFERTA string,
    NM_CICLO_MATRICULA string,
    CH_TOTAL string,
    CURSO_DATA_INICIO string,
    CURSO_DATA_FIM string,
    MODALIDADE_ENSINO string,
    NM_STATUS_CICLO string,
    NM_SITUACAO_CICLO string,
    NM_TIPO_OFERTA_CURSO string,
    NM_ESTAGIO string,
    CH_ESTAGIO string,
    NM_PROJETO_PEDAGOGICO string,
    NM_ETEC string,
    QTD_VAGAS int,
    QTD_INSCRITOS int,
    QTD_MATRICULAS int,
    CURSO_ANO_INICIO string,
    CURSO_ANO_FIM string)
    """).show()


if __name__ == '__main__':
    run()
