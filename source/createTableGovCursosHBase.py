from constants import *
import findspark

findspark.init()
from pyspark.sql import SparkSession


def run():
    spark = SparkSession.builder.getOrCreate()

    spark.sql("""
    CREATE TABLE GOV.CURSOS_HBASE
    (
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
    CURSO_ANO_FIM string
    )
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
    WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,
    cf:MUNICIPIO,
    cf:NM_SUBTIPO_CURSO,
    cf:CO_CURSO,
    cf:NM_CURSO,
    cf:CH_MINIMA,
    cf:OFERTA,
    cf:NM_CICLO_MATRICULA,
    cf:CH_TOTAL,
    cf:CURSO_DATA_INICIO,
    cf:CURSO_DATA_FIM,
    cf:MODALIDADE_ENSINO,
    cf:NM_STATUS_CICLO,
    cf:NM_SITUACAO_CICLO,
    cf:NM_TIPO_OFERTA_CURSO,
    cf:NM_ESTAGIO,
    cf:CH_ESTAGIO,
    cf:NM_PROJETO_PEDAGOGICO,
    cf:NM_ETEC,
    cf:QTD_VAGAS,
    cf:QTD_INSCRITOS,
    cf:QTD_MATRICULAS,
    cf:CURSO_ANO_INICIO,
    cf:CURSO_ANO_FIM")
    TBLPROPERTIES ("hbase.table.name" = "CURSOS_HBASE")
    """).show()


if __name__ == '__main__':
    run()
