from pyspark.sql.types import StructType, StructField, StringType

schema_AuxilioBrasil = StructType([
    StructField("MES_COMPETENCIA", StringType(), True),
    StructField("MES_REFERENCIA", StringType(), True),
    StructField("UF", StringType(), True),
    StructField("CODIGO_MUNICIPIO_SIAFI", StringType(), True),
    StructField("NOME_MUNICIPIO", StringType(), True),
    StructField("CPF_FAVORECIDO", StringType(), True),
    StructField("NIS_FAVORECIDO", StringType(), True),
    StructField("NOME_FAVORECIDO", StringType(), True),
    StructField("VALOR_PARCELA", StringType(), True)
])
