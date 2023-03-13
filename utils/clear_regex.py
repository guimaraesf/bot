from pyspark.sql.functions import regexp_replace, col

def clear_regex(file_name, df, logger):
    if file_name == "AuxilioBrasil.csv":
        logger.info(f"Removendo caracteres especiais.")
        return df.select(
            regexp_replace(col("MES_COMPETENCIA"), "[^A-Za-z0-9 ]", "").alias("MES_COMPETENCIA"),
            regexp_replace(col("MES_REFERENCIA"), "[^A-Za-z0-9 ]", "").alias("MES_REFERENCIA"),
            regexp_replace(col("UF"), "[^A-Za-z0-9 ]", "").alias("UF"),
            regexp_replace(col("CODIGO_MUNICIPIO_SIAFI"), "[^A-Za-z0-9 ]", "").alias("CODIGO_MUNICIPIO_SIAFI"),
            regexp_replace(col("NOME_MUNICIPIO"), "[^A-Za-z0-9 ]", "").alias("NOME_MUNICIPIO"),
            regexp_replace(col("CPF_FAVORECIDO"), "[^A-Za-z0-9 ]", "").alias("CPF_FAVORECIDO"),
            regexp_replace(col("NIS_FAVORECIDO"), "[^A-Za-z0-9 ]", "").alias("NIS_FAVORECIDO"),
            regexp_replace(col("NOME_FAVORECIDO"), "[^A-Za-z0-9 ]", "").alias("NOME_FAVORECIDO"),
            regexp_replace(col("VALOR_PARCELA"),"[.]", "").alias("VALOR_PARCELA")
        )

    else:
        logger.info(f"Arquivo {file_name} não encontrado")

