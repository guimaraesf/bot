from pyspark.sql.functions import col, lit, substring, concat, regexp_replace

def date_mutation(file_name, df, logger):
    if file_name == "AuxilioBrasil.csv":
        logger.info("Formatando as colunas de data e valor.")
        df = df.withColumn("VALOR_PARCELA", regexp_replace(col("VALOR_PARCELA"), "[,]", "."))
        df = df.withColumn("MES_REFERENCIA", concat(substring(col("MES_REFERENCIA"), 0, 4), lit("-"), substring(col("MES_REFERENCIA"), 5, 2), lit("-01")))
        df = df.withColumn("MES_COMPETENCIA", concat(substring(col("MES_COMPETENCIA"), 0, 4), lit("-"), substring(col("MES_COMPETENCIA"), 5, 2), lit("-01")))

        return df

    else:
        logger.info(f"Arquivo {file_name} não encontrado")
