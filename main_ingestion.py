import os
import sys
from utils.utils import *
from utils.schemas import *
from utils.clear_regex import *
from utils.clear_date import *
from utils.queries import *
from datetime import date, timedelta, datetime, timezone
from pyspark.sql.functions import *

local_dir = os.getenv("FILES")

# Variáveis de configuração SparkSession
master = "local[*]"
app_name = "beneficios-sociais"
spark, logger = get_sparksession(master, app_name)

# Variáveis de configuração de conexão com MySQL
user = "root"
password = "UtcqupJy3vH@1Vid"
database = "beneficios"
table = "auxbrasil"

# Arquivos dos benefícios para download
file_auxbrasil = "AuxilioBrasil.csv"


def data_ingestion(spark, snippet_name, schema, query, logger):
    # Número de dias subtraídos da data atual.
    date_list = [126]

    for i in date_list:
        try:
            last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=i)
            date_ref = last_day_of_prev_month.strftime("%Y%m")
            file_name = date_ref + "_" + snippet_name

            df = get_dataframe_from_file(spark, file_name, schema, query, logger)
            df = clear_regex(snippet_name, df, logger)
            df = date_mutation(snippet_name, df, logger)
            logger.info(f"Realizando a ingestão dos dados no MySQL {database} na tabela {table}.")
            save_table(df, user, password, database, table)
            logger.info(f"Encerrando a sessão Spark")
            spark.stop()

        except Exception as e:
            logger.info(f"Erro no arquivo {file_name}: {type(e).__name__} - {e}")


def main():
    logger.info(f"Iniciando o processo de ingestão de dados dos Benefícios Sociais.")
    data_ingestion(spark, file_auxbrasil, schema_AuxilioBrasil, select_AuxilioBrasil, logger)


if __name__ == "__main__":
    main()
