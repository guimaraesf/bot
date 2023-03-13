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
snippet_name = "AuxilioBrasil.csv"

# SparkSession Setup Variables
master = "local[*]"
app_name = "beneficios-sociais"
spark, logger = get_sparksession(master, app_name)

# Connector MySQL Setup Variables
user = "root"
password = "UtcqupJy3vH@1Vid"
database = "beneficios"
table = "auxbrasil"


def main(spark, snippet_name, schema, query, logger):
    # Number of days subtracted from the current date
    date_list = [126]

    for i in date_list:
        try:
            last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=i)
            date_ref = last_day_of_prev_month.strftime("%Y%m")
            file_name = date_ref + "_" + snippet_name
            df = get_dataframe_from_file(spark, file_name, schema, query, logger)
            df = clear_regex(snippet_name, df, logger)
            df = date_mutation(snippet_name, df, logger)
            logger.info(f"Record inseting into {database} on {table} table.")
            save_table(df, user, password, database, table)
            spark.stop()

        except Exception as e:
            logger.info(f"Error in file {file_name}: {type(e).__name__} - {e}")


if __name__ == "__main__":
    main(spark, snippet_name, schema_AuxilioBrasil, select_AuxilioBrasil, logger)
