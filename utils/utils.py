import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, NullType
from subprocess import PIPE, Popen

local_dir = os.getenv("FILES")

def configure_session(spark):
    spark.conf.spark.ui.port = 4040
    spark.conf.spark.ui.port = 4041


def set_logger(sc):
    log4jLogger = sc._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)
    sc.setLogLevel('INFO')
    return logger


def get_sparksession(master, app_name):
    spark = SparkSession \
        .builder \
        .master(master) \
        .appName(app_name) \
        .config("spark.debug.maxToStringFields", 100) \
        .config("spark.driver.extraClassPath", "C:/Program Files (x86)/MySQL/Connector J 8.0/mysql-connector-j-8.0.32.jar") \
        .getOrCreate()

    logger = set_logger(spark.sparkContext)

    return spark, logger


def save_table(df, user, password, database, table):
    df.write.format("jdbc") \
        .mode("append") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://localhost:3306/" + database) \
        .option("dbtable", table) \
        .option("user", user) \
        .option("password", password) \
        .save()


def get_dataframe_from_file(spark, file_name, schema, query, logger):
    for file in os.listdir(local_dir):
        file_path = local_dir + "/" + file
        if file_name in file:
            logger.info(f"{file_name} encontrado. Procedendo com as transformações.")

            # logger.info("Popen Hadoop")
            # rm = Popen(["hadoop", "fs", "-rm", local_dir + "/" + file], stdin=PIPE, bufsize=-1)
            # rm.communicate()
            #
            # put = Popen(["hadoop", "fs", "-put", local_dir + "/" + file], stdin=PIPE, bufsize=-1)
            # put.communicate()

            logger.info(f"Lendo o arquivo CSV: {file_name}")
            df = spark.read.option("delimiter", ";").option('encoding', 'ISO-8859-1').csv(file_path, header=True, schema=schema, enforceSchema=True)
            df = df.select(query)

            return df


def query():
    pass