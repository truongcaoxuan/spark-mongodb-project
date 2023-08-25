import configparser
import re
from pyspark import SparkConf

#--------------------------------------------------
def get_spark_config(config_file):
    spark_conf = SparkConf()
    parser = configparser.ConfigParser()
    parser.read(r'config_file')
    for sect in parser.sections():
        print('Section:', sect)
        for (key, val) in parser.items(sect):
            spark_conf.set(key, val)
    return spark_conf

#--------------------------------------------------
def load_df(spark, mongodb_uri, db_name, table_name):
    uri_source = mongodb_uri+db_name+'.'+table_name
    print(uri_source )
    return spark.read \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("uri", uri_source) \
        .load()

#--------------------------------------------------
def reformat_date(date):
    re_pattern_date = r"[0-9]{2}-[0-9]{2}-[0-9]{4}"
    if re.match(re_pattern_date, date):
        date_parts = date.split("-")
        day   = date_parts[0]
        month = date_parts[1]
        year  = date_parts[2]
        
        return year + "-" + month + "-" + day
    else:
        return date