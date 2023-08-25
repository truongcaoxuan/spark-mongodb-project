
import configparser
from pyspark import SparkConf

cfg_data = '''
[SPARK_CONFIGS]
spark.app.name = "Analytic User Behavior on Stack Overflow"
spark.master = local[3]
spark.jars.packages = 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1'

'''

'''
parser = configparser.ConfigParser()
parser.read('sampleconfig.ini')
for sect in parser.sections():
   print('Section:', sect)
   for k,v in parser.items(sect):
      print(' {} = {}'.format(k,v))
   print()
'''

spark_conf = SparkConf()
config = configparser.ConfigParser()
config.read('spark.ini')
for sect in config.sections():
    print('Section:', sect)
    for (key, val) in config.items(sect):
        print(key +":"+ val)
        spark_conf.set(key, val)
