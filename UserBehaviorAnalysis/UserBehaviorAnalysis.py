

# Import package and depedencies
import re
import configparser
#import findspark
#findspark.init()

from pyspark import SparkConf
from pyspark.sql import Window, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from libs.udfs import find_programing_language, get_url
from libs.utils import get_spark_config, load_df, reformat_date

MONGODB_URI     = 'mongodb://127.0.0.1/'
DB_INPUT        = 'DEP303_ASM1'
DB_OUTPUT       = 'DEP303_ASM1'
QUESTIONS_TBL   = 'Questions'
ANSWERS_TBL     = 'Answers'

# spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 UserBehaviorAnalysis.py

if __name__ == "__main__":

    
    conf = get_spark_config('spark.ini')
    spark = SparkSession.builder \
        .config(conf=conf) \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .config('spark.mongodb.input.uri', MONGODB_URI+DB_INPUT) \
        .config('spark.mongodb.output.uri', MONGODB_URI+DB_OUTPUT) \
        .getOrCreate()
    
    '''
    spark = SparkSession.builder \
        .appName("Analysis StackOverflow's User Behavior") \
        .master("local[5]") \
        .config('spark.executor.memory', '4g') \
        .config('spark.driver.memory', '2g') \
        .config('spark.cores.max', '2') \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .config('spark.mongodb.input.uri', MONGODB_URI+DB_INPUT) \
        .config('spark.mongodb.output.uri', MONGODB_URI+DB_OUTPUT) \
        .getOrCreate()
    '''

    # Read data from collection names "Questions"
    questions_df = load_df(spark, MONGODB_URI, DB_INPUT, QUESTIONS_TBL).drop("_id")
    questions_df.printSchema()

    # Standardize Questions data
    questions_standardized_df = questions_df \
        .withColumn("ClosedDate", to_date(regexp_extract(col("ClosedDate"), r"([0-9-]+)T", 1), "yyyy-MM-dd")) \
        .withColumn("CreationDate", to_date(regexp_extract(col("CreationDate"), r"([0-9-]+)T", 1), "yyyy-MM-dd")) \
        .withColumn("OwnerUserId", col("OwnerUserId").cast(IntegerType()))

    # Check DF after Standardizing
    questions_standardized_df.printSchema()
    questions_standardized_df.filter(col("OwnerUserId") == "NA").show()

    # Read data from collection "Answers"
    answers_df = load_df(spark, MONGODB_URI, DB_INPUT, ANSWERS_TBL).drop("_id")
    answers_df.printSchema()

    # Standardize Answers data
    answers_standardized_df = answers_df \
        .withColumn("CreationDate", to_date(regexp_extract(col("CreationDate"), r"([0-9-]+)T", 1), "yyyy-MM-dd")) \
        .withColumn("OwnerUserId", col("OwnerUserId").cast(IntegerType()))
    
    # Check DF after Standardizing
    answers_standardized_df.printSchema()

    
    # ------------------------------------------
    # Requirement 1: Count programing languages
    # language_lst = ["Java", "Python", "C++", "C#", "Go", "Ruby", "Javascript", "PHP", "HTML", "CSS", "SQL"]
    # ------------------------------------------
    
    # Create UDF find programing language
    #def find_programing_language(body):
    # create regex language list
    #    re_language_lst = ["Java", "Python", "C++", "C#", "Go", "Ruby", "Javascript", "PHP", "HTML", "CSS", "SQL"]
    #
    #    # Find programing language
    #    programing_languages_lst = []
    #    for re_language in re_language_lst:
    #        if  re_language in body:
    #            programing_languages_lst.append(re_language)
    #    return programing_languages_lst
    
    # Define UDF Count programing languages
    find_programing_languageUDF = udf(find_programing_language, ArrayType(StringType()))

    count_programing_language = questions_standardized_df \
        .withColumn("Programing Language", find_programing_languageUDF(col('Body'))) \
        .withColumn("Programing Language", explode("Programing Language")) \
        .groupBy("Programing Language") \
        .count() 

    print("------------------------------------------")
    print("Requirement 1: Count programing languages")
    count_programing_language.printSchema()
    count_programing_language.show(truncate=False)
   
    
    # ------------------------------------------
    # Requirement 2: Get 20 domain mostly used
    # ------------------------------------------

    #-- Create get URL
    #def get_url(body):
    #    re_url = r'href="([0-9a-zA-Z_.:\/]*)"'
    #    return re.findall(re_url, body)

    # Define UDF get URL
    get_urlUDF = udf(get_url, ArrayType(StringType()))

    #-- Get 20 domain mostly used
    re_domain = r"https*:\/\/([a-zA-Z0-9\.]+)\/"

    print("------------------------------------------")
    print("Requirement 2: Get 20 domain mostly used")
    questions_domain_df = questions_standardized_df \
        .withColumn("Url", get_urlUDF(col("Body"))) \
        .withColumn("Url", explode(col("Url"))) \
        .withColumn("Domain", regexp_extract(col("Url"), re_domain, 1)) \
        .groupBy("Domain") \
        .count() \
        .orderBy(col("count").desc()) \
        .filter(col("Domain") != "") \
        .select("Domain", "count") \
        .show(20, truncate=False)

    
    # ------------------------------------------
    # # Requirement 3: Get score of each user in each day
    # ------------------------------------------
    
    #-- Get Question/Answers Field to repair for Union
    questions_for_union_df = questions_standardized_df \
        .select("OwnerUserId", "CreationDate", "Score") \
        .filter(col("OwnerUserId").isNotNull())

    answers_for_union_df = answers_standardized_df \
        .select("OwnerUserId", "CreationDate", "Score") \
        .filter(col("OwnerUserId").isNotNull()) 

    #-- Using unionAll to avoid lost duplicate data
    ownerUserId_score_df = answers_for_union_df.unionAll(questions_for_union_df)

    #-- Get score of each user in each day
    total_score_window = Window \
     .partitionBy("OwnerUserId") \
        .orderBy("CreationDate") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    score_users_per_day = ownerUserId_score_df \
        .filter(col("OwnerUserId").isNotNull()) \
        .withColumn("TotalScore", sum("Score").over(total_score_window))

    print("------------------------------------------")
    print("Requirement 3: Get score of each user in each day")
    score_users_per_day.show()

    
    # ------------------------------------------
    # # Requirement 4: Get score of each user in range of time
    # ------------------------------------------

    START_DATE = "01-01-2008"
    END_DATE   = "01-01-2009"

    # Re format date
    start_date = reformat_date(START_DATE)
    end_date = reformat_date(END_DATE)

    # ------------------------------------------
    #-- Get Question/Answers Field to repair for Union
    questions_for_union_df = questions_standardized_df \
        .select("OwnerUserId", "CreationDate", "Score") \
        .filter(col("OwnerUserId").isNotNull())

    answers_for_union_df = answers_standardized_df \
        .select("OwnerUserId", "CreationDate", "Score") \
        .filter(col("OwnerUserId").isNotNull()) 

    #-- Using unionAll to avoid lost duplicate data
    ownerUserId_score_df = answers_for_union_df.unionAll(questions_for_union_df)

    #-- Get score of each user in range of time
    score_users_in_range_time = ownerUserId_score_df \
        .filter((col("CreationDate") >= start_date) & (col("CreationDate") <= end_date)) \
        .groupBy("OwnerUserId") \
        .agg(sum("Score").alias("TotalScore")) \
        .orderBy("OwnerUserId")

    print("------------------------------------------")
    print("Requirement 4: Get score of each user in range of time")
    score_users_in_range_time.show()

    
    # ------------------------------------------
    # # Requirement 5: Find good questions with details
    # ------------------------------------------

    good_questions_df = answers_standardized_df \
        .groupBy("ParentId") \
        .agg(count(col("*")).alias("Number_answers")) \
        .filter(col("Number_answers") > 5)
    
    #good_questions_df.show()

    # Optimize join
    # spark.conf.set("spark.sql.shuffle.partitions", 3)

    join_expr = questions_standardized_df.Id == good_questions_df.ParentId
    join_df = questions_standardized_df.join(good_questions_df, join_expr, "inner")

    join_df.printSchema()

    
    # # Using Bucket join
    # good_questions_df.write \
    #     .format('parquet')  \
    #     .bucketBy(16, 'ParentId') \
    #     .sortBy('Number_answers') \
    #     .mode("overwrite") \
    #     .saveAsTable("good_questions_bucketed")
    
    # questions_standardized_df.write\
    #     .format('parquet')  \
    #     .bucketBy(16, "ParentId") \
    #     .sortBy("CreationDate") \
    #     .mode("overwrite") \
    #     .saveAsTable("questions_bucketed")
    
    # q1 = spark.table("good_questions_bucketed")
    # q2 = spark.table("questions_bucketed")

    # join_df = q1.join(q2, "ParentId")
    # # End Using Bucket join

    print("------------------------------------------")
    print("Requirement 5: Find questions with more than 5 answers")
    join_df.orderBy(col("Number_answers").asc(),col("Id").asc()).show()
    join_df.orderBy(col("Number_answers").desc(),col("Id").asc()).show()

    

    # ------------------------------------------
    # # Requirement 6: Find active users
    # ------------------------------------------
    # # active_users_df = find_active_user(join_df)
    # # active_users_df.show()

    # Find good questions. It is a simple way to do requirement 5
    # ParentID equals QuestionID
    # good_questions_df = find_good_questions(answers_standardized_df).show()

    # Rename column field for Answers DF to repair for Join with Question DF
    answers_standardized_df2 = answers_standardized_df \
        .withColumnRenamed("Id", "answerId") \
        .withColumnRenamed("OwnerUserId", "answerUserId") \
        .withColumnRenamed("CreationDate", "answerCreationDate") \
        .withColumnRenamed("Score", "answerScore") \
        .withColumnRenamed("Body", "answerBody")

    # Join questions_standardized_df with answers_standardized_df2 
    join_expr = questions_standardized_df.Id == answers_standardized_df2.ParentId
    join_df = questions_standardized_df.join(answers_standardized_df2, join_expr, "inner")
    #join_df.show()

    # Condition 1 : Use Answers Field to filter User has more than 50 answers or sum of answer's score greater than 500
    df1 = join_df \
        .groupBy("answerUserId") \
        .agg(count(col("*")).alias("number_answers"),
               sum(col("answerScore")).alias("total_answers_score")) \
        .filter((col("number_answers") > 50) | (col("total_answers_score") > 500)) \
        .select("answerUserId","number_answers","total_answers_score")

    df1.orderBy(col("number_answers").asc(),col("total_answers_score").asc()).show()
    df1.orderBy(col("total_answers_score").asc(),col("number_answers").asc()).show()

    # Condition 2: Use Answers/Questions Field to filter User has more than 5 answers in date when questions created
    df2 = join_df \
        .filter(col("answerCreationDate") == col("CreationDate")) \
        .groupBy("OwnerUserId") \
        .agg(count("*").alias("number_answers_same_date_question")) \
        .filter(col("number_answers_same_date_question") > 5) \
        .select("OwnerUserId", "number_answers_same_date_question")

    df2.orderBy(col("number_answers_same_date_question").asc(),col("OwnerUserId").asc()).show()

    # Get all active user using union (not unionAll) to avoid duplicate UserId
    active_users_df =  df1.withColumnRenamed("answerUserId", "UserId").select("UserId") \
        .union(df2.select("OwnerUserId"))
    
    print("------------------------------------------")
    print("Requirement 6: Find active users")
    active_users_df.filter(col("UserId").isNotNull()) \
        .orderBy(col("UserId").asc()) \
        .show()
   