from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, TimestampType, LongType
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("--target_date", required=True)
args = parser.parse_args()

target_date = args.target_date
print(f"------------------------------------------------------------Target date: {target_date}")


spark = SparkSession.builder \
    .master('spark://spark-master:7077') \
    .appName('staging') \
    .getOrCreate()


event_schema = StructType([
    StructField("id", LongType(), False),
    StructField("type", StringType(), False),
    StructField("actor_id", LongType(), False),
    StructField("repo_id", LongType(), False),
    StructField("public", BooleanType(), False),
    StructField("created_at", TimestampType(), False),
    StructField("org_id", StringType(), True)
])

user_schema = StructType([
    StructField("id", LongType(), False),
    StructField("login", StringType(), False),
    StructField("display_login", StringType(), True),
    StructField("gravatar_id", StringType(), True),
    StructField("url", StringType(), False),
    StructField("avatar_url", StringType(), True)
])

org_schema = StructType([
    StructField("id", LongType(), False),
    StructField("login", StringType(), False),
    StructField("gravatar_id", StringType(), True),
    StructField("url", StringType(), False),
    StructField("avatar_url", StringType(), True)
])

repo_schema = StructType([
    StructField("id", LongType(), False),
    StructField("name", StringType(), False),
    StructField("url", StringType(), False)
])


event_df = spark.read.schema(event_schema)\
.option("header", "true")\
.csv(f"s3a://airflow/gh_data/events/events_{target_date}_*.csv").dropDuplicates(['id'])
event_df.show()

user_df = spark.read.schema(user_schema)\
.option("header", "true")\
.csv(f"s3a://airflow/gh_data/users/users_{target_date}_*.csv").dropDuplicates(['id'])

org_df = spark.read.schema(org_schema)\
.option("header", "true")\
.csv(f"s3a://airflow/gh_data/orgs/orgs_{target_date}_*.csv").dropDuplicates(['id'])

repo_df = spark.read.schema(repo_schema)\
.option("header", "true")\
.csv(f"s3a://airflow/gh_data/repos/repos_{target_date}_*.csv").dropDuplicates(['id'])


event_df.write.mode("overwrite").parquet(f"s3a://airflow/gh_data_staging/events/{target_date}/")
user_df.write.mode("overwrite").parquet(f"s3a://airflow/gh_data_staging/users/{target_date}/")
org_df.write.mode("overwrite").parquet(f"s3a://airflow/gh_data_staging/orgs/{target_date}/")
repo_df.write.mode("overwrite").parquet(f"s3a://airflow/gh_data_staging/repos/{target_date}/")