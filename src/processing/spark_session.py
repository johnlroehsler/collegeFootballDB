import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# Config
load_dotenv(dotenv_path="config/keys.env")
_JAVA17 = "/opt/homebrew/opt/openjdk@17"
if os.path.isdir(_JAVA17):
    os.environ["JAVA_HOME"] = _JAVA17
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION     = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

# Return a SparkSession configured for local mode with S3A support.
def get_spark(app_name: str = "CollegeFootballDB") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.4.1,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.367",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY or "")
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY or "")
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
