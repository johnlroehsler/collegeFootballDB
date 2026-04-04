import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# Config
load_dotenv(dotenv_path="config/keys.env")
_JAVA17_PATHS = [
    r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot",
    "/opt/homebrew/opt/openjdk@17",
]
for _p in _JAVA17_PATHS:
    if os.path.isdir(_p):
        os.environ["JAVA_HOME"] = _p
        break

# Hadoop winutils for Windows
_HADOOP_HOME = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, "hadoop")
_HADOOP_HOME = os.path.normpath(_HADOOP_HOME)
if os.name == "nt" and os.path.isdir(_HADOOP_HOME):
    os.environ["HADOOP_HOME"] = _HADOOP_HOME
    _hadoop_bin = os.path.join(_HADOOP_HOME, "bin")
    if _hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = _hadoop_bin + os.pathsep + os.environ.get("PATH", "")

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION     = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

# Return a SparkSession configured for local mode with S3A support.
def get_spark(app_name: str = "CollegeFootballDB") -> SparkSession:
    spark = (
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
        .config("spark.driver.extraJavaOptions",
                "--add-opens java.base/javax.security.auth=ALL-UNNAMED")
        .config("spark.executor.extraJavaOptions",
                "--add-opens java.base/javax.security.auth=ALL-UNNAMED")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark