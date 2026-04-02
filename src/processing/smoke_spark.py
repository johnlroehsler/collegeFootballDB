import sys
import os
from dotenv import load_dotenv
from spark_session import get_spark

# Config
load_dotenv(dotenv_path="config/keys.env")
BUCKET = os.getenv("S3_BUCKET_NAME")
if not BUCKET:
    sys.exit("ERROR: S3_BUCKET_NAME not set. Create config/keys.env from config/example.env.")
year = sys.argv[1] if len(sys.argv) > 1 else "2025"
week = sys.argv[2] if len(sys.argv) > 2 else "1"
team = sys.argv[3] if len(sys.argv) > 3 else "Georgia"

# Build the S3 file path, formatting team names with underscores
s3_path = f"s3a://{BUCKET}/raw/year={year}/week={week}/{team.replace(' ', '_')}_plays.json"

print(f"\nSpark S3 Smoke Test")
print(f"Target: {s3_path}\n")

# Start a Spark session named "SmokeTest"
spark = get_spark("SmokeTest")

# Hide noisy INFO logs
spark.sparkContext.setLogLevel("WARN")

# Load the JSON data from S3 into a DataFrame
df = spark.read.json(s3_path)

print(f"Schema:")
# Show the data structure
df.printSchema()

# Count the total rows
print(f"\nRow count: {df.count()}")

print(f"\nSample (5 rows):")
# Show the first 5 rows of key columns without truncating text
df.select("id", "playType", "yardsGained", "down", "distance").show(5, truncate=False)

# Close the Spark session
spark.stop()
print("\nSmoke test passed.")