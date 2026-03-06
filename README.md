CollegeFootballDB:
A distributed analytics platform for processing and querying SEC football play-by-play data (2021–2025). Built for CS 4265: Big Data Analytics.

Description:
This project proposes a distributed analytics platform for processing and querying of SEC football data from
2021–2025. The system addresses the scale challenges of analyzing
tens of thousands of plays across multiple seasons, demonstrating
distributed storage, parallel processing, and query optimization
techniques applicable to larger datasets.

Data Source
CollegeFootballData.org API — https://api.collegefootballdata.com/

Setup insturctions
Clone the repository to your local machine.
Navigate to the root directory of the project.
Run this command: pip install -r requirements.txt

Enviorment setup:
Navigate to the config directory
Create a copy of example.env and add your API keys:
CFB_API_KEY=example
AWS_ACCESS_KEY_ID=example
AWS_SECRET_ACCESS_KEY=example
AWS_DEFAULT_REGION=example
S3_BUCKET_NAME=example

How to run:
To test API and store in local and s3, run: python src/ingestion/test_ingestion.py

To test retrival from S3 run: python src/processing/test_pull.py

Current status:
Curently, API is authenticated and data retrival is possible from CollegeFootballData.org API. Data is stored locally and in AWS S3. Project directory is also setup. Batch processing pipeline: JSON ingestion to Parquet conversion to aggregation is still in development as well as MapReduce pattern for aggregations, and lazy evaluation with optimized DAG execution.
