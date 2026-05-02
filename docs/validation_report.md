# Validation Report

## Data Quality Metrics

| Metric | Value | Notes |
|---|---|---|
| Total plays ingested | 151,344 | 14 SEC teams x 5 seasons |
| Game stat rows produced | 1,158 | Output of MapReduce aggregation |
| Schema columns enforced | 27 | Explicit typed schema in Stage 2 |
| Seasons covered | 5 | 5 complete seasons |
| Teams converted | 14 | Texas and Oklahoma excluded because they joined in 2024 |
| Corrupt / rejected records | 0 | Logged to `corrupt_record` column |
| Null rate | 25.11% | Kickoffs and returns have null `ppa` by design |
| Duplicate records | 0 | S3 is checked during ingestion to prevent duplicates |
| Parquet compression ratio | 9.35x | Snappy vs raw JSON |

## Validation Summary

The pipeline successfully ingested and processed the expected SEC play by play dataset for 14 teams across 5 complete seasons. Stage 2 enforced the typed play schema during JSON to Parquet conversion, and Stage 3 produced game-level offensive statistics through the MapReduce aggregation job.

No corrupt, rejected, or duplicate records were reported. The observed null rate is expected because some play categories, especially kickoffs and returns, do not receive `ppa` values from the source data. The final Snappy Parquet output achieved a 9.35x compression ratio compared with the raw JSON inputs.

## Validation Checks

| Check | Result | Evidence |
|---|---|---|
| Ingestion completeness | Pass | 151,344 total plays ingested |
| Schema enforcement | Pass | 27 Stage 2 schema columns enforced |
| Aggregation output | Pass | 1,158 game stat rows produced |
| Corrupt record handling | Pass | 0 corrupt / rejected records |
| Duplicate prevention | Pass | 0 duplicate records |
| Storage optimization | Pass | 9.35x Parquet compression ratio |

