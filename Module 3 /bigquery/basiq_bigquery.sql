-- Creating external table referring to gcs path 
CREATE or REPLACE EXTERNAL TABLE `de-zoomcamp-412301.ny_taxi.external_yellow_tripdata`
OPTIONS( format = 'parquet',
uris = ['gs://module-3-zoomcamp/yellow/yellow_tripdata_2020-*.parquet',
    'gs://module-3-zoomcamp/yellow/yellow_tripdata_2019-*.parquet']);


-- change the datetime to TIMESTAMP
    CREATE OR REPLACE TABLE de-zoomcamp-412301.ny_taxi.external_yellow_tripdata_corrected AS 
    SELECT *, PARSE_DATETIME('%Y-%m-%d %H:%M:%S', tpep_pickup_datetime) AS tpep_pickup_datetime
    FROM de-zoomcamp-412301.ny_taxi.external_yellow_tripdata;

-- select 10 rows to see 
SELECT * FROM `ny_taxi.external_yellow_tripdata_corrected`
ORDER BY pickup_date  ASC
LIMIT 500 ;


-- Create a no partitioned table from external table 
CREATE OR REPLACE TABLE de-zoomcamp-412301.ny_taxi.external_yellow_tripdata_corrected AS
SELECT * FROM de-zoomcamp-412301.ny_taxi.external_yellow_tripdata_corrected;

--Create a partitioned table from external table 
CREATE OR REPLACE TABLE de-zoomcamp-412301.ny_taxi.yellow_tripdata_partitoned
PARTITION BY 
    DATE(pickup_date) AS
    SELECT * FROM de-zoomcamp-412301.ny_taxi.external_yellow_tripdata_corrected;