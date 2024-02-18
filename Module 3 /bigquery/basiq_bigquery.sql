-- Creating external table referring to gcs path 
CREATE or REPLACE EXTERNAL TABLE `de-zoomcamp-412301.ny_taxi.external_yellow_tripdata`
OPTIONS( format = 'parquet',
uris = ['gs://module-3-zoomcamp/yellow/yellow_tripdata_2020-*.parquet',
    'gs://module-3-zoomcamp/yellow/yellow_tripdata_2019-*.parquet']);
    