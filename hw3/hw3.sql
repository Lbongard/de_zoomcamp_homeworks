CREATE OR REPLACE EXTERNAL TABLE `inbound-ship-412204.ny_taxi.green_cab_data_2022_external`
OPTIONS (
  format ="PARQUET",
  uris = ['gs://mage_zoomcamp_reid/green_cab_data_2022/green_tripdata_2022-01.parquet',
          'gs://mage_zoomcamp_reid/green_cab_data_2022/green_tripdata_2022-02.parquet',
          'gs://mage_zoomcamp_reid/green_cab_data_2022/green_tripdata_2022-03.parquet',
          'gs://mage_zoomcamp_reid/green_cab_data_2022/green_tripdata_2022-04.parquet',
          'gs://mage_zoomcamp_reid/green_cab_data_2022/green_tripdata_2022-05.parquet',
          'gs://mage_zoomcamp_reid/green_cab_data_2022/green_tripdata_2022-06.parquet',
          'gs://mage_zoomcamp_reid/green_cab_data_2022/green_tripdata_2022-07.parquet',
          'gs://mage_zoomcamp_reid/green_cab_data_2022/green_tripdata_2022-08.parquet',
          'gs://mage_zoomcamp_reid/green_cab_data_2022/green_tripdata_2022-09.parquet',
          'gs://mage_zoomcamp_reid/green_cab_data_2022/green_tripdata_2022-10.parquet',
          'gs://mage_zoomcamp_reid/green_cab_data_2022/green_tripdata_2022-11.parquet',
          'gs://mage_zoomcamp_reid/green_cab_data_2022/green_tripdata_2022-12.parquet']);


CREATE OR REPLACE TABLE `inbound-ship-412204.ny_taxi.green_cab_data_2022` 
  AS
  SELECT * FROM inbound-ship-412204.ny_taxi.green_cab_data_2022_external;


CREATE OR REPLACE TABLE `inbound-ship-412204.ny_taxi.green_cab_data_2022_partitioned` 
  PARTITION BY DATE(lpep_pickup_datetime)
  CLUSTER BY PUlocationID
  AS
  SELECT * FROM inbound-ship-412204.ny_taxi.green_cab_data_2022_external;

SELECT COUNT(DISTINCT PUlocationID)
FROM `inbound-ship-412204.ny_taxi.green_cab_data_2022`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

SELECT COUNT(DISTINCT PUlocationID)
FROM `inbound-ship-412204.ny_taxi.green_cab_data_2022_partitioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

SELECT COUNT(*)
FROM `inbound-ship-412204.ny_taxi.green_cab_data_2022`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
