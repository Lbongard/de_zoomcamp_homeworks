# Q1
CREATE MATERIALIZED VIEW trip_time_by_zones as (
SELECT taxi_zone.Zone as pickup_zone, taxi_zone_1.Zone as dropoff_zone, AVG(tpep_dropoff_datetime - tpep_pickup_datetime) as avg_trip_time
FROM trip_data
JOIN taxi_zone ON trip_data.PULocationID = taxi_zone.location_id
JOIN taxi_zone as taxi_zone_1 ON trip_data.DOLocationID = taxi_zone_1.location_id
GROUP BY 1, 2
ORDER BY 1, 2);


SELECT pickup_zone, dropoff_zone, avg_trip_time 
FROM trip_time_by_zones
ORDER BY avg_trip_time DESC
LIMIT 1;


# Q2

CREATE MATERIALIZED VIEW trip_time_and_counts_by_zones as (
SELECT taxi_zone.Zone as pickup_zone, 
	   taxi_zone_1.Zone as dropoff_zone, 
	   AVG(tpep_dropoff_datetime - tpep_pickup_datetime) as avg_trip_time,
	   count(*) as trip_count
FROM trip_data
JOIN taxi_zone ON trip_data.PULocationID = taxi_zone.location_id
JOIN taxi_zone as taxi_zone_1 ON trip_data.DOLocationID = taxi_zone_1.location_id
GROUP BY 1, 2
ORDER BY 1, 2);


SELECT pickup_zone, dropoff_zone, avg_trip_time , trip_count
FROM trip_time_and_counts_by_zones
ORDER BY avg_trip_time DESC
LIMIT 1;


# Q3

SELECT taxi_zone.Zone as pickup_zone, count(*) pickup_count
FROM trip_data
JOIN taxi_zone ON trip_data.PULocationID = taxi_zone.location_id
WHERE tpep_pickup_datetime > (select max(tpep_pickup_datetime) from trip_data) - interval '17 hours'
GROUP BY taxi_zone.Zone
ORDER BY pickup_count DESC
limit 3;

