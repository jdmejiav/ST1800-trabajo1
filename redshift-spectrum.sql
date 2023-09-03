create external schema global_land_temperature_by_major_city
from data catalog
database 'spectrum_temperature_db'
iam_role 'arn:aws:iam::085392363666:role/LabRole'
create external database if not exists;


CREATE external TABLE global_land_temperature_by_major_city.global_land_temperature_by_major_city (
    dt DATE,
    AverageTemperature DECIMAL(10, 3),
    AverageTemperatureUncertainty DECIMAL(10, 3),
    City VARCHAR(255),
    Country VARCHAR(255),
    Latitude VARCHAR(10),
    Longitude VARCHAR(10)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
location 's3://jdmejiav-datalake/raw-temperatures/';


SELECT
    *
FROM
    "dev"."global_land_temperature_by_major_city"."global_land_temperature_by_major_city";