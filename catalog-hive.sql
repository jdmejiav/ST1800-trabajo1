CREATE EXTERNAL TABLE climate_change ( 
    CountryCode STRING, 
    CountryName STRING, 
    SeriesCode STRING, 
    SeriesName STRING, 
    SCALE INT, 
    Decimals INT, 
    `1990` DOUBLE, 
    `1991` DOUBLE, 
    `1992` DOUBLE, 
    `1993` DOUBLE, 
    `1994` DOUBLE, 
    `1995` DOUBLE, 
    `1996` DOUBLE, 
    `1997` DOUBLE
     `1998` DOUBLE
     `1999` DOUBLE
     `2000` DOUBLE
     `2001` DOUBLE
     `2002` DOUBLE
     `2003` DOUBLE
     `2004` DOUBLE
     `2005` DOUBLE
     `2006` DOUBLE
     `2007` DOUBLE
     `2008` DOUBLE
     `2009` DOUBLE
     `2010` DOUBLE, 
     `2011` DOUBLE ) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE 
LOCATION '/user/hadoop/datos_csv';  
LOAD DATA INPATH '/user/hadoop/climate_change_download_0.csv' 
INTO TABLE climate_change; 


 SELECT * 
 FROM temperatures_hive_catalog.climate_change 
 LIMIT 100 ; 