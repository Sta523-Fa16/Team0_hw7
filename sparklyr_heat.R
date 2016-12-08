spark_home = "/data/spark/spark-2.0.1-bin-hadoop2.7/"
Sys.setenv(SPARK_HOME=spark_home)

library(sparklyr)
library(dplyr)
library(ggplot2)
library(tidyr)

sc = spark_connect(master = "local", version="2.0.1")

green  = spark_read_csv(sc, "green", "/data/nyc-taxi-data/data/green_tripdata_2016-06.csv")
yellow = spark_read_csv(sc, "yellow", "/data/nyc-taxi-data/data/yellow_tripdata_2016-06.csv")

fix_names = function(df)
{
  df %>%
    setNames(
      colnames(df) %>% 
        tolower() %>% 
        sub("[tl]pep_","",.) 
    )
}

green  = green  %>% fix_names()
yellow = yellow %>% fix_names()

green_heat = 
  green %>% 
  transmute(long = as.integer(pickup_longitude * 1e3), 
            lat  = as.integer(pickup_latitude  * 1e3)) %>% 
  filter(lat > 40477, lat < 40917, long > -74259, long < -73700) %>%
  group_by(long, lat) %>%
  count() %>%
  collect()

yellow_heat = 
  yellow %>% 
  transmute(long = as.integer(pickup_longitude * 1e3), 
            lat  = as.integer(pickup_latitude  * 1e3)) %>% 
  filter(lat > 40477, lat < 40917, long > -74259, long < -73700) %>%
  group_by(long, lat) %>%
  count() %>%
  collect()

plot(green_heat$long, green_heat$lat, pch=16, cex=0.2)
points(yellow_heat$long, yellow_heat$lat, pch=16, cex=0.2, col='blue')
