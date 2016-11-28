spark_home = "/data/spark/spark-2.0.1-bin-hadoop2.7/"
Sys.setenv(SPARK_HOME=spark_home)

library(sparklyr)
library(dplyr)
library(ggplot2)
library(tidyr)

sc = spark_connect(master = "local", version="2.0.1")

## Send df to spark 
si = copy_to(sc, iris, "spark_iris")

si %>% 
  group_by(Species) %>% 
  summarize(
    avg_sl = mean(Sepal_Length),
    avg_sw = mean(Sepal_Width)
  ) %>%
  collect()
  


## Taxi

### NYC Taxi 
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


### Weekday and hour summary stats

clean_up = function(df)
{
  df %>%
    mutate(
      hour = hour(pickup_datetime),
      wday = date_format(pickup_datetime, "EEE"),
      trip_time = (unix_timestamp(dropoff_datetime) -
                     unix_timestamp(pickup_datetime)) / 60
    ) %>%
    select(hour,wday,trip_time, trip_distance,fare_amount,tip_amount)
}

wday_hourly_summary = function(df, label="")
{
  taxi_type = deparse(substitute(df))
  df %>% 
    group_by(hour,wday) %>%
    summarize(
      avg_distance = mean(trip_distance),
      avg_time     = mean(trip_time),
      avg_fare     = mean(fare_amount),
      avg_tip_perc = mean(tip_amount/fare_amount)
    ) %>%
    mutate(taxi = label) %>%
    collect()
}

taxi_wday_hourly = rbind(
                     green  %>% clean_up() %>% wday_hourly_summary("green"),
                     yellow %>% clean_up() %>% wday_hourly_summary("yellow")
                   )

taxi_wday_hourly$wday=factor(taxi_wday_hourly$wday, 
                             labels = c("Sun","Mon","Tue","Wed","Thu","Fri","Sat"), 
                             ordered = TRUE)

taxi_wday_hourly_tidy = gather(taxi_wday_hourly, 
                               key = avg_type, 
                               value = avg_value, 
                               avg_distance:avg_tip_perc)


ggplot(taxi_wday_hourly_tidy, aes(x=hour, y=avg_value, color=taxi)) +
  geom_line() + 
  scale_colour_manual(values=c("#009E73","#F0E442")) + 
  theme_bw() +
  facet_grid(avg_type~wday, scales="free") + 
  ylab("")



### Using SQL interface

library(DBI)

dbGetQuery(sc, "SELECT * FROM green LIMIT 10")
dbGetQuery(sc, "SELECT COUNT(*) FROM green GROUP BY ratecodeid")
dbGetQuery(sc, "SELECT date_format(pickup_datetime, 'EEE') AS wday, pickup_datetime FROM green LIMIT 10")
dbGetQuery(sc, "SELECT date_format(pickup_datetime, 'EEE') AS wday, pickup_datetime FROM green ORDER BY wday LIMIT 10")

green_clean  = green  %>% clean_up() %>% sdf_register("green_clean")
yellow_clean = yellow %>% clean_up() %>% sdf_register("yellow_clean")
 
dbGetQuery(sc, "SELECT wday FROM green_clean LIMIT 10")


taxi_wday_hourly %>% ungroup() %>% filter(avg_tip_perc == max(avg_tip_perc))

tips_dplyr = yellow_clean %>% filter(wday == "Tue", hour == 16) %>% collect()
tips = dbGetQuery(sc, "SELECT mean(tip_amount/fare_amount) AS tip_perc_avg FROM yellow_clean WHERE wday = 'Tue' AND hour = 16")
