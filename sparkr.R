spark_home = "/data/spark/spark-2.0.1-bin-hadoop2.7/"
Sys.setenv(SPARK_HOME=spark_home)

library(SparkR, lib.loc = file.path(spark_home,"R/lib"))

library(magrittr)
library(ggplot2)
library(gridExtra)
library(tidyr)

sparkR.session(
  master="local[*]", 
  sparkConfig=list(
    spark.executor.memory="16g", 
    spark.driver.memory="8g"
  )
)


## JSON 

reddit = read.df("/data/reddit/small.json", "json")

## Subreddit

reddit %>% 
  select("subreddit") %>%
  group_by("subreddit") %>%
  count() %>%
  arrange("count",decreasing=TRUE) %>%
  head(n=20)

## Author

tmp = reddit %>% 
  select("author") %>%
  filter(reddit$author != "[deleted]") %>%
  filter(reddit$author != "AutoModerator") %>%
  mutate(author_lower = lower(reddit$author))

tmp %>%
  filter(locate("bot",tmp$author_lower,1) == 0) %>%
  group_by("author") %>%
  count() %>%
  arrange("count",decreasing=TRUE) %>%
  head(n=20)

## Avg number of posts

tmp = reddit %>% 
  select("author") %>%
  group_by("author") %>%
  count() 

tmp %>%
  summarize(avg_posts = mean(tmp$count)) %>%
  collect()



### NYC Taxi 

taxi_colors = c("#009E73","#F0E442")

taxi_green  = read.df("/data/nyc-taxi-data/data/green_tripdata_2016-06.csv", "csv", 
                      header = "true", inferSchema = "true", na.strings = "NA")


taxi_yellow = read.df("/data/nyc-taxi-data/data/yellow_tripdata_2016-06.csv", "csv", 
                      header = "true", inferSchema = "true", na.strings = "NA") 

taxi_green = taxi_green %>%
  withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") %>%
  withColumnRenamed("Lpep_dropoff_datetime", "dropoff_datetime") %>%
  withColumnRenamed("Pickup_latitude", "lat") %>%
  withColumnRenamed("Pickup_longitude", "long")
  
taxi_yellow = taxi_yellow %>%
  withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") %>%
  withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") %>%
  withColumnRenamed("pickup_latitude", "lat") %>%
  withColumnRenamed("pickup_longitude", "long")


schema(taxi_green)
schema(taxi_yellow)


taxi_green %>% count()
taxi_yellow %>% count()


clean_locs = function(df)
{
  df %>%
    select("lat","long") %>%
    filter(df$lat > 40.477) %>%
    filter(df$lat < 40.917) %>%
    filter(df$long > -74.259) %>%
    filter(df$long < -73.700) %>%
    sample_frac(FALSE, 0.01) %>%
    collect()
}

taxi_pickups = rbind(
  cbind(taxi="green", taxi_green %>% clean_locs()),
  cbind(taxi="yellow", taxi_yellow %>% clean_locs())
)


ggplot(taxi_pickups, aes(x=long, y=lat, color=taxi)) + 
  geom_point(size=0.05, alpha=0.05) + 
  theme_bw() + 
  scale_color_manual(values=taxi_colors)



### Weekday and hour summary stats

wday_hourly_summary = function(df)
{
  tmp = df %>%
    mutate(
      hour = hour(df$pickup_datetime),
      wday = date_format(df$pickup_datetime, "EEE"),
      trip_time = (unix_timestamp(df$dropoff_datetime) -
                   unix_timestamp(df$pickup_datetime)) / 60
    ) %>%
    select("hour","wday","trip_time", "Trip_distance","Fare_amount","Tip_amount")
  
  tmp %>% 
    group_by("hour","wday") %>%
    summarize(
      avg_distance = mean(tmp$Trip_distance),
      avg_time = mean(tmp$trip_time),
      avg_fare = mean(tmp$Fare_amount),
      avg_tip_perc = mean(tmp$Tip_amount / tmp$Fare_amount)
    ) %>%
    collect()
}

taxi_wday_hourly = rbind(
  cbind(taxi = "green",  wday_hourly_summary(taxi_green)),
  cbind(taxi = "yellow", wday_hourly_summary(taxi_yellow))
) 

taxi_wday_hourly$wday=factor(taxi_wday_hourly$wday, 
                             labels = c("Sun","Mon","Tue","Wed","Thu","Fri","Sat"), 
                             ordered = TRUE)

taxi_wday_hourly_tidy = gather(taxi_wday_hourly, key = avg_type, value = avg_value, avg_distance:avg_tip_perc)


ggplot(taxi_wday_hourly_tidy, aes(x=hour, y=avg_value, color=taxi)) +
  geom_line() + 
  scale_colour_manual(values=taxi_colors) + 
  theme_bw() +
  facet_grid(avg_type~wday, scales="free_y") + 
  ylab("")



which_wday = "Wed"
which_hour = 16



df1 = taxi_yellow %>%
  mutate(
    hour = hour(taxi_yellow$pickup_datetime),
    wday = date_format(taxi_yellow$pickup_datetime, "EEE")
  ) %>%
  select("hour","wday","Fare_amount","Tip_amount")

df2 = df1 %>%
  filter(df1$hour == which_hour) %>%
  filter(df1$wday == which_wday) %>%
  mutate(tip_perc = df1$Tip_amount / df1$Fare_amount)
  
df2 %>% 
  summarize(avg_tip_perc1 = mean(df2$Tip_amount / df2$Fare_amount),
            avg_tip_perc2 = mean(df2$tip_perc)) %>%
  collect()
 
mean(collect(df2)$tip_perc,na.rm=TRUE)

taxi_wday_hourly %>% dplyr::filter(wday == which_wday, hour == which_hour)
