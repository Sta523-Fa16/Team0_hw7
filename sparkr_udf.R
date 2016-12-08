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


x = head(reddit)

as.POSIXct(x$created_utc[1])

library(lubridate)

get_datetime = function(x)
{
  library(magrittr)
  library(lubridate)
  
  dt = x$created_utc[1:2] %>% 
    as.numeric() %>% 
    as.POSIXct(origin="1970-1-1", tz="UTC")
  
  cbind(
    x, 
    data.frame(
      day = day(dt),
      month = month(dt),
      year = year(dt),
      hour = hour(dt),
      minute = minute(dt),
      second = second(dt)
    )
  )
}

schema = structType(structField("day","integer"),
                    structField("month","double"),
                    structField("year","double"),
                    structField("hour","integer"),
                    structField("minute","integer"),
                    structField("second","double"))

(r = dapply(reddit, get_datetime, schema))

cbind(reddit,r)
