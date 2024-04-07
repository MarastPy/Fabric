import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit, year, month, quarter, dayofmonth, dayofweek, dayofyear, weekofyear, to_date, expr, current_date, date_format, date_sub
from pyspark.sql import functions as F

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

table_name = "Dim_date"

# Initialize a Spark session
spark = SparkSession.builder.appName("DateDimension").getOrCreate()

# Create a list of dates from 2021-01-01 to 2022-12-31
start_date = "2024-01-01"
end_date = "2024-12-31"
num_days = spark.sql("SELECT datediff('{}', '{}')".format(end_date, start_date)).collect()[0][0]
date_range = spark.range(num_days).toDF("offset")
date_range = date_range.withColumn("date", expr("date_add('{}', cast(offset as int))".format(start_date)))

# Extract year, month, day, day_of_week, day_of_year, and so on
date_dim = date_range.withColumn("date", to_date(col("date"))) \
    .select(col("offset").alias("Date_key"),
            col("date").alias("Date"),
            year(col("date")).alias("Year"),
            quarter(col("date")).alias("Quarter"),
            month(col("date")).alias("Month"),
            weekofyear(col("date")).alias("Week_of_year"),
            date_format(col("Date"), "w").cast("int").alias("ISO_Week_Number"),
            when(weekofyear(col("date")) == weekofyear(current_date())-1, 1).otherwise(lit(0)).cast('int').alias("LastWeek"),
            when(weekofyear(col("date")) <= weekofyear(current_date())-1, 1).otherwise(lit(0)).cast('int').alias("LastWeekPlus"),
            dayofmonth(col("date")).cast("string").alias("Day"),
            F.date_format(col("date"), 'EEEE').alias("DayName"),
            when(col("date") <= current_date(), 0).otherwise(lit(1)).cast('int').alias("FutureDate"),
            dayofweek(col("date")).alias("DayOfWeek"),
            # Calculate fiscal year starting on May 1
            when(month(col("date")) >= 4, year(col("date"))).otherwise(year(col("date")) - 1).alias("FiscalYear"),
            when(month(col("Date")) >= 4, quarter(col("Date")) -1).otherwise(quarter(col("Date")) +3).alias("FiscalQuarter"),
            when(month(col("Date")) >= 4, month(col("Date")) -3).otherwise(month(col("Date")) + 9).alias("FiscalMonth"),
            )

# Show the first few rows of the date dimension using show()
date_dim.show(90)
date_dim.printSchema()
date_dim.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(f"Tables/{table_name}")


# Stop the Spark session
#spark.stop()