from pyspark.sql.types import DateType
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit, col, current_timestamp, input_file_name, to_timestamp
from notebookutils import mssparkutils
import pandas as pd



def TableExists(TableName, Source):
    exists = mssparkutils.fs.exists(f'abfss://BI_Fabric_Development@onelake.dfs.fabric.microsoft.com/LH_Bronze.Lakehouse/Tables/{Source}_{TableName}')
    return exists




def addWaterMark(TableName):
    df = spark.sql("SELECT * FROM LH_Stage.source_control_table")
    df = df.withColumn('Last_run', when(col('Destination_table')== Destination_table, lit(WaterMark) ) ) # .otherwise('null')
    print(df)

    df.write.mode("overwrite").format("delta").save("Tables/source_control_table")


def WriteToPath(SourcePath, DataSource, Mode, SinkPath):
    df = spark.read.parquet(SourcePath) \
    .withColumn("DataSource", lit(DataSource)) \
    .withColumn("RowStartDate", current_timestamp()) \
    .withColumn("RowEndDate", to_timestamp(lit("9999-12-31"), 'yyyy-MM-dd'))
    df.write.format('delta').mode(Mode).save(SinkPath)