from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, IntegerType, StringType, StructType, StructField, DoubleType
from datetime import datetime

class ChargePointsETLJob:
    input_path = 'gs://hadoop-bigdata-cluster/inbound/chargingpointdata.csv'
    #output_path = 'gs://hadoop-bigdata-cluster/output/chargingpointdata_output'
    output_table = 'hanumanth-gcp-pde-492305.test_data.chargingpointdata_output'
    temp_bucket = 'hadoop-bigdata-cluster'

    def __init__(self):
        self.spark_session = (SparkSession.builder
                              .master("local[*]")
                              .appName("ElectricChargePointsETLJob")
                              .getOrCreate())

    def extract(self):
        schema = StructType([
            StructField("ChargingEvent", IntegerType(), True),
            StructField("CPID", StringType(), True),
            StructField("StartDate", StringType(), True),
            StructField("StartTime", StringType(), True),
            StructField("EndDate", StringType(), True),
            StructField("EndTime", StringType(), True),
            StructField("Energy", DoubleType(), True),
            StructField("PlugInDuration", IntegerType(), True)
        ])
        df = self.spark_session.read.option("header", True).schema(schema).csv(self.input_path)
        return df

    def transform(self, df):
        # Adjust column names if needed based on actual dataset
        # df = df.withColumn(
        #     "start_ts",
        #     F.to_timestamp(F.concat_ws(" ", F.col("StartDate"), F.col("StartTime")),"yyyy-MM-dd HH:mm:ss")
        # ).withColumn(
        #     "end_ts",
        #     F.to_timestamp(F.concat_ws(" ", F.col("EndDate"), F.col("EndTime")),"yyyy-MM-dd HH:mm:ss")
        # )

        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y %H:%M"
        ]

        def parse_datetime(date_str, time_str):
            if date_str is None or time_str is None:
                return None
            dt_str = f"{date_str} {time_str}"
            for fmt in formats:
                try:
                    return datetime.strptime(dt_str, fmt)
                except:
                    continue
            return None

        parse_udf = F.udf(parse_datetime, TimestampType())

        df = df.withColumn("start_ts", parse_udf(F.col("StartDate"), F.col("StartTime")))
        df = df.withColumn("end_ts", parse_udf(F.col("EndDate"), F.col("EndTime")))
        print("print start_ts, end_ts")
        df.show()

        # start_ts = F.coalesce(
        #     F.to_timestamp(F.concat_ws(' ', F.col("StartDate"), F.col("StartTime")), "yyyy-MM-dd HH:mm:ss"),
        #     F.to_timestamp(F.concat_ws(' ', F.col("StartDate"), F.col("StartTime")), "dd/MM/yyyy HH:mm:ss"),
        #     F.to_timestamp(F.concat_ws(' ', F.col("StartDate"), F.col("StartTime")), "yyyy-MM-dd HH:mm"),
        #     F.to_timestamp(F.concat_ws(' ', F.col("StartDate"), F.col("StartTime")), "dd/MM/yyyy HH:mm")
        # )

        # # Parse end timestamp with multiple formats
        # end_ts = F.coalesce(
        #     F.to_timestamp(F.concat_ws(' ', F.col("EndDate"), F.col("EndTime")), "yyyy-MM-dd HH:mm:ss"),
        #     F.to_timestamp(F.concat_ws(' ', F.col("EndDate"), F.col("EndTime")), "dd/MM/yyyy HH:mm:ss"),
        #     F.to_timestamp(F.concat_ws(' ', F.col("EndDate"), F.col("EndTime")), "yyyy-MM-dd HH:mm"),
        #     F.to_timestamp(F.concat_ws(' ', F.col("EndDate"), F.col("EndTime")), "dd/MM/yyyy HH:mm")
        # )

        df = df.withColumn("duration", (F.col("end_ts").cast("long") - F.col("start_ts").cast("long")) / 60.0)
        print("duration")
        df.show()

        # Aggregate per chargepoint
        result_df = df.groupBy("CPID").agg(
        F.round(F.max("duration"), 2).alias("max_duration"),
        F.round(F.avg("duration"), 2).alias("avg_duration")).withColumnRenamed("CPID", "chargepoint_id")
        result_df.show()

        return result_df

    def load(self, df):
        #(df.write
        #   .mode("overwrite")
        #   .option("header", True)
        #   .csv(self.output_path))
        (df.write
           .format("bigquery")
           .option("table", self.output_table)
           .option("temporaryGcsBucket", self.temp_bucket)
           .mode("overwrite")   # or "append"
           .save())
    
if __name__ == "__main__":
    job = ChargePointsETLJob()
    raw_df = job.extract()
    
    # Step 2: Transform
    transformed_df = job.transform(raw_df)
    
    # Step 3: Load
    job.load(transformed_df)
    
    print("ETL job completed successfully!")
