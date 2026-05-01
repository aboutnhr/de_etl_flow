from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
import pyspark.sql.functions as F

plants = StructType([
    StructField("plantId", StringType()),
    StructField("temperature", DoubleType())
])

class PySparkJob:

    def init_spark_session(self) -> SparkSession:
        return (
            SparkSession.builder
            .appName("ManufacturingFaultDetection")
            .getOrCreate()
        )

    def read_csv(self, input_path: str) -> DataFrame:
        spark = self.init_spark_session()
        #Try reading with header first
        df = (
            spark.read.option("header", "true").csv(input_path)
        )
        # If header is missing, Spark will name columns _c0, _c1
        # Detect that and re-read with schema + header=false
        if "_c0" in df.columns or "plantId" not in df.columns:
            df = (
                spark.read
                .option("header", "false")
                .schema(plants)
                .csv(input_path)
            )
        else:
            # Header exists → apply schema manually
            df = df.toDF("plantId", "temperature").selectExpr("cast(plantId as string)", "cast(temperature as double)")
        return df

    def calc_average_temperature(self, observed: DataFrame) -> DataFrame:
        return (
            observed.groupBy("plantId")
            .agg(F.avg("temperature").alias("avg_observed_temp"))
        )

    def find_faulty_plants(self, avg_observed: DataFrame, required: DataFrame) -> DataFrame:
        joined = avg_observed.join(required, on="plantId")

        # difference = |avg_observed_temp - required_temperature|
        faulty = joined.withColumn(
            "difference",
            F.abs(F.col("avg_observed_temp") - F.col("temperature"))
        ).filter(F.col("difference") >= 5)

        # final output: plantId, avg_observed_temp
        return faulty.select(
            "plantId",
            F.col("avg_observed_temp").alias("temperature")
        )

    def save_as(self, data: DataFrame, output_path: str) -> None:
        (
            data.coalesce(1)
            .write.mode("overwrite")
            .option("header", "false")
            .csv(output_path)
        )
