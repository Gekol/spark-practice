from core.session_manager import SessionManager
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


def main():
    with SessionManager("MinTemperaturesDataFrame") as spark:
        schema = StructType([
            StructField("station_id", StringType(), True),
            StructField("date", IntegerType(), True),
            StructField("measure_type", StringType(), True),
            StructField("temperature", FloatType(), True)
        ])

        df = spark.read.schema(schema).csv("./1800.csv")

        min_temperatures = df.filter(df.measure_type == "TMIN")

        station_temperatures = min_temperatures.groupBy("station_id").min("temperature")

        min_station_temperatures = station_temperatures.withColumn("temperature", func.round(
            func.col("min(temperature)") * 0.1 * (9 / 5) + 32, 2)).select("station_id", "temperature")\
            .sort("temperature")

        min_station_temperatures.show()


if __name__ == '__main__':
    main()
