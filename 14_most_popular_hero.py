from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as func

from core.session_manager import SessionManager


def main():
    with SessionManager("MostPopularHero") as spark:
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])
        names = spark.read.schema(schema).option("sep", " ").csv("./marvel_names.txt")
        lines = spark.read.text("./marvel_graph.txt")
        connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0])\
            .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1)\
            .groupBy("id").agg(func.sum("connections").alias("connections"))
        most_popular = connections.sort(func.col("connections").desc()).first()
        most_popular_name = names.filter(func.col("id") == most_popular.id).select("name").first()
        print(f"{most_popular_name.name} is the most popular superhero with {most_popular.connections} co-appearances.")


if __name__ == '__main__':
    main()
