from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as func

from core.session_manager import SessionManager


def main():
    with SessionManager("MostObscureHero") as spark:
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])
        names = spark.read.schema(schema).option("sep", " ").csv("./marvel_names.txt")
        lines = spark.read.text("./marvel_graph.txt")
        connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
            .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
            .groupBy("id").agg(func.sum("connections").alias("connections"))
        min_connections_count = connections.agg(func.min("connections")).first()[0]
        most_obscure = connections.filter(func.col("connections") == min_connections_count)
        most_obscure_with_names = most_obscure.join(names, on=names.id == most_obscure.id, how="inner")\
            .sort(func.col("name")) \
            .select(most_obscure.id, names.name, most_obscure.connections)
        most_obscure_with_names.show(most_obscure.count())


if __name__ == '__main__':
    main()
