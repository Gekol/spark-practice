from pyspark.sql.types import StructType, StructField, IntegerType, LongType

from core.session_manager import SessionManager


def main():
    with SessionManager("PopularMovies") as spark:
        schema = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("movie_id", IntegerType(), True),
            StructField("rating", IntegerType(), True),
            StructField("timestamp", LongType(), True),
        ])

        ratings = spark.read.option("sep", "\t").schema(schema).csv("./ml-100k/u.data")
        results = ratings.groupBy("movie_id").count().sort("count", ascending=False)
        results.show(10)


if __name__ == '__main__':
    main()
