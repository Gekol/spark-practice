from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from pyspark.sql import functions as func
import codecs

from core.session_manager import SessionManager


def load_movie_names():
    movie_names = {}
    with codecs.open("./ml-100k/u.item", "r", encoding="ISO-8859-1", errors="ignore") as file:
        for line in file:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1]
    return movie_names


def get_look_up_name_func(names):
    def look_up_name(movie_id):
        return names.value[movie_id]

    return look_up_name


def main():
    with SessionManager("PopularMoviesWithBroadcast") as spark:
        schema = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("movie_id", IntegerType(), True),
            StructField("rating", IntegerType(), True),
            StructField("timestamp", LongType(), True),
        ])
        names = spark.sparkContext.broadcast(load_movie_names())

        ratings = spark.read.option("sep", "\t").schema(schema).csv("./ml-100k/u.data")
        counts = ratings.groupBy("movie_id").count().sort("count", ascending=False)
        look_up_name_udf = func.udf(get_look_up_name_func(names))

        results = counts.withColumn("movie_title", look_up_name_udf(func.col("movie_id")))\
            .select("movie_title", "count").sort("count", ascending=False)

        results.show(10)


if __name__ == '__main__':
    main()
