from pyspark.sql import DataFrame
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys
import time

from core.session_manager import SessionManager


def compute_cosine_similarity(data: DataFrame):
    pair_scores = data \
        .withColumn("xx", func.col("rating1") * func.col("rating1")) \
        .withColumn("yy", func.col("rating2") * func.col("rating2")) \
        .withColumn("xy", func.col("rating1") * func.col("rating2"))

    calculate_similarity = pair_scores \
        .groupBy("movie1", "movie2") \
        .agg(func.sum(func.col("xy")).alias("numerator"),
             (func.sqrt(func.sum(func.col("xx"))) * func.sqrt(func.sum(func.col("yy")))).alias("denominator"),
             func.count(func.col("xy")).alias("num_pairs")
             )

    result = calculate_similarity \
        .withColumn("score", func.when(func.col("denominator") != 0,
                                       func.col("numerator") / func.col("denominator")
                                       ).otherwise(0)
                    ).select("movie1", "movie2", "score", "num_pairs")

    return result


def get_movie_name(movie_id, names):
    return names.filter(func.col("movie_id") == movie_id).select("movie_title").collect()[0].movie_title


def main():
    with SessionManager("MovieSimilaritiesDataframe") as spark:
        start = time.perf_counter()
        movie_names_schema = StructType([
            StructField("movie_id", IntegerType(), True),
            StructField("movie_title", StringType(), True)
        ])

        movies_schema = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("movie_id", IntegerType(), True),
            StructField("rating", IntegerType(), True),
            StructField("timestamp", LongType(), True),
        ])

        movie_names = spark.read \
            .option("sep", "|") \
            .option("charset", "ISO-8859-1") \
            .schema(movie_names_schema) \
            .csv("./ml-100k/u.item")

        movies = spark.read \
            .option("sep", "\t") \
            .schema(movies_schema) \
            .csv("./ml-100k/u.data")

        ratings = movies.select("user_id", "movie_id", "rating")

        average_ratings = ratings.groupBy("movie_id").agg(func.round(func.avg("rating"), 2).alias("average_rating"))

        movie_pairs = ratings.alias("ratings1") \
            .join(ratings.alias("ratings2"), (func.col("ratings1.user_id") == func.col("ratings2.user_id"))
                  & (func.col("ratings1.movie_id") < func.col("ratings2.movie_id"))) \
            .select(func.col("ratings1.movie_id").alias("movie1"), func.col("ratings2.movie_id").alias("movie2"),
                    func.col("ratings1.rating").alias("rating1"), func.col("ratings2.rating").alias("rating2"),
                    )

        movie_pair_similarities = compute_cosine_similarity(movie_pairs).cache()

        if len(sys.argv) > 1:
            score_threshold = 0.97
            co_occurrence_threshold = 50
            min_rating = 4

            movie_id = int(sys.argv[1])

            filtered_results = movie_pair_similarities.filter(
                ((func.col("movie1") == movie_id) | (func.col("movie2") == movie_id)) &
                (func.col("score") > score_threshold) & (func.col("num_pairs") > co_occurrence_threshold)
            )

            results_with_ratings = filtered_results.alias("movies").join(
                average_ratings.alias("average_ratings"), (
                        ((func.col("movies.movie1") == func.col("average_ratings.movie_id")) &
                         (func.col("movies.movie1") != movie_id)) |
                        ((func.col("movies.movie2") == func.col("average_ratings.movie_id")) &
                         (func.col("movies.movie2") != movie_id))
                )
            ).select(
                func.col("movies.movie1"),
                func.col("movies.movie2"),
                func.col("movies.score"),
                func.col("movies.num_pairs"),
                func.col("average_ratings.average_rating")
            ).cache()

            filtered_results = results_with_ratings.filter(func.col("average_rating") >= min_rating)

            sorted_results = filtered_results.sort(
                func.col("score").desc(), func.col("average_rating")
            )

            results = spark.createDataFrame(sorted_results.head(20))  # .take(20)

            named_results = results.alias("movies") \
                .join(movie_names.alias("names"),
                      ((func.col("movies.movie1") == func.col("names.movie_id")) &
                       (func.col("movies.movie1") != movie_id)) |
                      ((func.col("movies.movie2") == func.col("names.movie_id")) &
                       (func.col("movies.movie2") != movie_id))
                      ).select("movie_title", "score", "num_pairs", "average_rating")

            print(f"Top 20 similar movies for {get_movie_name(movie_id, movie_names)}")

            named_results.show(20)

            # for result in results:
            #     # Display the similarity result that isn't the movie we're looking at
            #     similar_movie_id = result.movie1
            #     if similar_movie_id == movie_id:
            #         similar_movie_id = result.movie2
            #
            #     print(f"name - {get_movie_name(similar_movie_id, movie_names)}"
            #           f"\tscore - {result.score}"
            #           f"\tnum_pairs - {result.num_pairs}"
            #           f"\taverage_rating - {result.average_rating}")

            end = time.perf_counter()
            print(end - start)


if __name__ == '__main__':
    main()
