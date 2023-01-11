from core.session_manager import SessionManager
from pyspark.sql import functions as func


def main():
    with SessionManager("FriendsByAgeDataFrame") as spark:
        people = spark.read.option("header", "true").option("inferSchema", "true") \
            .csv("./fakefriends-header.csv")

        friends_by_age = people.select("age", "friends")
        average_friends_count = friends_by_age.groupBy("age")\
            .agg(func.round(func.avg("friends"), 2).alias("average_friends_count")).sort("age")
        average_friends_count.show()


if __name__ == '__main__':
    main()
