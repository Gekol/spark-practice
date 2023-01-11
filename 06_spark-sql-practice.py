from pyspark.sql import Row

from core.session_manager import SessionManager


def mapper(line: str):
    fields = line.split(",")
    return Row(id=int(fields[0]), name=str(fields[1].encode("utf-8")), age=int(fields[2]), num_friends=int(fields[3]))


def main():
    with SessionManager("SparkSQL") as spark:
        lines = spark.sparkContext.textFile("./fakefriends.csv")
        people = lines.map(mapper)

        schema = spark.createDataFrame(people).cache()
        schema.createOrReplaceTempView("people")

        teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 and age <= 19")

        for teenager in teenagers.collect():
            print(teenager)

        schema.groupBy("age").count().orderBy("age").show()


if __name__ == '__main__':
    main()
