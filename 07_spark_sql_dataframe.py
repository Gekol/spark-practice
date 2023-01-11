from core.session_manager import SessionManager


def main():
    with SessionManager("SparkSQLDataFrame") as spark:
        people = spark.read.option("header", "true").option("inferSchema", "true")\
            .csv("./fakefriends-header.csv")
        print("Schema")
        people.printSchema()

        print("Name column")
        people.select("name").show()

        print("People younger than 21")
        people.filter(people.age < 21).show()

        print("Group by age")
        people.groupBy("age").count().show()

        print("Make everyone 10 years older:")
        people.select(people.name, people.age + 10).show()


if __name__ == '__main__':
    main()
