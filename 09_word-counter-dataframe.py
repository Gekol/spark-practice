from core.session_manager import SessionManager
from pyspark.sql import functions as func


def main():
    with SessionManager("WordCounterDataFrame") as spark:
        text = spark.read.text("./Book.txt")
        words = text.select(func.explode(func.split(text.value, "\\W+")).alias("word"))
        words.filter(words.word != "")

        lower_case_words = words.select(func.lower(words.word).alias("word"))
        word_counts = lower_case_words.groupBy("word").count().sort("count")
        word_counts.show(word_counts.count())


if __name__ == '__main__':
    main()
