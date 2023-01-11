from pyspark import SparkConf, SparkContext
import re


def parse_words(line: str):
    return re.compile(r"\W+", re.UNICODE).split(line.lower())


def main():
    conf = SparkConf().setMaster("local").setAppName("WordCountSorted")
    sc = SparkContext(conf=conf)

    words = sc.textFile("./Book.txt").flatMap(parse_words)
    word_counts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
    sorted_word_counts = word_counts.map(lambda x: (x[1], x[0])).sortByKey()
    for count, word in sorted_word_counts.collect():
        clean_word = word.encode("ascii", "ignore")
        if clean_word:
            print(f"{clean_word}\t{count}")


if __name__ == '__main__':
    main()
