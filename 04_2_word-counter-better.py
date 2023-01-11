from pyspark import SparkConf, SparkContext
import re


def parse_words(line: str):
    return re.compile(r"\W+", re.UNICODE).split(line.lower())


def main():
    conf = SparkConf().setMaster("local").setAppName("WordCountBetter")
    sc = SparkContext(conf=conf)

    words = sc.textFile("./Book.txt").flatMap(parse_words)
    word_counts = words.countByValue()
    for word, count in sorted(word_counts.items(), key=lambda x: x[1]):
        clean_word = word.encode("ascii", "ignore")
        if clean_word:
            print(f"{clean_word}\t{count}")


if __name__ == '__main__':
    main()
