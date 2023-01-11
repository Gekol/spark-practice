from pyspark import SparkConf, SparkContext


def main():
    conf = SparkConf().setMaster("local").setAppName("WordCount")
    sc = SparkContext(conf=conf)

    words = sc.textFile("./Book.txt").flatMap(lambda x: x.split())
    word_counts = words.countByValue()

    for word, count in word_counts.items():
        clean_word = word.encode("ascii", "ignore")
        if clean_word:
            print(f"{clean_word}\t{count}")


if __name__ == '__main__':
    main()
