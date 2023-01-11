from pyspark import SparkConf, SparkContext


def parse_line(line: str):
    values = line.split(",")
    age = int(values[2])
    friends = int(values[3])
    return age, friends


def main():
    conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
    sc = SparkContext(conf=conf)

    lines = sc.textFile("./fakefriends.csv")
    rdd = lines.map(parse_line)
    totals_by_age = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    averages_by_age = totals_by_age.mapValues(lambda x: x[0] / x[1])
    result = averages_by_age.collect()
    result.sort(key=lambda x: x[0])

    for line in result:
        print(line)


if __name__ == '__main__':
    main()
