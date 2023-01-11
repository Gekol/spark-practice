from pyspark import SparkConf, SparkContext


def parse_line(line: str):
    values = line.split(",")
    station_id = values[0]
    entry_type = values[2]
    temperature = float(values[3]) * 0.1 * (9 / 5) + 32
    return station_id, entry_type, temperature


def main():
    conf = SparkConf().setMaster("local").setAppName("MaxTemperatures")
    sc = SparkContext(conf=conf)

    lines = sc.textFile("./1800.csv")
    max_temperatures = lines.map(parse_line).filter(lambda x: x[1] == "TMAX")
    station_max_temperatures = max_temperatures.map(lambda x: (x[0], x[2])).reduceByKey(lambda x, y: max(x, y))
    results = station_max_temperatures.collect()
    for station, temperature in results:
        print(f"{station} - {round(temperature, 2)}")


if __name__ == '__main__':
    main()
