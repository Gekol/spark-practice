from pyspark import SparkConf, SparkContext


def parse_line(line: str):
    values = line.split(",")
    customer_id = int(values[0])
    amount = float(values[2])
    return customer_id, amount


def main():
    conf = SparkConf().setMaster("local").setAppName("TotalCustomerExpensesSorted")
    sc = SparkContext(conf=conf)

    orders = sc.textFile("./customer-orders.csv").map(parse_line)
    total_expenses = orders.reduceByKey(lambda x, y: x + y).map(lambda x: (x[1], x[0])).sortByKey()
    results = total_expenses.collect()
    for amount, customer_id in results:
        print(f"{customer_id}\t{round(amount, 2)}")


if __name__ == '__main__':
    main()
