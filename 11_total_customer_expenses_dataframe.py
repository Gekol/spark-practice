from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

from core.session_manager import SessionManager


def main():
    with SessionManager("TotalCustomerExpensesDataFrame") as spark:
        schema = StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("item_id", IntegerType(), True),
            StructField("amount", FloatType(), True)
        ])

        orders = spark.read.schema(schema).csv("./customer-orders.csv")

        total_expenses_by_customer = orders.groupBy("customer_id").agg(
            func.round(func.sum("amount"), 2).alias("total_expenses")
        ).sort("total_expenses")
        total_expenses_by_customer.show(total_expenses_by_customer.count())


if __name__ == '__main__':
    main()
