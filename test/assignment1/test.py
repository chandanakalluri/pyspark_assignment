import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark_assignment.src.assignment1.driver import only_iphone13  # Import your function from the module
from pyspark_assignment.src.assignment1.driver import purchase_all_products, upgraded_customers


class MyTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Create a SparkSession for testing
        cls.spark = SparkSession.builder \
            .appName("Testing") \
            .getOrCreate()

        # Define schema and sample data
        schema1 = StructType([
            StructField("customer", StringType(), nullable=False),
            StructField("product_model", StringType(), nullable=False)
        ])
        data1 = [
            ("1", "iphone13"),
            ("1", "dell i5 core"),
            ("2", "iphone13"),
            ("2", "dell i5 core"),
            ("3", "iphone13"),
            ("3", "dell i5 core"),
            ("1", "dell i3 core"),
            ("1", "hp i5 core"),
            ("1", "iphone14"),
            ("3", "iphone14"),
            ("4", "iphone13")
        ]
        cls.purchase_data_df = cls.spark.createDataFrame(data=data1, schema=schema1)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_only_iphone13(self):
        result_df = only_iphone13()

        expected_result = [('4',)]
        expected_schema = StructType([StructField("customer", StringType(), nullable=True)])

        expected_df = self.spark.createDataFrame(data=expected_result, schema=expected_schema)

        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_purchase_all_products(self):
        # Call the function under test
        result_df = purchase_all_products()

        # Define the expected result
        expected_result = [('1',)]
        expected_schema = StructType([StructField("customer", StringType(), nullable=False)])

        # Assert that the result matches the expected output
        self.assertEqual(result_df.collect(), expected_result)
        self.assertEqual(result_df.schema, expected_schema)

    def test_upgraded_customers(self):
        result = upgraded_customers()
        expected_result = ['1', '3',] # Assuming customer IDs '1' and '3' have upgraded their devices
        result_list = [row.customer for row in result.collect()]
        self.assertCountEqual(result_list, expected_result)


if __name__ == '__main__':
    unittest.main()

