import unittest
from pyspark_assignment.src.assignment2.driver import *

class TestUtil(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("Assignment 2 Testcase").getOrCreate()


    def test_create_dataframe(self):
        df = create_dataframe(self.spark)
        self.assertEqual(df.count(), 5)

    def test_print_partitions(self):   # testing number of partitions
        df = create_dataframe(self.spark)
        initial_partitions = df.rdd.getNumPartitions()
        self.assertEqual(initial_partitions, 8)

    def test_increase_partitions(self):    # testing increase of partitions
        df = create_dataframe(self.spark)
        initial_partitions = df.rdd.getNumPartitions()
        new_df = increase_partitions(df, num_partitions=5)
        self.assertEqual(new_df.rdd.getNumPartitions(), initial_partitions + 5)

    def test_decrease_partitions(self):   # testing decrease of partitions
        df = create_dataframe(self.spark)
        new_df = decrease_partitions(df, num_partitions=2)
        self.assertEqual(new_df.rdd.getNumPartitions(), 2)

    def test_mask_card_numbers(self):   # testing marking card numbers
        df = create_dataframe(self.spark)
        masked_df = mask_card_numbers(df)
        masked_numbers = masked_df.select("masked_card_number").rdd.flatMap(lambda x: x).collect()
        expected_masked_numbers = ['************4567', '************1234', '************5678',
                                   '************1122', '************1342']
        self.assertEqual(masked_numbers, expected_masked_numbers)


if __name__ == '__main__':
    unittest.main()