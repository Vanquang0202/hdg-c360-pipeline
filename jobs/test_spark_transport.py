"""Offline Spark transport tests. Creates small DataFrames without JDBC connections."""

import os
import sys
import unittest
from pathlib import Path

import pyspark

os.environ["SPARK_HOME"] = str(Path(pyspark.__file__).parent)

os.environ["PYSPARK_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, functions as F

from spark_common import canonical_number, reconcile_frames


class TransportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[1]")
                     .appName("HDG transport tests")
                     .config("spark.ui.enabled", "false")
                     .config("spark.driver.host", "127.0.0.1")
                     .config("spark.driver.bindAddress", "127.0.0.1")
                     .config("spark.sql.shuffle.partitions", "1")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_exact_decimal_representation(self):
        examples = [
            (None, None), ("0", "0"), ("-0.000", "0"),
            (".5", "5E-1"), ("-.00120", "-12E-4"), ("1.", "1E0"),
            ("1.2300", "123E-2"), ("123E-2", "123E-2"),
            ("-0.00120", "-12E-4"),
            ("1E+99", "1E99"), ("1E-99", "1E-99"),
            ("99999999999999999999999999999999999999", "99999999999999999999999999999999999999E0"),
            ("1.2345678901234567890123456789012345678E-99", "12345678901234567890123456789012345678E-136"),
            ("99999999999999999999", "99999999999999999999E0"),
        ]
        frame = self.spark.createDataFrame(examples, "raw string, expected string")
        actual = frame.select(
            canonical_number(F.col("raw")).alias("actual"), "expected").collect()
        self.assertTrue(all(row.actual == row.expected for row in actual))

    def test_counts_alone_cannot_hide_content_or_duplicate_mismatch(self):
        schema = dict(columns=[
            dict(name="amount", oracle="NUMBER"),
            dict(name="label", oracle="VARCHAR2(20 BYTE)"),
        ])
        left = self.spark.createDataFrame(
            [("1.2300", "A"), ("1.2300", "A"), (None, None)], "amount string, label string")
        matching = self.spark.createDataFrame(
            [("123E-2", "A"), ("1.23", "A"), (None, None)], "amount string, label string")
        wrong = self.spark.createDataFrame(
            [("1.23", "A"), ("1.23", "B"), (None, None)], "amount string, label string")
        self.assertTrue(reconcile_frames(left, matching, schema)["equal"])
        result = reconcile_frames(left, wrong, schema)
        self.assertEqual(result["source_rows"], result["target_rows"])
        self.assertFalse(result["content_equal"])

    def test_truncated_id_unicode_date_and_timestamp_are_not_equal(self):
        table = dict(columns=[
            dict(name="id", oracle="VARCHAR2(40 BYTE)"),
            dict(name="ho_ten", oracle="VARCHAR2(250 BYTE)"),
            dict(name="amount", oracle="NUMBER"),
            dict(name="day", oracle="DATE"),
            dict(name="instant", oracle="TIMESTAMP(6)"),
        ])
        row = ("HDG00000000000001", "Nguyễn Văn An ", "1E99",
               "2026-11-11 23:59:59 AD", "2026-11-11 12:34:56.123456 AD")
        definition = "id string, ho_ten string, amount string, day string, instant string"
        left = self.spark.createDataFrame([row], definition)
        for index, wrong in [(0,"HD"),(1,"Nguyễn Văn An"),(2,"1E98"),
                             (3,"2026-11-11 00:00:00 AD"),
                             (4,"2026-11-11 12:34:56.123455 AD")]:
            changed = list(row)
            changed[index] = wrong
            result = reconcile_frames(left, self.spark.createDataFrame([tuple(changed)], definition), table)
            self.assertEqual(result["source_rows"], result["target_rows"])
            self.assertFalse(result["equal"], index)

    def test_null_empty_and_duplicate_multiplicity(self):
        table=dict(columns=[dict(name="v",oracle="VARCHAR2(40 BYTE)")])
        left=self.spark.createDataFrame([(None,), ("A",), ("A",)], "v string")
        wrong=self.spark.createDataFrame([("",), ("A",), ("B",)], "v string")
        self.assertFalse(reconcile_frames(left,wrong,table)["equal"])


if __name__ == "__main__":
    unittest.main()
