from pyspark.testing import assertDataFrameEqual
from src.utils.reader_utils import dedupe

def test_dedupe_keeps_latest(spark):
    data = [
        (1, "2024-01-02", "B"),
        (1, "2024-01-01", "A"),
        (2, "2024-01-01", "C"),
    ]
    df = spark.createDataFrame(data, ["id", "ts", "val"])
    result = dedupe(df, partition_cols=["id"], order_col="ts")
    
    expected = spark.createDataFrame([
        (1, "2024-01-02", "B"),
        (2, "2024-01-01", "C"),
    ], ["id", "ts", "val"])
    
    assertDataFrameEqual(result, expected)