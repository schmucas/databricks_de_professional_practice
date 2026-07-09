from src.utils.transform_utils import generate_sk


def test_generate_sk_is_deterministic(spark):
    df = spark.createDataFrame([(42, "acme")], ["id", "name"])
    first = generate_sk(df, "sk", ["id", "name"]).collect()[0]["sk"]
    second = generate_sk(df, "sk", ["id", "name"]).collect()[0]["sk"]
    assert first == second


def test_generate_sk_delimiter_prevents_boundary_collision(spark):
    """('ab','c') and ('a','bc') must not produce the same key."""
    sk1 = generate_sk(
        spark.createDataFrame([("ab", "c")], ["a", "b"]), "sk", ["a", "b"]
    ).collect()[0]["sk"]
    sk2 = generate_sk(
        spark.createDataFrame([("a", "bc")], ["a", "b"]), "sk", ["a", "b"]
    ).collect()[0]["sk"]
    assert sk1 != sk2


def test_generate_sk_column_order_matters(spark):
    df = spark.createDataFrame([("x", "y")], ["a", "b"])
    sk_ab = generate_sk(df, "sk", ["a", "b"]).collect()[0]["sk"]
    sk_ba = generate_sk(df, "sk", ["b", "a"]).collect()[0]["sk"]
    assert sk_ab != sk_ba


def test_generate_sk_prepends_key_and_preserves_columns(spark):
    df = spark.createDataFrame([(1, "x")], ["id", "val"])
    result = generate_sk(df, "sk", ["id"])
    assert result.columns == ["sk", "id", "val"]
