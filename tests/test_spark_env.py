def test_spark_map_in_pandas(spark):
    df = spark.range(100).withColumnRenamed("id", "x")

    def double(batches):
        for b in batches:
            yield b.assign(y=b.x * 2)

    out = df.mapInPandas(double, "x long, y long").toPandas().sort_values("x")
    assert out.y.tolist() == [2 * i for i in range(100)]
