# spark-snippets

## Collect list with nulls

Collect list will skip null values by default. A workaround is to wrap all values in an array, then collect_list, and finally flatten.

### Simple group by - agg

    sc.parallelize(Array((1, Some("a")), (1, Some("b")), (2, Some("c")), (2, None), (2, Some("e"))))
        .toDF("x1", "x2")
        .groupBy("x1")
        .agg(flatten(collect_list(array("x2"))).as("x2"))
        .show

    +---+-------+
    | x1|     x2|
    +---+-------+
    |  2|[c,, e]|
    |  1| [a, b]|
    +---+-------+

### Window aggregation

    import org.apache.spark.sql.expressions.Window
    
    sc.parallelize(Array((1, Some("a"), 1), (1, Some("b"), 2), (2, Some("c"), 3), (2, None, 4), (2, Some("e"), 5)))
        .toDF("group", "x", "ord")
        .withColumn("window_cum", flatten(
                                    collect_list(array($"x"))
                                      .over(
                                        Window
                                          .partitionBy("group")
                                          .orderBy("ord")
                                          .rowsBetween(Window.unboundedPreceding, Window.currentRow)
                                        )
                                      )
                                    )
        .show
        
    +-----+----+---+----------+
    |group|   x|ord|window_cum|
    +-----+----+---+----------+
    |    2|   c|  3|       [c]|
    |    2|null|  4|      [c,]|
    |    2|   e|  5|   [c,, e]|
    |    1|   a|  1|       [a]|
    |    1|   b|  2|    [a, b]|
    +-----+----+---+----------+

