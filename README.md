# spark-snippets

A collection of useful spark snippets and patterns.

## 1. Apply a function to multiple columns

Put the columns to a list and fo a fold operation.


    import org.apache.spark.sql.functions.col
    
    val cols = List("x1", "x2", "x3")
    
    val df = sc.parallelize(Array((1, Some(10), 1.1), (1, Some(11), 1.2), (2, Some(12), 1.3), (2, None, 1.4), (2, Some(13), 1.5)))
      .toDF("x1", "x2", "x3")
    
    cols
      .foldLeft(df){ case (df, c) => df.withColumn(c + "_neg", -col(c)) }
      .show

    +---+----+---+------+------+------+
    | x1|  x2| x3|x1_neg|x2_neg|x3_neg|
    +---+----+---+------+------+------+
    |  1|  10|1.1|    -1|   -10|  -1.1|
    |  1|  11|1.2|    -1|   -11|  -1.2|
    |  2|  12|1.3|    -2|   -12|  -1.3|
    |  2|null|1.4|    -2|  null|  -1.4|
    |  2|  13|1.5|    -2|   -13|  -1.5|
    +---+----+---+------+------+------+


## 2. Collect list with nulls

Collect list will skip null values by default. A workaround is to wrap all values in an array, then collect_list, and finally flatten.

#### Simple group by - agg

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

#### Window aggregation

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

