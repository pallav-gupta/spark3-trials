package com.trial

import com.trial.common.{ConfigProvider, DefaultSparkConfProvider, SparkSessionProvider}
import org.apache.spark.joins.BroadCastLoopBreakJoinSelection
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}
import org.slf4j.LoggerFactory

object TestJoin
    extends SparkSessionProvider
    with DefaultSparkConfProvider
    with ConfigProvider
    with App {
  val logger = LoggerFactory.getLogger(getClass.getName)
  val spark = getSparkSession("TEST-JOIN")
  import spark.implicits._
  spark.experimental.extraStrategies = BroadCastLoopBreakJoinSelection :: Nil
  //spark.experimental.extraOptimizations = BroadCastHint :: Nil

  //spark.experimental.extraPreOptimizations = BroadCastHint :: Nil

  val df1 = spark.createDataFrame(
    spark.sparkContext.parallelize(
      Seq(
        Row(1, 2.0),
        Row(2, 100.0),
        Row(2, 1.0), // This row is duplicated to ensure that we will have multiple buffered matches
        // Row(2, 1.0),
        Row(3, 3.0),
        Row(5, 1.0),
        Row(6, 6.0),
        Row(null, null)
      )
    ),
    new StructType().add("a", IntegerType).add("b", DoubleType)
  )

  val df2 = spark.createDataFrame(
    spark.sparkContext.parallelize(
      Seq(
        Row(0, 0.0),
        Row(2, 3.0), // This row is duplicated to ensure that we will have multiple buffered matches
        Row(2, -1.0),
        Row(2, -1.0),
        Row(2, 3.0),
        Row(2, 4.0),
        Row(2, 5.0),
        Row(3, 2.0),
        Row(4, 1.0),
        Row(5, 3.0),
        Row(7, 7.0),
        Row(null, null)
      )
    ),
    new StructType().add("c", IntegerType).add("d", DoubleType)
  )

  val df3 = df1
    .hint("skr", "hello")
    .join(df2, $"a".eqNullSafe($"c") and $"b".lt($"d"), "left_outer")
  val df4 = df1.join(df2, $"a".eqNullSafe($"c") and $"b".lt($"d"), "left_outer")

  df3.show(false)
  df4.show(false)
}
