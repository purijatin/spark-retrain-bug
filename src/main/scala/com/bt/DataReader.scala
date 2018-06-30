package com.bt

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType}
import org.slf4j.LoggerFactory

import scala.util.Try


class DataReader(spark: SparkSession) {

  @transient private val log = LoggerFactory.getLogger(getClass)

  def data: DataFrame = {

    val schema =
      List("reviewtext", "summary")
      .foldLeft(new StructType)((st, col) => st.add(col, StringType))

    @transient
    val data: DataFrame = spark.read
      .schema(schema)
      .json("data/*.json")

    log.info(data.toDF().schema.treeString)

    if (hasErrors(data)) {
      throw new RuntimeException("Not proceeding forward as data is error")
    } else {
      LoggerFactory.getLogger(getClass).info("No corrupt records...")
      data
    }
  }


  private def hasErrors(data: DataFrame): Boolean = {
    if (Try(data("_corrupt_record")).isSuccess) {
      val corruptRows = data.filter("_corrupt_record is not null")
      val corrupt = corruptRows.count()
      LoggerFactory.getLogger(getClass).info(s"$corrupt number of records are corrupt. Some sample ones:")
      corruptRows.show(10, truncate = false)
      true
    } else {
      false
    }
  }
}

object SentientJsonBasedReader {
  def apply(sparkSession: SparkSession) = new DataReader(sparkSession)
}
