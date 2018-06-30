package com.bt

import java.util.Date

import org.apache.spark.ml.feature.{HashingTF, IDF, StopWordsRemover, Tokenizer}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object Main {

  @transient
  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Sentient")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.eventLog.enabled", value = false)
    .config("spark.ui.enabled", value = false)
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    for {
      iter <- Stream.from(1, 1)
    } {
      val data = new DataReader(spark).data

      val pipelineData: Pipeline = new Pipeline().setStages(english("reviewtext") ++ english("summary"))

      val pipelineModel = pipelineData.fit(data)

      val all: DataFrame = pipelineModel.transform(data)
        .withColumn("rowid", functions.monotonically_increasing_id())

      //evaluate the pipeline
      all.rdd.foreach(x => x)
      println(s"$iter - ${all.count()}. ${new Date()}")
      data.unpersist()
    }
  }


  def english(inputColumn: String): Array[PipelineStage] = {

    val wordTokenizer = new Tokenizer()
      .setInputCol(inputColumn)
      .setOutputCol(inputColumn+"_tokened")

    val remover = new StopWordsRemover()
      .setInputCol(inputColumn+"_tokened")
      .setOutputCol(inputColumn+"_swRemoved")

    val stem = new SnowballStemmer()
      .setInputCol(inputColumn+"_swRemoved")
      .setOutputCol(inputColumn+"_stemmed")

    val tf = new HashingTF()
      .setInputCol(inputColumn+"_stemmed")
      .setOutputCol(inputColumn+"_hashTfVector")
      .setNumFeatures(math.pow(2, 22).toInt)

    val idf = new IDF()
      .setInputCol(inputColumn+"_hashTfVector")
      .setOutputCol(inputColumn+"_tfidf")
      .setMinDocFreq(2)

    Array(wordTokenizer, remover, stem, tf, idf)
  }
}

