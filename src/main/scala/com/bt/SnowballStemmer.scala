package com.bt

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.tartarus.snowball.ext.EnglishStemmer

class ScalaSnowball extends Serializable {
  val stemmer = new EnglishStemmer

  def stemWord(str: String): String = {
//    stemmer.synchronized {
      stemmer.setCurrent(str)
      stemmer.stem()
      stemmer.getCurrent
//    }
  }

  /**
    * Any change in ommitting or adding elements, makes the mapping from stemmed to unstemmed impossible
    * @param arr
    * @return
    */
  def stemArray(arr: Seq[String]): Seq[String] = {
    // here you can also remove punctuation and numbers!
    arr.map(stemWord).filter(!_.isEmpty)
  }
}

class SnowballStemmer(override val uid: String) extends UnaryTransformer[Seq[String], Seq[String], SnowballStemmer] {

  val snowballStemmer = new Param[Broadcast[ScalaSnowball]](this,
    "snowballStemmer", "snowball stemmer broadcast")

  def getStemmerInstance: Broadcast[ScalaSnowball] = $(snowballStemmer)

//  def setStemmerInstance(value: Broadcast[ScalaSnowball]): this.type =
//    set(snowballStemmer, value)

  override protected def createTransformFunc: Seq[String] => Seq[String] = {
    x => new ScalaSnowball().stemArray(x)
  }

  override protected def outputDataType = ArrayType(StringType)

  def this() = this(Identifiable.randomUID("stemmer"))

  override def copy(extra: ParamMap): SnowballStemmer = defaultCopy(extra)

  val language: Param[String] = new Param(this, "language", "stemming language (case insensitive).")

  def getLanguage: String = $(language)

  def setLanguage(value: String): this.type = set(language, value)

  setDefault(language -> "English")
}

