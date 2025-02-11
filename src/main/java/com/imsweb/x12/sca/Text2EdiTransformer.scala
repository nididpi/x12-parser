package com.imsweb.x12.sca

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import com.imsweb.x12.udf.X12Reader_udf2

object Text2EdiTransformer {
  def transform(textDf: DataFrame, messageType: String): RDD[Row] = {
    //add all the nonValueColumns to the schema
    //    val newSchema = StructType.f
    //    implicit val rowEncoder: ExpressionEncoder[Row] = ExpressionEncoder(newSchema)


    val resultRDD = textDf.rdd.map { row =>
      // Create an instance of the Java class for each row

      // Extract values, process, and create new Row
      val x12content = row.getAs[String]("value")
      val a = new X12Reader_udf2()
      val jsonMessage = a.call(x12content, messageType)

      // Create a new Row with the existing fields and the new fullName
      Row.fromSeq(row.toSeq :+ jsonMessage)
    }

    resultRDD


    //
    //    val jsonDf = df.withColumn("value", from_json(df("value"), hl7Options.hl7Schema.get))
    //    jsonDf

  }
}

//object Main {
//  def main(args: Array[String]): Unit = {
//    // Initialize a Spark session
//    val spark = SparkSession.builder()
//      .appName("Text2EdiTransformerTest")
//      .master("local[*]") // Use local master for testing
//      .getOrCreate()
//
//    import spark.implicits._
//
//    // Create a sample DataFrame
//    val sampleData = Seq(
//      ("ISA*00*          *00*          *ZZ*ABCDEFGHIJKLM    *ZZ*123456789012345*150102*1753*^*00501*000000905*0*T*:~"),
//      ("ISA*00*          *00*          *ZZ*ABCDEFGHIJKLM    *ZZ*123456789012345*150103*1749*^*00501*000000906*0*T*:~")
//    )
//    val sampleDf: DataFrame = spark.createDataFrame(sampleData.map(Tuple1(_))).toDF("value")
//
//    // Call the transform method
//    val messageType = "SampleMessageType"
//    val resultRDD: RDD[Row] = Text2EdiTransformer.transform(sampleDf, messageType)
//
//    // Show the results
//    resultRDD.collect().foreach(println)
//
//    // Stop the Spark session
//    spark.stop()
//  }
//}