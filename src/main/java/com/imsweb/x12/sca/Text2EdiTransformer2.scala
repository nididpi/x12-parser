package com.imsweb.x12.sca

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import com.imsweb.x12.udf.X12Reader_udf2


object X12RowIterator2 {
  def apply(iteratorRow: Iterator[Row], messageType: String): Iterator[Row] = {
    iteratorRow.map { row =>
      val x12content = row.getAs[String]("value")
      val a = new X12Reader_udf2()
      val jsonMessage = a.call(x12content, messageType)
      // Create a new Row with the existing fields and the new fullName
      Row.fromSeq(row.toSeq :+ jsonMessage)
//      Row(jsonMessage)
    }
  }
}


object Text2EdiTransformer2 {
  def transform(textDf: DataFrame, messageType: String): DataFrame = {

    val schema = StructType(Seq(
      StructField("value", StringType, nullable = true),
      StructField("jsonMessage", StringType, nullable = true),
    ))

    val otherColumns = textDf.columns.filterNot(_ == "value")

    //add all the nonValueColumns to the schema
    val newSchema = StructType(schema.fields ++ otherColumns.map(column => textDf.schema(column)))
    implicit val rowEncoder: ExpressionEncoder[Row] = ExpressionEncoder(newSchema)

    val df = textDf.mapPartitions {
      partition =>
        X12RowIterator2(partition, messageType)
    }

//    val jsonDf = df.withColumn("parsed", from_json(df("jsonMessage"), x12Schema))
//    val parsed_json_expr = from_json(df("json_string"), ArrayType(x12Schema))
//    val jsonDf = df.select(inline(parsed_json_expr))

    //    val a = new BuildX12JsonSchema(messageType)
    //    val schema = a.getJsonSchema()

    df

  }
}