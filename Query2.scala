import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc}
import org.apache.spark.sql.types.IntegerType

object Query2 {
  def main(args: Array[String]): Unit = {

    val spark =
      SparkSession
        .builder
        .appName("Query 2 App")
        .master("local")
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.query2")
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.query2")
        .getOrCreate()


    var df = spark.read.option("Header", "true").csv("Delitti-italia.csv")

    def intersezione(c: String, a: String): Unit = {
      df = df.filter(df("Territorio") === c)
        .filter(df("TIME") === a)
        .filter(!(df("Tipo di delitto") === "totale"))
        .toDF()
        .select(df("Territorio"), df("Tipo di delitto"), df("TIME"), df("Value"))
        .withColumn("Value", col("Value").cast(IntegerType))
        .sort(desc("Value"))
        .toDF()
      df.coalesce(1).write.mode("overwrite").option("header", "true").csv("result2")
      df.write.format("mongo").mode("overwrite").save()
    }

    //intersezione("Bari", "2019")
    spark.time(intersezione("Cosenza", "2020"))

    spark.stop()
  }
}