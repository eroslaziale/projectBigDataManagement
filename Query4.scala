import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc, col}
import org.apache.spark.sql.types.IntegerType

object Query4 {
  def main(args: Array[String]): Unit = {

    val spark =
      SparkSession
        .builder
        .appName("Query 2 App")
        .master("local")
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.query4")
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.query4")
        .getOrCreate()

    var df = spark.read.option("Header", "true").csv("Delitti-italia.csv")

    def intersezione(c: String, d: String): Unit ={
      df = df.filter(df("Territorio")===c)
        .filter(df("Tipo di delitto") === d)
        .toDF()
        .select(df("Territorio"), df("Tipo di delitto"), df("TIME"), df("Value"))
        .withColumn("TIME", col("TIME").cast(IntegerType))
        .sort(asc("TIME"))
        .toDF()
      df.coalesce(1).write.mode("overwrite").option("header", "true").csv("result4")
      df.write.format("mongo").mode("overwrite").save()
    }

    spark.time(intersezione("Cosenza", "omicidi colposi"))

    spark.stop()
  }
}