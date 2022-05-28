import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, desc, lower}
import org.apache.spark.sql.types.IntegerType

object Query3 {
  def main(args: Array[String]): Unit = {

    val spark =
      SparkSession
        .builder
        .appName("Query 2 App")
        .master("local")
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.query3")
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.query3")
        .getOrCreate()

    var df = spark.read.option("Header", "true").csv("Delitti-italia.csv")

    def filterByDelitto(t:String): DataFrame = {
      df.filter(lower(df("Tipo di delitto")).contains(t.toLowerCase.trim)).toDF()
    }

    def intersezione(d: String,a:String): Unit ={
      val l= Seq("Sud","Nord","Centro","Nord-ovest","Nord-est","Isole","Italia","Abruzzo", "Basilicata", "Calabria",
        "Campania", "Emilia-Romagna", "Friuli-Venezia Giulia", "Lazio", "Liguria", "Lombardia", "Marche",
        "Molise", "Piemonte", "Puglia", "Sardegna", "Sicilia", "Toscana", "Trentino Alto Adige / Südtirol", "Umbria",
        "Valle d'Aosta / Vallée d'Aoste", "Veneto")

      df = df.filter(!(df("Territorio").isin(l:_*)))
        .filter(df("TIME") === a)
        .filter(df("Tipo di delitto") === d)
        //.intersect(filterByDelitto(d))
        .toDF()
        .select(df("Territorio"), df("Tipo di delitto"), df("TIME"), df("Value"))
        .withColumn("Value", col("Value").cast(IntegerType))
        .sort(desc("Value"))
        .limit(10)
        .toDF()
      df.coalesce(1).write.mode("overwrite").option("header", "true").csv("result3")
      df.write.format("mongo").mode("overwrite").save()
    }

    spark.time(intersezione("omicidi colposi", "2018"))

    spark.stop()
  }
}