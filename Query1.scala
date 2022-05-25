import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc}
import org.apache.spark.sql.types.IntegerType

object Query1 {
  def main(args: Array[String]): Unit = {

    val spark =
      SparkSession
        .builder
        .appName("Query 2 App")
        .master("local")
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.query1")
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.query1")
        .getOrCreate()

    var df = spark.read.option("Header", "true").csv("Delitti-italia.csv")


    def intersezioneReg(anno: String): Unit ={
      val l= Seq("Abruzzo", "Basilicata", "Calabria",
        "Campania", "Emilia-Romagna", "Friuli-Venezia Giulia", "Lazio", "Liguria", "Lombardia", "Marche",
        "Molise", "Piemonte", "Puglia", "Sardegna", "Sicilia", "Toscana", "Trentino Alto Adige / Südtirol", "Umbria",
        "Valle d'Aosta / Vallée d'Aoste", "Veneto")
      df = df.filter((df("Territorio").isin(l:_*)))
        .filter((df("Tipo di delitto")==="totale"))
        .filter(df("TIME")=== anno)
        .toDF()
        .select(df("Territorio"), df("Tipo di delitto"), df("TIME"), df("Value"))
        .withColumn("Value", col("Value").cast(IntegerType))
        .sort(desc("Value"))
      df.coalesce(1).write.mode("overwrite").option("header", "true").csv("result")
      df.write.format("mongo").mode("overwrite").save()
    }

    spark.time(intersezioneReg("2019"))
    spark.stop()
  }
}