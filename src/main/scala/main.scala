
object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSessionProvider.createSparkSession("Spark")
    spark.sparkContext.setLogLevel("ERROR")

   // ParquetSaver.execute(spark)
    Analysis.execute(spark)


    spark.stop()

  }
}