
object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSessionProvider.createSparkSession("Spark")
    spark.sparkContext.setLogLevel("ERROR")


    spark.stop()

  }
}