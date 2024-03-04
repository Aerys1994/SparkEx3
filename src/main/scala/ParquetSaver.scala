import org.apache.spark.sql.functions.{col, split, to_timestamp}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}



object ParquetSaver {

  def execute(spark: SparkSession): Unit = {

   // val logsRDD = spark.sparkContext.wholeTextFiles("src/main/resources/*.gz")


    val rawLogsRdd = spark.sparkContext.textFile("src/main/resources/*.gz")

    val logPattern = """(\S+) - - \[([^\]]+)\] "(\S+) (\S+) (\S+)" (\S+) (\S+)""".r

    val structuredLogsRdd = rawLogsRdd.flatMap(line => logPattern.findAllMatchIn(line + " placeholder").map(_.subgroups))

    val logSchema = List("host", "date", "method", "resource", "protocol", "status_code", "size")

    val logsRowsRdd = structuredLogsRdd.map(row => Row(row: _*))

    val logsStructType = StructType(logSchema.map(fieldName => StructField(fieldName, StringType, nullable = true)))

    val logsDF = spark.createDataFrame(logsRowsRdd, logsStructType)

    val dfNasa = logsDF
      .withColumn("date", split(to_timestamp(split(col("date"), " ")(0), "dd/MMM/yyyy:HH:mm:ss"), "\\+")(0))

    dfNasa.write.mode("overwrite").parquet("src/main/resources/parquet")


  }
}
