import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Analysis {

  def execute(spark: SparkSession): Unit = {

    val logsDF = spark.read.parquet("src/main/resources/parquet")
    //logsDF.show()
    //logsDF.printSchema()

    // Agrupacion por protocolo
    val protocolCounts = logsDF.groupBy("protocol").count()
    println("protocols")
    protocolCounts.show()

    // Codigo más comunes
    val statusCounts = logsDF.groupBy("status_code").count()
    val sortedStatusCounts = statusCounts.orderBy(desc("count"))
    println("most common codes")
    sortedStatusCounts.show()

    // verbos más comunes
    val petitionCounts = logsDF.groupBy("method").count()
    val sortedPetitionCounts = petitionCounts.orderBy(desc("count"))
    println("most common petitions")
    sortedPetitionCounts.show()


    // bytes transferidos
    val resourceBytes = logsDF.groupBy("resource").agg(sum("size").cast("long").alias("total_bytes"))
    val sortedResources = resourceBytes.orderBy(desc("total_bytes"))
    val maxBytesResource = sortedResources.select("resource", "total_bytes").first()
    println(s"El recurso con la mayor transferencia de bytes es: ${maxBytesResource.getString(0)}, con un total de ${maxBytesResource.getLong(1)} bytes.")

    // recurso con más registro
    val resourceCounts = logsDF.groupBy("resource").count()
    val sortedResources2 = resourceCounts.orderBy(desc("count"))
    val maxTrafficResource = sortedResources2.select("resource", "count").first()
    println(s"El recurso con más tráfico es: ${maxTrafficResource.getString(0)}, con un total de ${maxTrafficResource.getLong(1)} registros.")

    //Dia de más tráfico
    val dfWithDay = logsDF.withColumn("day", dayofmonth(col("date")))
    val dailyTraffic = dfWithDay.groupBy("day").count()
    val sortedDailyTraffic = dailyTraffic.orderBy(desc("count"))
    println("daily traffic")
    sortedDailyTraffic.show()

    // Hosts más frecuentes
    val hostCounts = logsDF.groupBy("host").count()
    val sortedHostCounts = hostCounts.orderBy(desc("count"))
    println("sorted hosts")
    sortedHostCounts.show()

    // Hora más tráfico
    val dfWithHour = logsDF.withColumn("hour", hour(col("date")))
    dfWithHour.createOrReplaceTempView("web_traffic")
    val hourTraffic = spark.sql("SELECT hour, COUNT(*) AS traffic_count FROM web_traffic GROUP BY hour ORDER BY traffic_count DESC")
    println("hours with more traffic")
    hourTraffic.show()

    //Errores cada dia
    val errors404 = dfWithDay.filter(col("status_code") === 404)
    val daily404Count = errors404.groupBy("day").count()
    println("daily 404 count")
    daily404Count.show()
  }

}
