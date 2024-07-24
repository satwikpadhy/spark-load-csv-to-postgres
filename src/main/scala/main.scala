import org.apache.spark.sql.SparkSession
import scala.io.Source
import org.apache.log4j.{Level, Logger}

//This branch is used to load a csv to a postgres so that we dont need to create a schema for it.

object main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("postgres-test").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    val csvFile = args(0)
    val ip = args(1)
    val port = args(2)
    val database = args(3)
    val tablename = args(4)
    val user = args(5)
    val password = args(6)

    println(s"Extracting csv : ${csvFile}")

    val df = spark.read
      .option("sep",",")
      .option("header", "true")  // Assuming first row is header
      .option("inferSchema", "true") // Infers the schema of the DataFrame
      .csv(csvFile)

    println(s"Extraction Successful for : ${csvFile}")
    
    println(s"Starting the insert to the table ${tablename}")

    df.distinct.repartition(10).write.mode("overwrite").format("jdbc")
              .option("url", s"jdbc:postgresql://${ip}:${port}/${database}")
              .option("user", s"${user}")
              .option("password", s"${password}")
              .option("dbtable", s"${tablename}")
              .option("driver", "org.postgresql.Driver")
              .save()

    println(s"${tablename} inserted to ${ip}:${port}/${database}")
    spark.stop()
  }
}