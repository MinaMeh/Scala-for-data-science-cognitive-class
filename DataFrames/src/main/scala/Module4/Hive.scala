package Module4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
object Hive {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkConf= new SparkConf()
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName("Spark data frames")
    sparkConf.set("spark.sql.shuffle.partitions","4")
    sparkConf.set("spark.app.id","SparkDataFramesHive")
    val sc= new SparkContext(sparkConf)
    val hiveContext=  new HiveContext(sc)
    import hiveContext.implicits._
    import hiveContext.sql
    import org.apache.spark.sql.functions._  // for min, max, etc.
    val dir  = "../data/airline-flights/hive/airports"
    val data = new java.io.File(dir)
    // Hive DDL statements require absolute paths:
    val path = data.getCanonicalPath
    println("Create an 'external' Hive table for the airports data:")
    sql ("DROP TABLE IF EXISTS airports")
    sql(s"""
        CREATE EXTERNAL TABLE IF NOT EXISTS airports (
          iata     STRING,
          airport  STRING,
          city     STRING,
          state    STRING,
          country  STRING,
          lat      DOUBLE,
          long     DOUBLE)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        LOCATION '$path'
        """).show
      sql("SHOW TABLES").show()
      sql("DESCRIBE airports").show(100)
      sql("DESCRIBE EXTENDED airports").show(100)
      sql("DESCRIBE FORMATTED airports").show(100)
      sql("DESCRIBE FORMATTED airports").show(100, truncate = false)


      sql("SELECT COUNT(*) FROM airports").show

    println("How many airports per country?")
    sql("""
        SELECT country, COUNT(*) FROM airports
        GROUP BY country
        """).show(100)
    println("There are four non-US airports:")
    sql("""
        SELECT iata, airport, country
        FROM airports WHERE country <> 'USA'
        """).show(100)
    sql("""
        SELECT state, COUNT(*) AS count FROM airports
        WHERE country = 'USA'
        GROUP BY state
        ORDER BY count DESC
        """).show(100)
    println("Hive queries return DataFrames; let's use that API:")
    val latlong = sql("SELECT state, lat, long FROM airports")
    latlong.cube("state").agg(
      min("lat"),
      max("lat"),
      avg("lat"),
      min("long"),
      max("long"),
      avg("long")
    ).show

    sql("DROP TABLE airports").show

    sc.stop()


  }
}
