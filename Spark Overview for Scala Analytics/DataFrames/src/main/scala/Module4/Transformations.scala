import data.Flight
import org.apache.log4j.{Logger,Level}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object Transformations{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkConf= new SparkConf()
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName("Spark data frames")
    sparkConf.set("spark.sql.shuffle.partitions","4")
    sparkConf.set("spark.app.id","SparkDataFramesTransformtions")
    val sc= new SparkContext(sparkConf)
    val sqlContext=  new SQLContext(sc)
    val flightsPath= "../data/airline-flights/alaska-airlines/2008.csv"
    val flightsRDD = for{
      line <- sc.textFile(flightsPath)
      flight<- Flight.parse(line)
    } yield flight
    val flights= sqlContext.createDataFrame(flightsRDD)
    flights.registerTempTable("flights")
    flights.cache
    flights.agg(
      min("times.actualElapsedTime"),
      max("times.actualElapsedTime"),
      avg("times.actualElapsedTime"),
      sum("times.actualElapsedTime")
    ).show()
    flights.agg(count("*")).show() //count all rows
    flights.agg(count("tailNum")).show() //same bcz no tailNum==NULL
    flights.agg(countDistinct("tailNum")).show() //how many distinct planes
    flights.agg(approx_count_distinct("tailNum")).show //approximatively ditinct planes

    flights.cube("origin","dest").avg().show()
    import sqlContext.implicits._ //needed for column idioms like $"foo".desc.

    flights.cube("origin","dest").avg()
      .select("origin","dest","avg(distance)")
        .filter($"origin" === "LAX").show()

    flights.cube("dest","origin").agg(
      Map(
        "*"->"count",
        "times.actualElapsedTime"->"avg",
        "distance"-> "avg"
      )
    ).orderBy($"avg(distance)".desc).show()
    val dist_time=flights.cube("origin","dest").agg(
      avg("distance").as("avg_dist"),
      min("times.actualElapsedTime").as("min_time"),
      max("times.actualElapsedTime").as("max_time"),
      avg("times.actualElapsedTime").as("avg_time"),
      (avg("distance")/avg("times.actualElapsedTime")).as("t_d")
    ).orderBy($"avg_dist".desc).cache()
    dist_time.show()
    sc.stop()
  }
}