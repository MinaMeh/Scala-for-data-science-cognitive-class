import data.{Airport, Flight}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object Joins{
  def main(args: Array[String]): Unit = {
    val sparkConf= new SparkConf()
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName("Spark data frames")
    sparkConf.set("spark.sql.shuffle.partitions","4")
    sparkConf.set("spark.app.id","SparkDataFrames")
    val sc= new SparkContext(sparkConf)
    val sqlContext=  new SQLContext(sc)
    import sqlContext.implicits._ //needed for column idioms like $"foo".desc.
    val flightsPath= "../data/airline-flights/alaska-airlines/2008.csv"
    val airportsPath= "../data/airline-flights/airports.csv"
    val flightsRDD = for{
      line <- sc.textFile(flightsPath)
      flight<- Flight.parse(line)
    } yield flight
    val airportRDD= for{
      line <- sc.textFile(airportsPath)
      airport<- Airport.parse(line)
    } yield airport
    val flights= sqlContext.createDataFrame(flightsRDD)
    val airports= sqlContext.createDataFrame(airportRDD)
    println("flight schema: ")
    flights.printSchema
    println("first 10 flights")
    flights.show(10)
    println("airport schema")
    airports.printSchema()
    println("First 10 airports")
    airports.show(10)
    val flights_between_airports= flights.select($"origin",$"dest")
      .groupBy($"origin",$"dest").count()
      .orderBy($"count".desc,$"origin",$"dest")
  //  val flights_between_airports_sql = sql("""
    //    SELECT origin, dest, COUNT(*) AS count FROM flights
      //  GROUP BY origin, dest
        //ORDER BY count DESC, origin, dest
        //""")
    val fba= flights_between_airports
    val air= airports
    val fba2= fba
      .join(air,fba("origin")===air("iata"))
        .select("origin","airport","dest","count")
        .toDF("origin","origin_airport","dest","count")
        .join (air,$"dest"===air("iata"))
      .select("origin","origin_airport","dest","airport","count")
        .toDF("origin","origin_airport","dest","dest_airport","count")
    println("fba2 schema")
    fba2.printSchema
    println("fba2")
    fba2.show()




    sc.stop()
  }
}