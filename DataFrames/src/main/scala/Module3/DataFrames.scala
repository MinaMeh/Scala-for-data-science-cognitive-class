import data.{Airport, Flight}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
object SparkDataFrames  {
  def main (args : Array[String]):Unit= {
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
    flights.cache
    airports.cache
    // SELECT COUNT(*) FROM flights WHERE f.canceled>0
    val canceled_flights= flights.filter(flights("canceled")>0)
    println("canceled_flights",canceled_flights )
    println("canceled_flights.explain(extended = false) :",canceled_flights.explain(extended = false))
    println("canceled_flights.explain(extended = true) :",canceled_flights.explain(extended = true))
    canceled_flights.cache

    //we can refrence the columns several ways:
    flights.orderBy(flights("origin")).show
    flights.orderBy("origin").show
    flights.orderBy($"origin").show
    flights.orderBy($"origin".desc).show



    //SELECT cf.date.month AS month , COUNT(*)
    //FROM canceled_flights cf
    //GROUP BY cf.date.mounth
    //ORDERED BY month;
    val canceled_flights_by_month= canceled_flights.groupBy("date.month").count()
    println( "canceled flights by month :", canceled_flights_by_month)
    println("canceled_flights_by_month.explain(extended = true): ",canceled_flights_by_month.explain(extended = true))
    canceled_flights.unpersist

    val flights_between_airports= flights.select($"origin",$"dest")
      .groupBy($"origin",$"dest").count()
      .orderBy($"count".desc,$"origin",$"dest")
    println("flights between airports")
    flights_between_airports.show()
    sc.stop()
  }
}