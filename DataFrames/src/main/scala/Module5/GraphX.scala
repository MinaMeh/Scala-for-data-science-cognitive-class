package Module5
package course2.module5

import data.Flight
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.hashing.MurmurHash3
object GraphX {
  def main(args: Array[String]): Unit = {
    val input = "../data/airline-flights/alaska-airlines/2008.csv"
    val conf = new SparkConf()
      .setAppName("GraphX")
      .setMaster("local[*]")
      .set("spark.app.id", "GraphX") // To silence Metrics warning.

    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val flights = for{
        line <- sc.textFile(input)
        flight <- Flight.parse(line)
      } yield  flight
      val airportCodes = flights.flatMap { f => Seq(f.origin, f.dest) }
      val airportVertices:RDD[(VertexId,String)]= airportCodes.distinct().map(x=> (MurmurHash3.stringHash(x).toLong,x))
      val flightsEdges= flights.map(f=>
        ((stringHash(f.origin),stringHash(f.dest)),1))
        .reduceByKey(_+_)
        .map{
          case ((src,dest),attr)=> Edge(src,dest,attr)
        }
      val graph= Graph(airportVertices,flightsEdges)
      println("\nNumber of airports in the graph:")
      println(graph.numVertices)
      println("\nNumber of flights in the graph:")
      println(graph.numEdges)
      println("\nFinding the most frequent flights between airports:")
      val triplets: RDD[EdgeTriplet[String,PartitionID]]=graph.triplets
      triplets.sortBy(_.attr,ascending = false)
        .map(triplet => s"${triplet.srcAttr}-> ${triplet.dstAttr} : ${triplet.attr}")
        .take(100).foreach(println)
      println("\nBusiest airport:")
      val by =
        triplets.map { triplet =>
          (triplet.srcAttr, triplet.attr)
        }.reduceByKey(_ + _)
      by.sortBy(-_._2).take(1).foreach(println)
      val incoming: RDD[(VertexId, (PartitionID, String))] = graph.inDegrees.join(airportVertices)

      println("\nAirports with least number of distinct incoming flights:")
      incoming.map {
        case (_, (count, airport)) => (count, airport)
      }.sortByKey().take(10).foreach(println)

      println("\nAirports with most number of distinct outgoing flights:")
      val outgoing: RDD[(VertexId, (PartitionID, String))] = graph.outDegrees.join(airportVertices)

      outgoing.map {
        case (_, (count, airport)) => (count, airport)
      }.sortByKey(ascending = false).take(10).foreach(println)

    } finally {
      sc.stop()
    }



  }
  def stringHash(str: String): Int = MurmurHash3.stringHash(str)
}
