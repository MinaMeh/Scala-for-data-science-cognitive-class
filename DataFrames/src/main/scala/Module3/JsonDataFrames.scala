import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

object JsonDataFrames{
  def main(args: Array[String]): Unit = {
    val sparkConf= new SparkConf()
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName("Spark data frames")
    sparkConf.set("spark.sql.shuffle.partitions","4")
    sparkConf.set("spark.app.id","DataFramesWithJson")
    val sc= new SparkContext(sparkConf)
    val sqlContext=  new SQLContext(sc)
    val json: DataFrame=sqlContext.read.json("../data/airline-flights/carriers.json")
    json.printSchema()
    print("loaded carrier information", json.collect().foreach(println))
    //collect corrupted records
    json.where(json("_corrupt_record").isNotNull).collect().foreach(println)
    sc.stop()
  }
}