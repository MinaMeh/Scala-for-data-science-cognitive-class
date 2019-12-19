package Module5
import org.apache.spark
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{VectorUDT, Vectors}
import org.apache.spark.ml.clustering.{KMeansModel, KMeans}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StructField, StructType}

object UnsupervisedExample {
  val FEATURES_COL = "features"


  def main(args: Array[String]): Unit = run("../data/kmeans_data.txt",3)
  def run(str: String, k: Int): Unit = {
    val conf = new SparkConf()
      .setAppName("UnsupervisedLearning")
      .setMaster("local[*]")
      .set("spark.app.id", "Kmeans") // To silence Metrics warning.

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    Logger.getRootLogger.setLevel(Level.ERROR)
    val rowRDD= sc.textFile(str)
      .filter(_.nonEmpty)
      .map(_.split(" ").map(_.toDouble))
      .map(Vectors.dense)
      .map(Row(_))
      .cache()
    val schema = StructType(Array(StructField(FEATURES_COL,new VectorUDT,false)))
    val dataset= sqlContext.createDataFrame(rowRDD,schema)

    val kmeans= new KMeans()
      .setK(k)
      .setMaxIter(10)
      .setFeaturesCol(FEATURES_COL)
    val model: KMeansModel= kmeans.fit(dataset)
    println ("final centers: ")
    model.clusterCenters.zipWithIndex.foreach(println)
    val testData = Seq("0.3 0.3 0.3", "8.0 8.0 8.0", "8.0 0.1 0.1")
      .map(_.split(" ").map(_.toDouble))
      .map(Vectors.dense)
    val test = sc.makeRDD(testData).map(Row(_))
    val testDF = sqlContext.createDataFrame(test, schema)

    model.transform(testDF).show()

    sc.stop()

  }

}
