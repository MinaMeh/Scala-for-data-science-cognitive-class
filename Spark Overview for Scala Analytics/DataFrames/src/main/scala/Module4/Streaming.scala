package Module4
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Seconds


object Streaming {
  val defaultPort = 9000
  val interval = Seconds(2)
  val pause = 10  // milliseconds
  val server = "127.0.0.1"
  val checkpointDir = "../output/checkpoint_dir"
  val runtime = 30 * 1000   // run for N*1000 milliseconds
  val numIterations = 100000

  def readSocket(ssc: StreamingContext, server: String, port: Int): DStream[String] =
    try {
      Console.err.println(s"Connecting to $server:$port...")
      ssc.socketTextStream(server, port)
    } catch {
      case th: Throwable =>
        ssc.stop()
        throw new RuntimeException(
          s"Failed to initialize server:port socket with $server:$port:",
          th)
    }
  def main(args: Array[String]): Unit = {
    val port= if (args.size>0) args(0).toInt else defaultPort
    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkConf= new SparkConf()
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName("Spark data frames")
    sparkConf.set("spark.sql.shuffle.partitions","4")
    sparkConf.set("spark.app.id","SparkDataFramesStreaming")
    val sc= new SparkContext(sparkConf)
    def createContext(): StreamingContext={
      val ssc = new StreamingContext(sc, interval)
      ssc.checkpoint(checkpointDir)
      val dstream = readSocket(ssc, server, port)
      val numbers=for{
        line<- dstream
        number<- line.trim.split("\\s+")
      }yield number.toInt
      numbers.foreachRDD{ rdd=>
        rdd.countByValue.foreach(println)
      }
      ssc
    }
    var ssc: StreamingContext= null
    var dataThread: Thread= null
    try {
      println("Creating source socket:")
      dataThread = startSocketDataThread(port)
      ssc = StreamingContext.getOrCreate(checkpointDir, createContext _)
      ssc.start()
      ssc.awaitTerminationOrTimeout(runtime)
    }
    finally {
      if (dataThread != null) dataThread.interrupt()
      if (ssc != null)
        ssc.stop(stopSparkContext = true, stopGracefully = true)
    }
  }

  import java.net.{Socket, ServerSocket}
  import java.io.PrintWriter

  def makeRunnable(port: Int) = new Runnable {
    def run() = {
      val listener = new ServerSocket(port);
      var socket: Socket = null
      try {
        val socket = listener.accept()
        val out = new PrintWriter(socket.getOutputStream(), true)
        (1 to numIterations).foreach { i =>
          val number = (100 * math.random).toInt
          out.println(number)
          Thread.sleep(pause)
        }
      } finally {
        listener.close();
        if (socket != null) socket.close();
      }
    }
  }

  def startSocketDataThread(port: Int): Thread = {
    val dataThread = new Thread(makeRunnable(port))
    dataThread.start()
    dataThread
  }
}
