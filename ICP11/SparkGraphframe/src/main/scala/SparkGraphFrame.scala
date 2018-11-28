import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame
import org.apache.spark.rdd.RDD
import scala.util.MurmurHash

object SparkGraphFrame {
  class SimpleCSVHeader(header:Array[String]) extends Serializable {
    val index = header.zipWithIndex.toMap
    def apply(array:Array[String], key:String):String = array(index(key))
  }

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    /*val input = spark.createDataFrame(List(
      ("a", "Alice", 34),
      ("b", "Bob", 36),
      ("c", "Charlie", 30),
      ("d", "David", 29),
      ("e", "Esther", 32),
      ("f", "Fanny", 36),
      ("g", "Gabby", 60)
    )).toDF("id", "name", "age")
    val output = spark.createDataFrame(List(
      ("a", "b", "friend"),
      ("b", "c", "follow"),
      ("c", "b", "follow"),
      ("f", "c", "follow"),
      ("e", "f", "follow"),
      ("e", "d", "friend"),
      ("d", "a", "friend"),
      ("a", "e", "friend")
    )).toDF("src", "dst", "relationship")

    val g = GraphFrame(input,output)
    g.vertices.show()
    g.edges.show()*/

    val stations = spark.read.format("csv").option("header", "true").load("D:\\Downloads\\201508_station_data.csv")
    val trips = spark.read.format("csv").option("header", "true").load("D:\\Downloads\\201508_trip_data.csv")

    val input = stations.select("id", "dockcount", "landmark")
    val output = trips.select("src", "dst", "relationship")

    val g = GraphFrame(input, output)
    g.vertices.show()
    g.edges.show()
  }
}