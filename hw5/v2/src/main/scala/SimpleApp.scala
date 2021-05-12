/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

object SimpleApp {
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }
  def main(args: Array[String]) {
    val logFile = "/opt/spark/workplace/vertices.tsv" // Should be some file on your system
	
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
	
	val dag = GraphLoader.edgeListFile(spark.sparkContext, "/opt/spark/workplace/dag_edge_list.txt")
    val edges = GraphLoader.edgeListFile(spark.sparkContext, "/opt/spark/workplace/edge_list.txt")
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
	
	
	println(s"edges.numEdges: $edges.numEdges")
	println(s"edges.numVertices: $edges.numVertices")
	val maxInDegree: (VertexId, Int)  = edges.inDegrees.reduce(max)
	val maxOutDegree: (VertexId, Int) = edges.outDegrees.reduce(max)
	val maxDegrees: (VertexId, Int)   = edges.degrees.reduce(max)
	println(s"maxInDegree: $maxInDegree")
	println(s"maxOutDegree: $maxOutDegree")
	println(s"maxDegrees: $maxDegrees")
	
    spark.stop()
  }
}