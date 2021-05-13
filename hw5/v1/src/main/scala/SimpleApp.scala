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
	val cc = edges.stronglyConnectedComponents(4)
	val vc = cc.vertices
	val d_vc = vc.distinct.count()
	//vc.take(100).foreach(println)
	val cntByKey = vc.map(x => (x._2, x._2)).countByKey()
	cntByKey.foreach(println)

	//println(s"d_cc: $d_cc")
	println(s"d_vc: $d_vc")
	
    spark.stop()
  }
}