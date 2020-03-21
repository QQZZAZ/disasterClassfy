package com.sinosoft

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, PartitionStrategy, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object GragphxPregleTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val vertexArray = Array(
      (1L, ("Alice", 0)),
      (2L, ("Henry", 0)),
      (3L, ("Charlie", 0)),
      (4L, ("Peter", 0)),
      (5L, ("Mike", 0)),
      (6L, ("Kate", 0))
    )

    // 边
    val edgeArray = Array(
      Edge(2L, 1L, 1),
      Edge(1L, 4L, 1),
      Edge(3L, 2L, 1),
      Edge(4L, 6L, 1),
      Edge(3L, 1L, 1),
      Edge(5L, 6L, 1)
      ,
      Edge(6L, 7L, 1)
    )

    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD).partitionBy(PartitionStrategy.RandomVertexCut)
    // Triplet操作
   /* println("列出所有的Triples:")
    for (triplet <- graph.triplets.collect) {
      println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
    }
    println

    println("列出边属性>3的Triples:")
    for (triplet <- graph.triplets.filter(t => t.attr > 1).collect) {
      println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
    }
    println

    // Degree操作
    println("找出图中最大的出度,入度,度数:")

    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }

    println("Max of OutDegrees:" + graph.outDegrees.reduce(max))
    println("Max of InDegrees:" + graph.inDegrees.reduce(max))
    println("Max of Degrees:" + graph.degrees.reduce(max))
    println*/


    val sourceId: VertexId = 3L
    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)


    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist),
      // 权重计算
      triplet => {
        println(triplet.srcId + " ++ " + triplet.srcAttr + " " + triplet.attr + " ==" + triplet.dstId + " " + triplet.dstAttr)
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          //          println("父ID"+triplet)
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      // 最短距离
      (a, b) => math.max(a, b)
    )
    println(sssp.vertices.collect.mkString("\n"))

  }
}
