package com.sinosoft.algorithm

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// representation of input: each person has an ID and an optional parent ID
case class Person(id: Int, parentId: Option[Int])

// representation of result: each person is optionally attached its "ultimate" ancestor,
// or none if it had no parent id in the first place
case class WithAncestor(person: Person, ancestor: Option[Person]) {
  def hasGrandparent: Boolean = ancestor.exists(_.parentId.isDefined)
}

/**
  * 递归查询父id的方法，可以解决类似微博转发层级问题
  */
object RecursiveParentLookup {
  // requested method
  def findUltimateParent(rdd: RDD[Person]): RDD[WithAncestor] = {

    // all persons keyed by id
    def byId = rdd.keyBy(_.id).cache()

    // recursive function that "climbs" one generation at each iteration
    def climbOneGeneration(persons: RDD[WithAncestor]): RDD[WithAncestor] = {
      val cached = persons.cache()
      // find which persons can climb further up family tree
      val haveGrandparents = cached.filter(_.hasGrandparent)

      if (haveGrandparents.isEmpty()) {
        cached // we're done, return result
      } else {
        val done = cached.filter(!_.hasGrandparent) // these are done, we'll return them as-is
        // for those who can - join with persons to find the grandparent and attach it instead of parent
        val withGrandparents = haveGrandparents
          .keyBy(_.ancestor.get.parentId.get) // grandparent id
          .join(byId)
          .values
          .map({ case (withAncestor, grandparent) => WithAncestor(withAncestor.person, Some(grandparent)) })
        // call this method recursively on the result 递归调用返回函数
        done ++ climbOneGeneration(withGrandparents)
      }
    }

    // call recursive method - start by assuming each person is its own parent, if it has one:
    climbOneGeneration(rdd.map(p => WithAncestor(p, p.parentId.map(i => p))))
  }

}


object SparkSQLRecurse {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("SparkSQLRecurse")
      .master("local[*]").getOrCreate()
    val person1 = Person(1, None)
    val person2 = Person(2, Some(1))
    val person3 = Person(3, Some(2))
    val person4 = Person(4, Some(2))
    val person5 = Person(5, None)
    val person6 = Person(6, Some(5))
    val sc = spark.sparkContext
    val input = sc.parallelize(Seq(person1, person2, person3, person4, person5, person6))
    val result = RecursiveParentLookup.findUltimateParent(input).collect()
    result.foreach(println(_))


    spark.close()
  }
}
