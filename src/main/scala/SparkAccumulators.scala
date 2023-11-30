package es.novaquality.spark

import org.apache.spark.{SparkConf, SparkContext}

object SparkAccumulators extends App {
  SparkLogLevels.setLogLevelToWarn()
  val conf = new SparkConf()
    .setAppName("SparkAccumulators")
    .setMaster("local[1]")

  val sc: SparkContext = new SparkContext(conf)

  val total = sc.longAccumulator("Total")
  val count = sc.longAccumulator("Count")

  val list1 = sc.parallelize(List(1, 3, 5, 7, 9))
  list1.foreach(i => {
    total.add(i)
    count.add(1)
  })

  val list2 = sc.parallelize(List(2, 4, 6, 8))
  list2.foreach(i => {
    total.add(i)
    count.add(1)
  })

  val avg = total.value / count.value

  println(s"SUM: ${total.value}")
  println(s"COUNT: ${count.value}")
  println(s"AVG: ${avg}")
}
