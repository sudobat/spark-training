package es.novaquality.spark

import org.apache.spark.{SparkConf, SparkContext}

object Pi extends App {
  val conf = new SparkConf()
    .setAppName("Pi")
    .setMaster("local[1]")

  val sc: SparkContext = new SparkContext(conf)

  val NUM_SAMPLES = 10_000_000

  val count = sc.parallelize(1 to NUM_SAMPLES)
    .filter { _ =>
      val x = math.random
      val y = math.random
      x * x + y * y < 1
    }.count()

  println(s"Pi is approximately ${4.0 * count / NUM_SAMPLES}")
}
