package es.novaquality.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount extends App {
  val conf = new SparkConf()
    .setAppName("WordCount")
    .setMaster("local[1]")

  val sc: SparkContext = new SparkContext(conf)

//  val textFile: RDD[String] = sc.textFile(getClass.getResource("/jabberwocky.txt").getPath)
//  val words: RDD[String] = textFile.flatMap(line => line.split(" "))
//  val wordTuple: RDD[(String, Int)] = words.map(word => (word, 1))
//  val wordCount: RDD[(String, Int)] = wordTuple.reduceByKey((valueA, valueB) => valueA + valueB)
//  val sortedWordCount: RDD[(String, Int)] = wordCount.sortBy(wordCount => wordCount._2, ascending = false)
//  val top10WordCount: Array[(String, Int)] = sortedWordCount.take(5)

  val top10WordsWithCount: Array[(String, Int)] = sc.textFile(getClass.getResource("/jabberwocky.txt").getPath)
    .flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey((valueA, valueB) => valueA + valueB)
    .sortBy(wordCount => wordCount._2, ascending = false)
    .take(5)

  top10WordsWithCount.foreach(wordWithCount => println(s"${wordWithCount._1} (${wordWithCount._2})"))
}
