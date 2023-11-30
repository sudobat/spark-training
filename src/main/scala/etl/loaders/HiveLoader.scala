package es.novaquality.spark
package etl.loaders

import etl.Loader

import org.apache.spark.sql.DataFrame

trait HiveLoader extends Loader {
  override def load(df: DataFrame): Unit = {
    println("Loading to Hive")
  }
}
