package es.novaquality.spark
package etl

import org.apache.spark.sql.DataFrame

trait ETL {
  def extract(): DataFrame

  def transform(df: DataFrame): DataFrame

  def load(df: DataFrame): Unit

  def run(): Unit = load(transform(extract()))
}
