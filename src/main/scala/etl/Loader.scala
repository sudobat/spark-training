package es.novaquality.spark
package etl

import org.apache.spark.sql.DataFrame

trait Loader {
  def load(df: DataFrame): Unit
}
