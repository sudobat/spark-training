package es.novaquality.spark
package etl

import org.apache.spark.sql.DataFrame

trait Transformer {
  def transform(df: DataFrame): DataFrame
}
