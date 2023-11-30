package es.novaquality.spark
package etl

import org.apache.spark.sql.DataFrame

trait Extractor {
  def extract(): DataFrame
}
