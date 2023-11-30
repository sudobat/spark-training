package es.novaquality.spark

import etl.ETL
import etl.loaders.HiveLoader

import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object TransactionAggregator extends App with ETL with HiveLoader {

  // Config
  val spark: SparkSession = configureSpark
  val JDBC_URL: String = "jdbc:postgresql://localhost:5432/postgres"
  val connectionProperties: Properties = configurePostgres

  run()

  override def extract(): DataFrame = {
    val transactionsDF = spark.read.jdbc(
      JDBC_URL,
      "transactions",
      connectionProperties
    )

    transactionsDF
  }

  override def transform(df: DataFrame): DataFrame = {
    val top10ByOriginDF = df
      .select("origin", "amount")
      .groupBy("origin")
      .sum("amount")
      .withColumnRenamed("sum(amount)", "total_amount")
      .orderBy(desc("total_amount"))
      .limit(10)

    top10ByOriginDF.show()

    top10ByOriginDF
  }

  private def configureSpark = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("PostgresReader")
      .getOrCreate()

    spark
  }

  private def configurePostgres = {
    val connectionProperties = new Properties()
    connectionProperties.setProperty("driver", "org.postgresql.Driver")
    connectionProperties.put("user", "postgres")
    connectionProperties.put("password", "12345")

    connectionProperties
  }
}
