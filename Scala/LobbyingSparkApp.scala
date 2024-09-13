package org.cscie88c.lobbyproject

import org.apache.spark.sql.SparkSession
import com.typesafe.scalalogging.{LazyLogging}
import org.cscie88c.config.{ConfigUtils}
import org.cscie88c.utils.{SparkUtils}
import org.apache.spark.sql.{Dataset, DataFrame, Row}
import org.apache.spark.sql.functions.when
import pureconfig.generic.auto._
import org.apache.spark.sql.functions.{when}


// run with: sbt "runMain org.cscie88c.lobbyproject.LobbyingSparkApp"
object LobbyingSparkApp {

  def main(args: Array[String]): Unit = {
    implicit val conf:LobbyingSparkAppConfig = readConfig()
    val spark = SparkUtils.sparkSession(conf.name, conf.masterUrl)
    val lobbyingDF = loadLobbyingData(spark)
    //val augmentedTransactionsDF = addCategoryColumn(transactionDF)
    val augmentedTransactionsDF = lobbyingDF
    augmentedTransactionsDF.createOrReplaceTempView("lobbyingdata")
    // val sparkSQL = "SELECT registrant, format_number(sum(amount), 2) as sum_contribution FROM lobbyingdata GROUP BY registrant order by sum_contribution desc" 
    // printDataFrame(spark.sql(sparkSQL))

    // val sparkSQLAffiliate = "SELECT affiliate, count(amount) as count_contribution FROM lobbyingdata GROUP BY affiliate"
    // printDataFrame(spark.sql(sparkSQLAffiliate))

    // val sparkSQLYear = "SELECT year, format_number(sum(amount), 2) as sum_contribution FROM lobbyingdata GROUP BY year order by year desc"
    // printDataFrame(spark.sql(sparkSQLYear))

    // val sparkSQLSource = "SELECT source, format_number(sum(amount), 2) as sum_contribution FROM lobbyingdata GROUP BY source order by sum_contribution desc"
    // printDataFrame(spark.sql(sparkSQLSource))

    // val categoryDF = loadCategoryData(spark)
    // val industryDF = loadIndustryData(spark)

    // import spark.implicits._
    // val joinedDf = industryDF.joinWith(categoryDF,  $"code" === $"catCode")

    // joinedDf.createOrReplaceTempView("industrydata")

    // val industrySQL = "SELECT _1.catcode, _2.name, format_number(sum(_1.total), 2) as sum_contribution FROM industrydata GROUP BY _1.catcode, _2.name order by sum_contribution desc"
    // printDataFrame(spark.sql(industrySQL))

    val lobbyistDF = loadLobbyistData(spark)

    lobbyistDF.createOrReplaceTempView("lobbyistdata")
    val sparkSQLLobbyist = "SELECT formercongmem, format_number(count(lobbyistId), 2) as count_lobbyist FROM lobbyistdata GROUP BY formercongmem order by count_lobbyist desc"
    printDataFrame(spark.sql(sparkSQLLobbyist))


    spark.stop()
  }

  def readConfig(): LobbyingSparkAppConfig = ConfigUtils.loadAppConfig[LobbyingSparkAppConfig]("org.cscie88c.lobbying-application")

  def loadLobbyingData(spark: SparkSession)(implicit conf: LobbyingSparkAppConfig): Dataset[Lobbying] = {
    import spark.implicits._

    val rdd = 
      spark
      .sparkContext
      .textFile(conf.lobbyingFile)

    spark.createDataset(rdd.map(Lobbying(_)).collect {case Some (item) => item})
  }

  def loadLobbyistData(spark: SparkSession)(implicit conf: LobbyingSparkAppConfig): Dataset[Lobbyist] = {
    import spark.implicits._

    val rdd = 
      spark
      .sparkContext
      .textFile(conf.lobbyistFile)

    spark.createDataset(rdd.map(Lobbyist(_)).collect {case Some (item) => item})
  }


   def loadIndustryData(spark: SparkSession)(implicit conf: LobbyingSparkAppConfig): Dataset[Industry] = {
    import spark.implicits._

    val rdd = 
      spark
      .sparkContext
      .textFile(conf.industryFile)

    //rdd.take(50).foreach(println(_))
    spark.createDataset(rdd.map(Industry(_)).collect {case Some (item) => item})
  }

   def loadCategoryData(spark: SparkSession)(implicit conf: LobbyingSparkAppConfig): Dataset[Category] = {
    import spark.implicits._

    val rdd = 
      spark
      .sparkContext
      .textFile(conf.categoryFile)

    //rdd.take(50).foreach(println(_))
    spark.createDataset(rdd.map(Category(_)).collect {case Some (item) => item})
  }

  def loadReportTypeData(spark: SparkSession)(implicit conf: LobbyingSparkAppConfig): Dataset[ReportType] = {
    import spark.implicits._

    val rdd = 
      spark
      .sparkContext
      .textFile(conf.reportFile)

    spark.createDataset(rdd.map(ReportType(_)).collect {case Some (item) => item})
  }

  def loadDataSQL(spark: SparkSession)(implicit conf: LobbyingSparkAppConfig): DataFrame = {
    
    spark
      .read
      //.option("delimiter", """,(?=(?:[^|]|[^|]|)[^|]$)""")
      .option("delimiter", "|,")
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(conf.lobbyingFile)

  }

  def addCategoryColumn(raw: DataFrame): DataFrame = {
    //import raw.sqlContext.implicits._

    //raw.withColumn("Category", when($"tran_amount" > 80, "High").otherwise("Standard"))
    raw
  }

  def printDataFrame(df: DataFrame): Unit = df.show()
  
}
