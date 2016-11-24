package us.patni.clickstream

import java.util.Calendar

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Created by Bhupendra Patni on 11/21/16.
  * The object takes --start_date, --end_date, --funnel_events & --input_dir
  * as a command line arguments to process funnel dynamically and write output
  * to a CSV file in the pre-defined output directory
  */
object EventFunnelAnalysis {

  def main(args: Array[String]): Unit = {

    //Parse Command Line Arguments
    val (startDate, endDate, funnelEvents, inputPartDir) = parseArguments(args)

    //Read Application Configuration
    val (env: String, sparkMaster: String, dataRoot: String) = getApplicationConfiguration


    //Setup spark sesion object
    def getSparkConfig: SparkSession = {
      //loading spark configuration
      val spark = new SparkSession.Builder().
        appName("ClickStream.SparkDataPipeline").
        master(sparkMaster).
        getOrCreate()
      spark
    }
    val spark: SparkSession = getSparkConfig
    val sqlContext = spark.sqlContext

    import spark.implicits._

    //Get all required ETL configuration
    val (inputDir: String, outputDir: String) = getEtlConfiguration(inputPartDir, dataRoot)

    //Load data in a data frame from all parquet files in the source folder
    val dfUserSessionEvents = sqlContext.read
      .option("mergeSchema", "true")
      .parquet(inputDir)
    //Register data frame as a temporary view to query using Spark SQL
    dfUserSessionEvents.createOrReplaceTempView("user_session_events")

    //inline function to create dynamic sql using the input funnel events
    def createDynamicSQLForFunnelEvents: String = {
      val listFunnelSteps = funnelEvents.split(",").toList
      var dynamicCase: StringBuilder = new StringBuilder()
      var strCondition: String = "%"
      var step = 0
      for (item <- listFunnelSteps) {
        step = step + 1
        strCondition = strCondition + item + "%"
        if (!dynamicCase.isEmpty)
          dynamicCase.append(",")
        dynamicCase.append(" sum(case when events like '" + strCondition + s"' then 1 else 0 end) as funnel_step$step")
      }

      val funnelSql = s"select count(1) as total_events, ${dynamicCase.toString()} from user_session_events " +
        s"where event_dt >= '$startDate' and event_dt <='$endDate' "
      funnelSql
    }
    val funnelSql: String = createDynamicSQLForFunnelEvents


    //Execute dynamic SQL and store results in CSV format
    val dfFunnel = sqlContext.sql(funnelSql)
      .write.csv(outputDir)
  }


  def parseArguments(args: Array[String]):(String, String, String, String) = {
    var startDate: String = ""
    var endDate: String = ""
    var funnelEvents: String = ""
    var inputPartDir = ""

    if (args.length < 3)
      throw new Exception("Not all required parameter passed!")

    for (arg <- args) {
      val pair = arg.split("=")
      if (arg.contains("--start_date="))
        startDate = pair(1)
      if (arg.contains("--end_date="))
        endDate = pair(1)
      if (arg.contains("--funnel_events="))
        funnelEvents = pair(1)
      if (arg.contains("--input_dir="))
        inputPartDir = pair(1)
    }
    return (startDate, endDate, funnelEvents, inputPartDir)
  }

  //Function to read Etl configuration
  def getEtlConfiguration(inputPartDir: String, dataRoot: String): (String, String) = {
    val separator = '|'
    val cal = Calendar.getInstance()
    val inputDir = dataRoot + "rollup/rollup_events_by_user_session/" + inputPartDir
    val outputDir = dataRoot + "rollup/funnel_events/" + cal.getTime.getTime()
    (inputDir, outputDir)
  }

  //Function to read application configuration
  def getApplicationConfiguration: (String, String, String) = {
    //Application Configuration
    val appConf = ConfigFactory.load()
    val env = appConf.getString("common.environment")
    val sparkMaster = appConf.getString(env + ".sparkMaster")
    val dataRoot = appConf.getString(env + ".dataRoot")
    (env, sparkMaster, dataRoot)
  }
}
