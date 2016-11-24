package us.patni.clickstream

import java.text.SimpleDateFormat
import java.util.Calendar

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel


/**
  * Created by Bhupendra Patni on 11/21/16.
  * The object takes --start_date, --end_date, --funnel_events & --input_dir
  * as a command line arguments to process funnel dynamically and write output
  * to a CSV file in the pre-defined output directory
  */
object EventTimestampFunnelAnalysis {

  def main(args: Array[String]): Unit = {
    //Load command line arguments
    var (startDate: String, endDate: String, funnelEvents:String, inputPartDir: String) = getCommandLineArguments(args)

    //Get Application Configuration
    val (sparkMaster: String, dataRoot: String) = getApplicationConfig

    //Load spark session with the configuration
    val spark = new SparkSession.Builder().
      appName("ClickStream.SparkDataPipeline").
      master(sparkMaster).
      getOrCreate()
    val sqlContext = spark.sqlContext

    import spark.implicits._

    //Create and register UDF to validate Funnel events and if it happened within 30 minutes
    import org.apache.spark.sql.functions.udf
    val udfValidateFunnelEvents = udf((userEvents:String, funnelEvents:String) =>  {
      {
        val funnelEventsMap = funnelEvents.split(",")
        val cal = Calendar.getInstance()
        val dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        var mapEvent: Map[String, Long] = Map()
        val eventsMap = userEvents.split("],")
          .map(event => userEvents.replace("]","").split(","))
          .map(item => {
            mapEvent += (item(1) -> dateTimeFormat.parse(item(0).substring(1)).getTime())
          })
        var lastEventTS: Long = 0
        var currentEventTS: Long = 0
        var firstFlag = true
        var isFailed = false

        for (event <- funnelEventsMap;if isFailed==false) {
          if (mapEvent.contains(event)) {
            currentEventTS = mapEvent.get(event).get
            val timeDifferenceInMinutes = (currentEventTS - lastEventTS) / (1000 * 60)
            if (firstFlag == true || (timeDifferenceInMinutes >=0 && timeDifferenceInMinutes <= 30)) {
              lastEventTS = currentEventTS
              firstFlag = false
            }
            else {
              isFailed=true
            }
          }
          else {
            isFailed = true
          }
        }
        val success:Int = 1
        val failed:Int = 0
        if (isFailed) (failed) else (success)
      }
    }
    )
    sqlContext.udf.register("validateFunnelEvents", (userEvents:String, funnelEvents:String) => udfValidateFunnelEvents)

    //Create data processing local variables
    val cal = Calendar.getInstance()
    val inputDir = dataRoot + "rollup/rollup_events_timestamp_by_user_session/" + inputPartDir
    val outputDir = dataRoot + "rollup/funnel_events/" + cal.getTime.getTime()

    //Load Parquet data from all files across all partitions
    val dfUserSessionEvents = sqlContext.read
      .option("mergeSchema", "true")
      .parquet(inputDir)
    dfUserSessionEvents.createOrReplaceTempView("user_session_events")

    //Create Dynamic SQL with the funnel events provided dynamically
    val funnelSql: String = createDynamicSQLForFunnelEvents(funnelEvents, startDate, endDate)

    //Persist & cache data in cache for next set of calls
    val dfFunnel = sqlContext.sql(funnelSql)
    dfFunnel.persist(StorageLevel.MEMORY_ONLY)
    dfFunnel.cache()

    //Write funnel results to a CSV file in the output folder
    dfFunnel.write.csv(outputDir)
  }

  def getApplicationConfig: (String, String) = {
    //Application Configuration
    val appConf = ConfigFactory.load()
    val env = appConf.getString("common.environment")
    val sparkMaster = appConf.getString(env + ".sparkMaster")
    val dataRoot = appConf.getString(env + ".dataRoot")
    (sparkMaster, dataRoot)
  }

  def createDynamicSQLForFunnelEvents(funnelEvents:String, startDate:String, endDate:String): String = {
    val listFunnelSteps = funnelEvents.split(",").toList
    var dynamicCase: StringBuilder = new StringBuilder()
    var strCondition: String = "%"
    var comma = ","
    var step = 0
    for (item <- listFunnelSteps) {
      if (step == 0)
        strCondition = item
      else
        strCondition = strCondition + "," + item
      step = step + 1
      if (!dynamicCase.isEmpty)
        dynamicCase.append(comma)
      dynamicCase.append(" udfValidateFunnelEvents('" + strCondition + s"')  as funnel_step$step")
    }
    val funnelSql = s"select count(1) as total_events, ${dynamicCase.toString()} from user_session_events " +
      s"where event_dt >= '$startDate' and event_dt <='$endDate' "
    funnelSql
  }

  //Function to read all command line arguments
  def getCommandLineArguments(args: Array[String]): (String, String, String, String) = {
    var startDate: String = ""
    var endDate: String = ""
    var funnelEvents: String = ""
    var inputPartDir = ""
    //Read Command Line Arguments
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
    (startDate, endDate, funnelEvents, inputPartDir)
  }


}
