package us.patni.clickstream

import java.text.SimpleDateFormat
import java.util.Calendar

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Bhupendra Patni on 11/19/16.
  * The class takes the input file pattern as a command line argument
  * to group events & its timestamp by User and write results in the
  * output folder in parquet format
  * */
object EventTimestampGroupingByUserSession {

  def main(args: Array[String]): Unit = {
    val (sparkMaster: String, dataRoot: String) = getApplicationConfig

    var filename:String = ""
    if (args.length > 0)
      filename = args(0)
    else
      throw new Exception("Filename parameter not passed!")

    //Create spark session object
    val spark = new SparkSession.Builder().
      appName("ClickStream.SparkDataPipeline").
      master(sparkMaster).
      getOrCreate()

    // Generate the schema based on the string of schema
    val fieldList="user_id,session_id,event_dt,session_min_ts,session_max_ts,events"
    val fields = fieldList.split(",")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    //local ETL variables to process the file
    val separator = '|'
    val cal = Calendar.getInstance()
    val dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val inputDir = dataRoot + filename
    val outputDir = dataRoot + "rollup/rollup_events_timestamp_by_user_session/" + cal.getTime().getTime()

    //Load all files from the input folder
    val csRdd = spark.sparkContext.textFile(inputDir)
      .map(line => line.split(separator))
      .map[((String, String), (Long, Long, String))] (

      cols => try {
          ((cols(27), cols(0)), //Key = sesssion_id, user_id
            ( //Values = min_session_ts, max_session_ts, comma_separated_events
              dateTimeFormat.parse(cols(2)).getTime(),
              dateTimeFormat.parse(cols(2)).getTime(),
              "[" + cols(2) + "," +cols(4) + ":" + cols(5) + ":" + cols(3)+ "]" //category+":"+action+":"+event
              ))
      }
      finally {}
    )
      .reduceByKey((v1,v2) =>
        (Math.min(v1._1, v2._1), Math.max(v1._2, v2._2),
          v1._3 + "," + v2._3))
      .map(pair => Row(pair._1._1, pair._1._2, dateFormat.format(pair._2._1), dateTimeFormat.format(pair._2._1), dateTimeFormat.format(pair._2._2), pair._2._3))

    //Create data frame based on the schema
    val csDf = spark.createDataFrame(csRdd, schema)
    //Register data frame as a temporary view
    csDf.createOrReplaceTempView("rollup_event_timestamp_by_user_session")

    //Write data to the output folder in a parquet format
    csDf.sqlContext.sql("select  * from rollup_event_timestamp_by_user_session order by session_min_ts, events")
      .write.partitionBy("event_dt")
      .parquet(outputDir)

  }

  //Function to read application configuration
  def getApplicationConfig: (String, String) = {
    val appConf = ConfigFactory.load()
    val env = appConf.getString("common.environment")
    val sparkMaster = appConf.getString(env + ".sparkMaster")
    val dataRoot = appConf.getString(env + ".dataRoot")
    (sparkMaster, dataRoot)
  }
}
