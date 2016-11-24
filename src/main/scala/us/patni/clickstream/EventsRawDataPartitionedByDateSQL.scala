package us.patni.clickstream

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by Bhupendra Patni on 11/20/16.
  * Object to load raw data in .gz format in the
  * input directory to transfomrm into parquet format
  * and store the parquet files in the output directory
  */
object EventsRawDataPartitionedByDateSQL {

  def main(args: Array[String]): Unit = {
    //Load application configuration variables
    val (sparkMaster: String, dataRoot: String) = getApplicationConfig

    //Read filename from the command line arguments
    var filename:String = ""
    if (args.length > 0)
      filename = args(0)
    else
      throw new Exception("Filename parameter not passed!")

    //load spark session object
    val spark = new SparkSession.Builder().
      appName("ClickStream.SparkDataPipeline").
      master(sparkMaster).
      getOrCreate()

    import spark.implicits._

    //Defined field list to generate schema based on the string with String type
    val fieldList="event_dt,session_id,user_id,event_ts,event"
    val fields = fieldList.split(",")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    //ETL Variables to
    val (separator: Char, outputDir: String) = readEtlVariables(dataRoot)

    val csRdd = spark.sparkContext.textFile(dataRoot + filename)
      .map(line => line.split(separator))
      .map(
      cols => try {
        Row(cols(2).substring(0,10), cols(27), cols(0), //Key = sesssion_id, user_id
            cols(2), cols(4) + ":" + cols(5) + ":" + cols(3) //category+":"+action+":"+event
            )
      }
        finally {}
    )
    //Create Data Frame with pre-defined schema
    val csDf = spark.createDataFrame(csRdd, schema)

    csDf.write.partitionBy("event_dt").parquet(outputDir)
  }

  def readEtlVariables(dataRoot: String): (Char, String) = {
    val separator = '|'
    val outputDir = dataRoot + "parquet/fact_clickstream/"
    (separator, outputDir)
  }

  def getApplicationConfig: (String, String) = {
    val appConf = ConfigFactory.load()
    val env = appConf.getString("common.environment")
    val sparkMaster = appConf.getString(env + ".sparkMaster")
    val dataRoot = appConf.getString(env + ".dataRoot")
    (sparkMaster, dataRoot)
  }

}
