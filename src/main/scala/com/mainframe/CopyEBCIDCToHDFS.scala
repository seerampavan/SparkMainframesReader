package com.mainframe

import java.io.{File, FileOutputStream}
import com.common.Configuration._
import org.apache.log4j.{Level, Logger}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import com.cloudera.sa.copybook.mapreduce.CopybookInputFormat
import net.sf.JRecord.External.{CobolCopybookLoader, CopybookLoader, ExternalRecord}
import net.sf.JRecord.Numeric.Convert
import net.sf.JRecord.Types.Type
import org.apache.log4j.Level
import org.apache.spark.sql.{Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import scala.collection.mutable.MutableList
import scala.util.Try

//clusterORLocal=l copybookHDFSInputPath=hdfs://localhost:9000/data1/test1/cpbook/example.cbl dataFileInputPath=hdfs://localhost:9000/data1/test1/data/ outputPath=test3/dataout/ isOverwriteDataOk=yes
object CopyEBCIDCToHDFS {

  def main(args: Array[String]) {

    if (args.length == 0) {
      println("Please provide the arguments")
      return
    }

    //Setting the log levels
    setLogLevels(Level.WARN, Seq("spark", "org", "akka"))

    val tmp_cb2xmlFile = "cb2xml.properties"

    val tmpcb2xml = new File(tmp_cb2xmlFile)
    tmpcb2xml.createNewFile()
    // if file already exists will do nothing
    new FileOutputStream(tmpcb2xml, false);

    val namedArgs = getNamedArgs(args)
    val runLocal = namedArgs("clusterORLocal")
    val copybookHDFSInputPath = namedArgs("copybookHDFSInputPath")
    val dataFileInputPath = namedArgs("dataFileInputPath")
    val outputPath = namedArgs("outputPath")
    val isOverwriteDataOk = namedArgs("isOverwriteDataOk").equalsIgnoreCase("yes")

    val spark: SparkSession = getSparkSession(runLocal, "CopyEBCIDCToHDFS")

    val config: Configuration = new Configuration
    config.addResource(new Path(hadoopClusterProperties.getString("hdfsConfigRootPath")+"hdfs-site.xml"))
    config.addResource(new Path(hadoopClusterProperties.getString("hdfsConfigRootPath")+"mapred-site.xml"))
    config.addResource(new Path(hadoopClusterProperties.getString("hdfsConfigRootPath")+"yarn-site.xml"))
    config.addResource(new Path(hadoopClusterProperties.getString("hdfsConfigRootPath")+"core-site.xml"))
    val fs = FileSystem.get(config)
    val copybookHDFSPath = new Path(copybookHDFSInputPath)

    if(fs.exists(copybookHDFSPath)) {
      CopybookInputFormat.setCopybookHdfsPath(config, copybookHDFSInputPath)
      val rdd = spark.sparkContext.newAPIHadoopFile(dataFileInputPath, classOf[CopybookInputFormat], classOf[LongWritable], classOf[Text], config)

      val pipeDelimiter = rdd.map(line => {
        val cells = line._2.toString.split(new Character(0x01.toChar).toString)
        val strBuilder: StringBuilder = new StringBuilder
        for (cell <- cells) {
          strBuilder.append(cell + "|")
        }
        strBuilder.toString
      })

      val dfRDD = pipeDelimiter.map(x => Row.fromSeq((x.split("\\|")).toSeq))

      val tmp_copybookName :String = s"copybook_"+new DateTime().getMillis
      fs.copyToLocalFile(false, copybookHDFSPath, new Path(tmp_copybookName));
      val schema = getFieldsSchema(tmp_copybookName)

      Try{
        new File(tmp_copybookName).delete()
        new File(s".$tmp_copybookName.crc").deleteOnExit()
        new File(tmp_cb2xmlFile).deleteOnExit()
      }.getOrElse(println(s"Unable to delete the local copybook file $copybookHDFSPath"))

      val dataframe = spark.createDataFrame(dfRDD, schema)
      dataframe.printSchema()
      dataframe.show()

      dataframe.write.mode(if (isOverwriteDataOk) SaveMode.Overwrite else SaveMode.Append).save(outputPath)
    }else{
      println(s"Copy book doesnot exists at $copybookHDFSPath")
    }
  }

  def getFieldsSchema(cbl: String):StructType = {

    val copybookInt: CopybookLoader = new CobolCopybookLoader
    val externalRecord: ExternalRecord = copybookInt.loadCopyBook(cbl, CopybookLoader.SPLIT_NONE, 0, "cp037", Convert.FMT_MAINFRAME, 0, null)

    var recordLength: Int = 0
    var lastColumn: String = ""
    var repeatCounter: Int = 1
    var fieldsList: MutableList[String] = new MutableList[String]

    for (field <- externalRecord.getRecordFields) {

      val `type`: Int = field.getType
      var typeString: String = "Unknown"
      val hiveType: String = "STRING"
      if (`type` == Type.ftChar) {
        typeString = "ftChar"
      }
      else if (`type` == Type.ftPackedDecimal) {
        typeString = "packedDecimal"
      }
      else if (`type` == Type.ftBinaryBigEndian) {
        typeString = "BinaryBigEndian"
      }
      else if (`type` == Type.ftZonedNumeric) {
        typeString = "ftZonedNumeric"
      }
      println(field.getCobolName + "\t" + typeString + "\t" + `type` + "\t" + field.getLen + "\t" + field.getDescription + "\t" + field.getCobolName)
      recordLength += field.getLen
      var columnName: String = field.getCobolName
      columnName = columnName.replace('-', '_')
      if (lastColumn == columnName) {
        columnName = columnName + ({
          repeatCounter += 1; repeatCounter - 1
        })
      }
      else {
        repeatCounter = 1
        lastColumn = columnName
      }
      fieldsList +=columnName
    }

    if (!fieldsList.isEmpty)
      StructType(fieldsList.map(f => StructField(f, StringType, true)))
    else
      null
  }

  def setLogLevels(level: Level, loggers: Seq[String]): Map[String, Level] = loggers.map(loggerName => {
    val logger = Logger.getLogger(loggerName)
    val prevLevel = logger.getLevel
    logger.setLevel(level)
    loggerName -> prevLevel
  }).toMap


  def getSparkSession(runLocal: String, appName: String) = {
    val sparkConfig = new SparkConf()
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Test1")
      .config(sparkConfig)
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration.setBoolean("parquet.enable.summary-metadata", false)
    spark
  }

  def getNamedArgs(args: Array[String]): Map[String, String] = {

    println("################### Input parameters are ############### " + args.toList)
    args.filter(line => line.contains("=")) //take only named arguments
      .map(x => {
      val key = x.substring(0, x.indexOf("="))
      val value = x.substring(x.indexOf("=") + 1)
      (key, if (value == null || "".equalsIgnoreCase(value)) null else value)
    }).toMap //convert to a map
  }

}
