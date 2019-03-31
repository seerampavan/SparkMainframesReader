package com.common

import com.typesafe.config.{Config, ConfigFactory}
import scala.io.Source
import scala.util.Try

object Configuration {

  private lazy val config:Config = readConfig()
  val properties = System.getProperties

  private def readConfig(): Config ={
    val confFile = "application_dev.conf"
    val filesList = properties.getProperty("spark.yarn.dist.files")
    try{
      val fileReader =
        if(filesList!= null && !filesList.isEmpty ){
          val files = filesList.split(",").filter(_.contains(confFile)).map(_.replace("file:", ""))
          Try(Source.fromFile(files(0)).bufferedReader()).getOrElse(null)
        }else{
          Source.fromFile(confFile).bufferedReader()
        }

      if(fileReader == null ) throw new Exception(s"Unable to read the $confFile configuration file ")
      ConfigFactory.parseReader(fileReader)
    }catch {
      case e: Exception=>{
        e.printStackTrace
        ConfigFactory.load("application_dev")
      }
    }
  }

  def hadoopClusterProperties =  config.getConfig("app.hadoopcluster-properties")
}
