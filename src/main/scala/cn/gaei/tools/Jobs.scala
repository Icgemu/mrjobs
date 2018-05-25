package cn.gaei.tools

import cn.gaei.tools.jobs.Job
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

class Jobs {

  val jobs= ListBuffer[Job]()

  def run(sc:SparkSession, df:DataFrame, cfg:Map[String,String]): Unit ={
      jobs.foreach(j=>{j.run(sc, df, cfg)})
  }


  def register(job:Job):Jobs = {
    jobs += job
    this
  }

}
