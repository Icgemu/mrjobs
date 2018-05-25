package cn.gaei.tools.jobs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql.EsSparkSQL
trait Job extends Serializable {

  def save(df:DataFrame, index:String, cfg:Map[String,String]): Unit ={
    val st = cfg.get("st").get.toLong
    val et = cfg.get("et").get.toLong
    val fl = df.filter(df.col("ts").geq(st) && df.col("ts").lt(et))
    EsSparkSQL.saveToEs(fl, index, cfg)
  }

  def run(sc:SparkSession, df:DataFrame, cfg:Map[String,String])

}
