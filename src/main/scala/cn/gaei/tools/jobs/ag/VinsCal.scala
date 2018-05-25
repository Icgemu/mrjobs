package cn.gaei.tools.jobs.ag

import cn.gaei.tools.jobs.Job
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql.EsSparkSQL


class VinsCal extends Job {
  override def run(sc:SparkSession, df: DataFrame, cfg:Map[String,String]): Unit = {

    import sc.implicits._
    val act = df.select($"date_str",$"vin").groupBy($"vin",$"date_str")
      .agg(count($"*").as("doc_cnt"))
      .select($"vin",$"date_str".as("day"),$"doc_cnt",lit("AG").as("vintype"))
      .withColumn("ts",unix_timestamp($"day","yyyyMMdd")*1000)
      .withColumn("id", concat($"vin", lit("-"), $"day"))

//    act.printSchema()
    val es_cfg = cfg + ("es.mapping.timestamp"->"ts" ,"es.mapping.id"->"id")
    save(act, "wh/vins", es_cfg)
  }
}
