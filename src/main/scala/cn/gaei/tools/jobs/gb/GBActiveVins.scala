package cn.gaei.tools.jobs.gb

import cn.gaei.tools.jobs.Job
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql.EsSparkSQL

class GBActiveVins extends Job{
  override def run(sc: SparkSession, df: DataFrame, cfg: Map[String, String]): Unit = {
    import sc.implicits._
    val act = df.select($"d".cast(StringType).as("day"),$"vin",$"vintype").groupBy($"day",$"vintype")
      .agg(count($"*").as("doc_cnt"),countDistinct($"vin").as("vins_distinct"))
      .withColumn("ts",unix_timestamp($"day","yyyyMMdd")*1000)
      .withColumn("id", concat($"vintype", lit("-"), $"day"))

//    act.printSchema()
    val es_cfg = cfg + ("es.mapping.timestamp"->"ts" ,"es.mapping.id"->"id")
    save(act, "wh/stat", es_cfg)
  }
}
