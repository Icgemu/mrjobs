package cn.gaei.tools.jobs.ag

import cn.gaei.tools.jobs.Job
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql.EsSparkSQL


class ActiveVins extends Job {
  override def run(sc:SparkSession, df: DataFrame, cfg:Map[String,String]): Unit = {

    import sc.implicits._
    val act = df.select($"date_str",$"vin").groupBy($"date_str")
      .agg(count($"*").as("doc_cnt"), countDistinct($"vin").as("vins_distinct"))
      .select($"date_str".cast(ByteType).as("day"),$"doc_cnt",$"vins_distinct",lit("AG").as("vintype"))
      .withColumn("ts",unix_timestamp($"day","yyyyMMdd")*1000)
      .withColumn("id", concat($"vintype", lit("-"), $"day"))

//    act.printSchema()
    val es_cfg = cfg + ("es.mapping.timestamp"->"ts" ,"es.mapping.id"->"id")
    save(act, "wh/stat", es_cfg)
  }
}
