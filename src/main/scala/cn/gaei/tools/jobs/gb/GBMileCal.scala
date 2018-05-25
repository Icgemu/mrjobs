package cn.gaei.tools.jobs.gb

import cn.gaei.tools.jobs.ag.MileCal
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql.EsSparkSQL

class GBMileCal extends MileCal{
  override def run(sc: SparkSession, df: DataFrame, cfg: Map[String, String]): Unit = {
    import sc.implicits._

    sc.udf.register("mileUdf", (in: Seq[Seq[Double]]) => {
      mile(in)
    })

    val act = df
      .filter($"loc_lon84" > 72.004 && $"loc_lon84" < 137.8347 && $"loc_lat84" > 0.8293 && $"loc_lat84" < 55.8271)
      .filter($"veh_st".equalTo(1) && $"veh_odo" > 0)
      .select($"vin",$"vintype", $"ts", $"d".cast(StringType).as("day"), $"eng_spd", $"veh_odo", $"eng_consumption", $"loc_lon84", $"loc_lat84")
      .groupBy($"vin",$"vintype", $"day")
      .agg(count($"*").as("cnt"), collect_list(array($"ts", $"eng_spd", $"veh_odo", $"eng_consumption", $"loc_lon84", $"loc_lat84")).as("collset"))
      .filter($"cnt" > 2)
      .select($"vin", $"vintype",$"day", callUDF("mileUdf", $"collset").as("data"))
      .withColumn("ts", unix_timestamp($"day", "yyyyMMdd")*1000).withColumn("id", concat($"vin", lit("-"), $"day"))

//    act.printSchema()
    val es_cfg = cfg + ("es.mapping.timestamp" -> "ts", "es.mapping.id" -> "id")
    save(act, "wh/mile", es_cfg)
  }
}
