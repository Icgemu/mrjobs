package cn.gaei.tools.jobs.gb

import cn.gaei.tools.jobs.ag.ChargingVol
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql.EsSparkSQL

class GBChargingVol extends ChargingVol {
  override def run(sc: SparkSession, df: DataFrame, cfg: Map[String, String]): Unit = {
    import sc.implicits._

    sc.udf.register("chargingUdf", (in: Seq[Seq[Double]]) => {
      val arr = in.map(e => {
        val ts = e(0).toLong
        val volt = e(1)
        val curr = e(2)
        val soc = e(3)
        InRow(ts, volt, curr, soc)
      })

      chargingCal(arr)
    })

    val act = df.filter($"veh_chargeSt".equalTo(1) && $"esd_volt".isNotNull && $"esd_curr" < 0)
      .select($"vin",$"vintype", $"ts", $"d".cast(StringType).as("day"), $"esd_volt", $"esd_curr",$"veh_soc")
      .groupBy($"vin",$"day",$"vintype")
      .agg(collect_list(array($"ts",  $"esd_volt", abs($"esd_curr"),$"veh_soc")).as("collset"))
      .select($"vin",$"vintype",$"day", callUDF("chargingUdf", $"collset").as("data"))
      .withColumn("ts", unix_timestamp($"day", "yyyyMMdd")*1000)
      .withColumn("id", concat($"vin", lit("-"), $"day"))

//    act.printSchema()
    val es_cfg = cfg + ("es.mapping.timestamp" -> "ts", "es.mapping.id" -> "id")
    save(act, "wh/charging", es_cfg)
  }
}
