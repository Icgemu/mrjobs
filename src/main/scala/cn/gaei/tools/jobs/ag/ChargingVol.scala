package cn.gaei.tools.jobs.ag

import cn.gaei.tools.jobs.Job
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql.EsSparkSQL

class ChargingVol extends Job{

  case class InRow(ts: Long, volt: Double, curr: Double, soc: Double)
  case class Out(count: Long, charge_time_in_second: Long, chargeVol_in_kwh: Double,start_soc:Double,end_soc:Double)

  def chargingCal(a:Seq[InRow]) = {
    val arr = a.sortWith((a1, a2) => a1.ts < a2.ts)

    var lastRow = arr.head
    val tail = arr.tail

    var W_sum = 0d
    var time_sum = 0l

    var maxSoc = lastRow.soc
    var isUpper = true
    tail.foreach(e=>{
      val t1 = lastRow.ts
      val t2 =  e.ts

      val v1 = lastRow.volt
      val v2 = e.volt

      val c1 = lastRow.curr
      val c2 = e.curr

      val timeDelta = t2 - t1

      //计算charging

      val W1 = (v1 * c1 + v2 * c2) * timeDelta / 1000 / 2 / (3600 * 1000)
      //因时间间隔短，W1基本为0.0几，若大于1（暂定阈值）即为异常
      //必须加入timeDelta<1min判定  避免出现连续两条数据之间时间间隔过大，导致W1过大
      if (W1 != 0 && timeDelta> 0 && timeDelta < 1 * 60 * 1000 && W1 < 1) { //电功累加
        W_sum += W1
        time_sum += timeDelta / 1000
      }

      if(e.soc >= maxSoc && isUpper) {maxSoc = e.soc} else{isUpper = false}
      lastRow = e
    })

    Out(arr.size, time_sum,W_sum, arr.head.soc, maxSoc)
  }

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

    val act = df.filter($"ccs_chargerstartst".equalTo(0) && $"ccs_chargevolt".isNotNull &&$"ccs_chargecur".isNotNull)
      .select($"vin", $"ts", $"date_str", $"ccs_chargevolt", $"ccs_chargecur",$"bms_battsoc")
      .groupBy($"vin",$"date_str")
      .agg(collect_list(array($"ts",  $"ccs_chargevolt", $"ccs_chargecur",$"bms_battsoc")).as("collset"))
      .select($"vin", $"date_str".as("day"), callUDF("chargingUdf", $"collset").as("data"),lit("AG").as("vintype"))
      .withColumn("ts", unix_timestamp($"day", "yyyyMMdd")*1000)
      .withColumn("id", concat($"vin", lit("-"), $"day"))

//    act.printSchema()
    val es_cfg = cfg + ("es.mapping.timestamp" -> "ts", "es.mapping.id" -> "id")
    save(act, "wh/charging", es_cfg)
  }
}
