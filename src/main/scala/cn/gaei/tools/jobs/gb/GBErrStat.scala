package cn.gaei.tools.jobs.gb

import cn.gaei.tools.jobs.Job
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql.EsSparkSQL

class GBErrStat extends Job{

  case class err(
  alm_common_temp_diff:Int,
  alm_common_temp_high:Int,
  alm_common_esd_high:Int,
  alm_common_esd_low:Int,
  alm_common_soc_low:Int,
  alm_common_sc_high:Int,
  alm_common_sc_low:Int,
  alm_common_soc_high:Int,
  alm_common_soc_hop:Int,
  alm_common_esd_unmatch:Int,
  alm_common_sc_consistency:Int,
  alm_common_insulation:Int,
  alm_common_dcdc_temp:Int,
  alm_common_brk:Int,
  alm_common_dcdc_st:Int,
  alm_common_dmc_temp:Int,
  alm_common_hvil_st:Int,
  alm_common_dm_temp:Int,
  alm_common_esd_charge_over:Int,
  err_cnt:Int,err_doc_cnt:Int
                )

  override def run(sc: SparkSession, df: DataFrame, cfg: Map[String, String]): Unit = {
    import sc.implicits._

    sc.udf.register("err", (e:Seq[Seq[Int]])=>{
      val buf = Array.ofDim[Int](19)
      var err_doc_cnt = 0;
      e.foreach(signal =>{
        var i = 0;
        val sum_1 = buf.sum
        signal.foreach(j=>{
          if(j != null && j>0){buf(i) = buf(i)+1}
          i= i+1
        })
        val sum_2 = buf.sum
        if(sum_2 > sum_1){err_doc_cnt += 1}
      })
      err(buf(0),buf(1),buf(2),buf(3),buf(4),buf(5),buf(6),
        buf(7),buf(8),buf(9),buf(10),buf(11),buf(12),buf(13),
        buf(14),buf(15),buf(16),buf(17),buf(18),buf.sum,err_doc_cnt)
    })

    val orig = df.withColumn("errsignals",array(
      $"alm_common_temp_diff",
      $"alm_common_temp_high",
      $"alm_common_esd_high",
      $"alm_common_esd_low",
      $"alm_common_soc_low",
      $"alm_common_sc_high",
      $"alm_common_sc_low",
      $"alm_common_soc_high",
      $"alm_common_soc_hop",
      $"alm_common_esd_unmatch",
      $"alm_common_sc_consistency",
      $"alm_common_insulation",
      $"alm_common_dcdc_temp",
      $"alm_common_brk",
      $"alm_common_dcdc_st",
      $"alm_common_dmc_temp",
      $"alm_common_hvil_st",
      $"alm_common_dm_temp",
      $"alm_common_esd_charge_over"
    ))
      .select($"vin",$"vintype",$"d", $"errsignals")

    val act = orig
      .groupBy($"vin",$"vintype",$"d").agg(count($"*").as("doc_cnt"),collect_list($"errsignals") as("collset"))
      .select($"vin",$"vintype",$"d".cast(StringType).as("day"), $"doc_cnt",callUDF("err",$"collset").as("data"))
      .withColumn("ts", unix_timestamp($"day", "yyyyMMdd")*1000)
      .withColumn("id", concat($"vin", lit("-"), $"day"))

//    act.printSchema()
    val es_cfg1 = cfg + ("es.mapping.timestamp" -> "ts", "es.mapping.id" -> "id")
    save(act, "wh/err_by_day", es_cfg1)

  }
}
