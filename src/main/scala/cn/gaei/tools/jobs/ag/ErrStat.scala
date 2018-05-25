package cn.gaei.tools.jobs.ag

import cn.gaei.tools.jobs.Job
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.elasticsearch.spark.sql.EsSparkSQL
class ErrStat extends Job{

  case class err(
    bms_bat_error_cell_t_h:Int,
    bms_bat_error_cell_v_h:Int,
    bms_bat_error_cell_v_l:Int,
    bms_bat_error_pack_sumv_h:Int,
    bms_bat_error_pack_sumv_l:Int,
    bms_bat_error_soc_l:Int,
    bms_failurelvl:Int,
    bms_insulationst:Int,
    edb_sterrlvlhves:Int,
    edc_sterrlvlcom:Int,
    ede_sterrlvleng:Int,
    edg_sterrlvlgen:Int,
    edm_sterrlvlmot:Int,
    edv_sterrlvlveh:Int,
    ems_faultranksig:Int,
    mcu_dcdc_stmode:Int,
    mcu_ftm_failst:Int,
    mcu_ftm_fault_info1:Int,
    mcu_ftm_fault_info2:Int,
    mcu_ftm_fault_info3:Int,
    mcu_ftm_stmode:Int,
    mcu_gm_failst:Int,
    mcu_gm_stmode:Int,
    bcs_ebdfaultst:Int,
    bcs_absfaultst:Int,
    edc_sterrlvlcomsup:Int,
    mcu_dcdc_failst:Int,
    bms_bat_error_t_unbalance:Int,
    hcu_oilpressurewarn:Int,
    err_cnt:Int,err_doc_cnt:Int
                )

  override def run(sc: SparkSession, df: DataFrame, cfg: Map[String, String]): Unit = {
    import sc.implicits._

    sc.udf.register("err", (e:Seq[Seq[Int]])=>{
      val buf = Array.ofDim[Int](29)
      var err_doc_cnt = 0;
      e.foreach(signal =>{
        val sum_1 = buf.sum
        if(Array(1,2,3).contains(signal(0)))buf(0) = buf(0)+1
        if(Array(1,2,3).contains(signal(1)))buf(1) = buf(1)+1
        if(Array(1,2,3).contains(signal(2)))buf(2) = buf(2)+1
        if(Array(1,2,3).contains(signal(3)))buf(3) = buf(3)+1
        if(Array(1,2,3).contains(signal(4)))buf(4) = buf(4)+1

        if(1 == signal(5))buf(5) = buf(5)+1
        if(Array(1,2,3,4,5).contains(signal(6)))buf(6) = buf(6)+1
        if(Array(1,2).contains(signal(7)))buf(7) = buf(7)+1

        if(Array(1,2,3).contains(signal(8)))buf(8) = buf(8)+1
        if(Array(1,2,3).contains(signal(9)))buf(9) = buf(9)+1
        if(Array(1,2,3).contains(signal(10)))buf(10) = buf(10)+1
        if(Array(1,2,3).contains(signal(11)))buf(11) = buf(11)+1
        if(Array(1,2,3).contains(signal(12)))buf(12) = buf(12)+1
        if(Array(1,2,3).contains(signal(13)))buf(13) = buf(13)+1

        if(0 != signal(14)) buf(14) = buf(14)+1
        if(5 == signal(15)) buf(15) = buf(15)+1
        if(Array(1,2,3).contains(signal(16)))buf(16) = buf(16)+1

        if(0 != signal(17))buf(17) = buf(17)+1
        if(0 != signal(18))buf(18) = buf(18)+1
        if(0 != signal(19))buf(19) = buf(19)+1
        if(13 ==signal(20))buf(20) = buf(20)+1
        if(Array(1,2,3).contains(signal(21)))buf(21) = buf(21)+1
        if(13 == signal(22))buf(22) = buf(22)+1

//        if(1 == signal(23)) buf(23) = buf(23)+1
//        if(1 == signal(24)) buf(24) = buf(24)+1
//        if(1 == signal(25)) buf(25) = buf(25)+1
//        if(1 == signal(26)) buf(26) = buf(26)+1
//        if(0 != signal(27)) buf(27) = buf(27)+1
//        if(1 == signal(28)) buf(28) = buf(28)+1
        val sum_2 = buf.sum

        if(sum_2> sum_1){err_doc_cnt += 1}
      })
      val err_cnt = buf.sum
      err(buf(0),buf(1),buf(2),buf(3),buf(4),buf(5),buf(6),
        buf(7),buf(8),buf(9),buf(10),buf(11),buf(12),buf(13),
        buf(14),buf(15),buf(16),buf(17),buf(18),buf(19),buf(20),
        buf(21),buf(22),buf(23),buf(24),buf(25),buf(26),buf(27),buf(28)
        ,err_cnt,err_doc_cnt)
    })

    val orig = df.withColumn("errsignals",array(
      $"bms_bat_error_cell_t_h",
      $"bms_bat_error_cell_v_h",
      $"bms_bat_error_cell_v_l",
      $"bms_bat_error_pack_sumv_h",
      $"bms_bat_error_pack_sumv_l",
      $"bms_bat_error_soc_l",
      $"bms_failurelvl",
      $"bms_insulationst",
      $"edb_sterrlvlhves",
      $"edc_sterrlvlcom",
      $"ede_sterrlvleng",
      $"edg_sterrlvlgen",
      $"edm_sterrlvlmot",
      $"edv_sterrlvlveh",
      $"ems_faultranksig",
      $"mcu_dcdc_stmode",
      $"mcu_ftm_failst",
      $"mcu_ftm_fault_info1",
      $"mcu_ftm_fault_info2",
      $"mcu_ftm_fault_info3",
      $"mcu_ftm_stmode",
      $"mcu_gm_failst",
      $"mcu_gm_stmode",
      $"bcs_ebdfaultst",
      $"bcs_absfaultst",
      $"edc_sterrlvlcomsup",
      $"mcu_dcdc_failst",
      $"bms_bat_error_t_unbalance",
      $"hcu_oilpressurewarn"
    ))
      .select($"vin",$"date_str", $"errsignals")

    val act = orig
      .groupBy($"vin",$"date_str").agg(count($"*").as("doc_cnt"),collect_list($"errsignals") as("collset"))
      .select($"vin",$"date_str".as("day"), $"doc_cnt",callUDF("err",$"collset").as("data"),lit("AG").as("vintype"))
      .withColumn("ts", unix_timestamp($"day", "yyyyMMdd")*1000)
      .withColumn("id", concat($"vin", lit("-"), $"day"))

//    act.printSchema()
    val es_cfg1 = cfg + ("es.mapping.timestamp" -> "ts", "es.mapping.id" -> "id")
    save(act, "wh/err_by_day", es_cfg1)

  }
}
