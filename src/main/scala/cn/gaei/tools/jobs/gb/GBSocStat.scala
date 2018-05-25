package cn.gaei.tools.jobs.gb

import java.io.PrintWriter

import cn.gaei.tools.jobs.ag.SocStat
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.elasticsearch.spark.sql.EsSparkSQL

class GBSocStat  extends SocStat{
  override def chargeSt(e: Seq[Seq[Double]]): Int = {
    //val e = m.map(_.map(Option(_)))
    var f = 0
    if (e.size == 2) {
      val t1 = e(0)(0).toLong
      val t2 = e(1)(0).toLong

      val c1 = if (Option(e(0)(1)).isEmpty) 0 else e(0)(1).toLong
      val c2 = if (Option(e(1)(1)).isEmpty) 0 else e(1)(1).toLong

      val s1 = if (Option(e(0)(2)).isEmpty) -1 else e(0)(2).toInt
      val s2 = if (Option(e(1)(2)).isEmpty) -1 else e(1)(2).toInt

      val tolerance = t2 - t1 < 300 * 1000

      if (tolerance && (s1 != 1) && (s2 == 1)) {
        f = 1
      } else if (tolerance && (s1 == 1) && (s2 == 1)) {
        f = 2
      } else if (tolerance && (s2 != 1) && (s1 == 1)) {
        f = 3
      } else if (!tolerance) {
        if(c2>0 && s2 == 1){f = 1}else{f = 3}
      }
    }
    //first point for window
    if (e.size == 1) {
      val s1 = e(0)(2).toInt
      val c1 = if (Option(e(0)(1)).isEmpty) 0 else e(0)(1).toLong
      if (s1 == 1 && c1 > 0) {
        f = 2
      }
    }
    f
  }

  override def run(sc: SparkSession, df: DataFrame, cfg: Map[String, String]): Unit = {
    import sc.implicits._

    sc.udf.register("chargeSt", (e: Seq[Seq[Double]]) => {
      chargeSt(e)
    })

    sc.udf.register("chargeSegment", (e: Seq[String]) => {
      val arr = e.map(_.split(",")).map(e => {Segment(e(0), e(1).toInt)})
      chargeSegment(arr)
    })
    sc.udf.register("merge", (e: Seq[Row]) => {
      merge(e)
    })

    sc.udf.register("chargeCal", (in: Seq[Seq[Double]]) => {
      val arr = in.map(e => {
        val ts = e(0).toLong
        val volt = if(Option(e(1)).isEmpty) 0 else e(1)
        val curr = if(Option(e(2)).isEmpty) 0 else e(2)
        val soc = if(Option(e(3)).isEmpty) -1 else e(3)
        val lon =  if(Option(e(4)).isEmpty) 0 else e(4)
        val lat = if(Option(e(5)).isEmpty) 0 else e(5)
        InRow(ts, volt, curr, soc,lon,lat)
      })

      chargeCal(arr)
    })

    val ws1 = Window.partitionBy($"vin").orderBy($"ts").rowsBetween(-1, 0)
    val ws2 = Window.partitionBy($"vin").orderBy($"ts").rowsBetween(Window.unboundedPreceding, 0)
    val orig = df
//      .filter($"vin".equalTo("LMGAJ1S83H1000990"))
      .select($"vin", $"vintype",$"ts", $"esd_curr", $"esd_volt", $"veh_chargeSt", $"veh_soc",$"loc_lon84",$"loc_lat84")
      .withColumn("chargeSt", callUDF("chargeSt", collect_list(array($"ts", $"esd_volt", $"veh_chargeSt")).over(ws1)))
      .withColumn("chargeId", callUDF("chargeSegment", collect_list(concat($"vin",lit(","),$"chargeSt")).over(ws1)))
      .filter($"chargeId" > 0)
    //.withColumn("chargeId", sum($"chargeSegment").over(ws2))

//        val writer = new PrintWriter("LMGAJ1S83H1000990.csv")
//
//        orig.collect().foreach(e => {
//          writer.write(e.mkString(",") + "\n")
//        })
//        writer.close()
    val act = orig.groupBy($"vin", $"vintype",$"chargeId").agg(callUDF("chargeCal",
          collect_list(array($"ts", $"esd_curr", $"esd_volt", $"veh_soc",$"loc_lon84",$"loc_lat84"))).as("res"))
      .filter($"res.st">0)
      .groupBy($"vin",$"vintype")
      .agg(callUDF("merge",collect_list($"res")).as("m"))
      .select($"vin",$"vintype",explode($"m").as("data"))
      .filter($"data.st".equalTo(1) && $"data.change_time_in_second" > 5*60 )
      .withColumn("id", concat($"vin", lit("-"), $"data.start_time"))
      .withColumn("ts", $"data.start_time")
    val es_cfg = cfg + ("es.mapping.timestamp" -> "ts", "es.mapping.id" -> "id")
    save(act, "wh/soc", es_cfg)
  }
}
