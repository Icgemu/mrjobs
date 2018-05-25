package cn.gaei.tools.jobs.gb

import java.io.PrintWriter

import cn.gaei.tools.api.Tools
import cn.gaei.tools.jobs.Job
import cn.gaei.tools.jobs.ag.TripCal
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.elasticsearch.spark.sql.EsSparkSQL

import scala.collection.mutable.ArrayBuffer

class GBTripCal extends TripCal {

  override def tripst(e: Seq[Seq[Double]]): Int = {
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
        if(s2 == 1){f = 1}else{f = 3}
      }
    }
    //first point
    if (e.size == 1) {
      val s1 = e(0)(2).toInt
      val c1 = if (Option(e(0)(1)).isEmpty) 0 else e(0)(1).toLong
      if (s1 == 1) {
        f = 2
      }
    }
    f
  }



  override def run(sc: SparkSession, df: DataFrame, cfg: Map[String, String]): Unit = {

    import sc.implicits._

    sc.udf.register("tripst", (e: Seq[Seq[Double]]) => {
      tripst(e)
    })

    sc.udf.register("tripSegment", (e: Seq[String]) => {
      val arr = e.map(_.split(",")).map(e => {Segment(e(0), e(1).toInt)})
      tripSegment(arr)
    })
    sc.udf.register("merge", (e: Seq[Row]) => {
      merge(e)
    })
    sc.udf.register("mileUdf", (in: Seq[Seq[Double]]) => {
      mile(in)
    })
    val ws1 = Window.partitionBy($"vin").orderBy($"ts").rowsBetween(-1, 0)

    val act = df
//        .filter($"vin".equalTo("LMGFJ1S50H1000540"))
      .filter($"loc_lon84" > 72.004 && $"loc_lon84" < 137.8347 && $"loc_lat84" > 0.8293 && $"loc_lat84" < 55.8271 && $"veh_odo"> 0)
      .select($"vin",$"vintype", $"ts", $"veh_st", $"veh_spd", $"eng_spd", $"veh_odo", $"eng_consumption", $"loc_lon84", $"loc_lat84")
      .withColumn("keyst", when($"veh_st".equalTo(1), 1).otherwise(0))
      .withColumn("tripst", callUDF("tripst", collect_list(array($"ts", $"veh_spd", $"keyst")).over(ws1)))
      .withColumn("tripId", callUDF("tripSegment", collect_list(concat($"vin",lit(","),$"tripst")).over(ws1)))
      .filter($"tripId" > 0)
      .groupBy($"vin", $"vintype",$"tripId")
      .agg(count($"*").as("cnt"),collect_list(array($"ts", $"eng_spd", $"veh_odo", $"eng_consumption", $"loc_lon84", $"loc_lat84")).as("collset"))
      .filter($"cnt">2)
      .select($"vin", $"vintype", callUDF("mileUdf", $"collset").as("res"))
      .groupBy($"vin",$"vintype").agg(callUDF("merge",collect_list($"res")).as("m"))
      .select($"vin",$"vintype", explode($"m").as("data"))
      .filter($"data.end_time" - $"data.start_time"  > 5*60*1000 && $"data.odoLen" > 0)
      .withColumn("ts", $"data.start_time")
      .withColumn("id", concat($"vin", lit("-"), $"data.start_time"))

    val es_cfg = cfg + ("es.mapping.timestamp" -> "ts", "es.mapping.id" -> "id")
    save(act, "wh/trip", es_cfg)


//        val writer = new PrintWriter("LMGFJ1S50H1000540.csv")
//
//        act.collect().foreach(e => {
//          writer.write(e.mkString(",") + "\n")
//        })
//        writer.close()
  }
}
