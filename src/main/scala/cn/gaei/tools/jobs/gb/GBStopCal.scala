package cn.gaei.tools.jobs.gb

import cn.gaei.tools.jobs.Job
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.elasticsearch.spark.sql.EsSparkSQL

class GBStopCal extends Job{
  case class Out(st:Int,start_time:Long, end_time:Long, duration:Long, stop_location:String)
  override def run(sc: SparkSession, df: DataFrame, cfg: Map[String, String]): Unit = {

    sc.udf.register("stop", (e:Seq[Row]) =>{
      var out = Out(0,0l,0l,0l,"")
      if(e.size == 2){
        val ts1 = e(0).getLong(0)
        val ts2 = e(1).getLong(0)

        if(ts2 -ts1 > 5*60 *1000){
          val duration_in_second = (ts2 -ts1)/1000
          val location = (e(0).getDouble(2)+e(1).getDouble(2))/2+","+(e(0).getDouble(3)+e(1).getDouble(3))/2
          out = Out(1, ts1,ts2,duration_in_second,location)
        }
      }
      out
    })

    import sc.implicits._

    val ws1 = Window.partitionBy($"vin").orderBy($"ts").rowsBetween(0, 1)
    val act = df
//      .filter($"vin".equalTo("LMGGN1S54G1002993")).orderBy($"ts")
      .select($"vin",$"vintype", $"ts", $"veh_st",  $"veh_spd", $"loc_lon84", $"loc_lat84")
      .filter( $"veh_st".equalTo(1) && $"veh_spd" >0 && $"loc_lon84".isNotNull)
      .withColumn("data", callUDF("stop",collect_list(struct($"ts",$"veh_spd",$"loc_lon84",$"loc_lat84")).over(ws1)))
      .filter($"data.st" >0 )
        .select($"vin",$"vintype",$"data",$"ts",concat($"vin",lit("-"),$"ts").as("id"))

    val es_cfg = cfg + ("es.mapping.timestamp" -> "ts", "es.mapping.id" -> "id")
    save(act, "wh/stop", es_cfg)
//    val writer = new PrintWriter("LMGGN1S54G1002993.csv")
//
//    act.collect().foreach(e => {
//      writer.write(e.mkString(",") + "\n")
//    })
//    writer.close()
  }
}
