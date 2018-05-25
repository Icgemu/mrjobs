package cn.gaei.tools.jobs.ag

import java.io.PrintWriter

import cn.gaei.tools.api.Tools
import cn.gaei.tools.jobs.Job
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.elasticsearch.spark.sql.EsSparkSQL

import scala.collection.mutable.ArrayBuffer

class TripCal extends Job {

  case class InRow(ts: Long, engspd: Integer,
                   odo: Integer, fuel: Double, lon: Double, lat: Double)

  case class LonLat(lon: Double, lat: Double)

  case class Out(count: Long, start_time:Long, end_time:Long,
                 fuel: Double, gpsLen: Double, elec_gps_len: Long,
                 gas_gps_len: Long, odoLen: Long, elec_odo_len: Long,
                 gas_odo_len: Long,start_location:String, end_location:String)


  case class Segment(vin:String, st:Int)

  val map = collection.mutable.Map[String,Int]()
  def tripst(e: Seq[Seq[Double]]): Int = {
    //val e = m.map(_.map(Option(_)))
    var f = 0
    if (e.size == 2) {
      val t1 = e(0)(0).toLong
      val t2 = e(1)(0).toLong

      val c1 = if (Option(e(0)(1)).isEmpty) 0 else e(0)(1).toLong
      val c2 = if (Option(e(1)(1)).isEmpty) 0 else e(1)(1).toLong

      val s1 = if (Option(e(0)(2)).isEmpty) -1 else e(0)(2).toInt
      val s2 = if (Option(e(1)(2)).isEmpty) -1 else e(1)(2).toInt

      val tolerance = t2 - t1 < 600 * 1000

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

  def tripSegment(e: Seq[Segment]): Int = {

    val vin = e(0).vin
    val f1 = -1
    var f2 = f1
    // ignore the first charging point that span two day...
    if (e.size == 1) {
      val s1 = e(0).st
      if (s1 == 2) {
        f2 = f1
      }
    }
    if (e.size == 2) {
      val s1 = e(0).st
      val s2 = e(1).st

      if (s2 == 1){
        f2 = map.getOrElse(vin, 0)+1
        map += (vin -> f2)
      }
      if (s2 == 2) f2 = map.getOrElse(vin, 0)
      if (s1 == 2 && s2 == 3) f2 = map.getOrElse(vin, 0)
    }

    f2
  }
  def finish(a: Seq[InRow]) = {
    val arr = a.filter(e => {
      e.odo < 1000000 && e.odo > 0
    }).sortWith((a1, a2) => a1.ts < a2.ts)

    try {
      arr.head
    }catch {
      case _: Throwable =>{
        println("err");
        throw new Exception("size:"+a.size+":"+a.mkString("\n"));
      }
    }
    var lastRow = arr.head
    val tail = arr.tail

    var length = 0d
    var time = 0l
    val fuelComsumption_Day = arr.map(_.fuel).filter(!Option(_).isEmpty).sum/arr.map(_.fuel).filter(!Option(_).isEmpty).size

    //gps
    var con_leng = 0d
    var elec_leng = 0d
    var gas_leng = 0d

    //od0
    var conodo_leng = 0d
    var elecodo_leng = 0d
    var gasodo_leng = 0d

    val sum_odometer = arr.last.odo - arr.head.odo

    val dl = arr.last.ts - arr.head.ts
    tail.foreach(e => {
      val t1 = lastRow.ts
      val t2 = e.ts

      val lonlat1 = LonLat(lastRow.lon, lastRow.lat)
      val lonlat2 = LonLat(e.lon, e.lat)

      //val k1 = lastRow.key_st
      //val k2 = e.key_st

      val ep1 = lastRow.engspd
      val ep2 = e.engspd

      val od1 = lastRow.odo
      val od2 = e.odo

      val fuelComsumption = if (Option(lastRow.fuel).isEmpty || Option(e.fuel).isEmpty) 0 else (lastRow.fuel + e.fuel) / 2
      val timeDelta = t2 - t1

      val disDelta = Tools.distance(lonlat1.lat, lonlat1.lon, lonlat2.lat, lonlat2.lon)
      val speed = disDelta * 3600 / timeDelta
      if (timeDelta>0 && timeDelta < 5 * 60 * 1000 && speed < 220 && speed > 0 && fuelComsumption < 100) {
        length += disDelta //meter

        time += timeDelta / 1000 //second

        //fuelComsumption_Day += fuelComsumption / 100000 * disDelta
        ///判断是否纯电：j及j+1点发动机转速为零
        if ((ep1 == 0) && (ep2 == 0)) {
          elec_leng += disDelta
        } else { //判断是否混动：j及j+1点发动机转速不为零
          if ((ep1 != 0) && (ep2 != 0)) {
            gas_leng += disDelta
          } else { //对临界点单独计算:（j点发动机转速为零，j+1不为零）或（j不为零，j+1为零）这类数据点，将s/2分别给gas及electric
            elec_leng += disDelta / 2
            gas_leng += disDelta / 2
          }
        }

        val disodoDelta = od2 - od1
        //km
        val speedodo = disodoDelta * 3600 * 1000 / timeDelta //kilometer/hour
        if (disodoDelta > 0 && disodoDelta < sum_odometer) {
          if ((ep1 == 0) && (ep2 == 0)) {
            //判断是否纯电：j及j+1点发动机转速为零
            elecodo_leng += disodoDelta
          } else {
            if ((ep1 != 0) && (ep2 != 0)) { //判断是否混动：j及j+1点发动机转速不为零
              gasodo_leng += disodoDelta
            } else { //对临界点单独计算:（j点发动机转速为零，j+1不为零）或（j不为零，j+1为零）这类数据点，将s/2分别给gas及electric
              elecodo_leng += disodoDelta / 2
              gasodo_leng += disodoDelta / 2
            }
          }
        }
      }

      //}
      lastRow = e
    })

    val start_loc = arr.head.lon +","+arr.head.lat
    val end_loc = arr.last.lon +","+arr.last.lat
    val avgFuel = fuelComsumption_Day
    Out(arr.size,arr.head.ts,arr.last.ts, avgFuel, Math.round(length / 1000d),
      Math.round(elec_leng / 1000d), Math.round(gas_leng / 1000d),
      Math.round(sum_odometer), Math.round(elecodo_leng), Math.round(gasodo_leng),start_loc,end_loc)
  }

  def mile(buf: Seq[Seq[Double]]) = {

    val arr = buf.map(e => {
      val ts = e(0).toLong
      //val key_st = e(1).toInt
      val engspd = e(1).toInt
      val odo = e(2).toInt

      val fuel = e(3)
      val lon = e(4)
      val lat = e(5)
      InRow(ts, engspd, odo, fuel, lon, lat)
    })

    val out = finish(arr)

    out
  }

  def to(row:Row) = {
    //val row = st.getStruct(0)
    Out(
      row.getLong(0),row.getLong(1),row.getLong(2),
      row.getDouble(3),row.getDouble(4),row.getLong(5),
      row.getLong(6),row.getLong(7),row.getLong(8),
      row.getLong(9),row.getString(10),row.getString(11))
  }

  def merge(m: Seq[Row]):Array[Out] = {

    val a = m.map(to(_)).sortWith((a,b)=>{a.start_time < b.start_time})

    val arr = new ArrayBuffer[Out]()
    var last = a.head

//    case class Out(count: Long, start_time:Long, end_time:Long,
//                   fuel: Double, gpsLen: Double, elec_gps_len: Long,
//                   gas_gps_len: Long, odoLen: Long, elec_odo_len: Long,
//                   gas_odo_len: Long,start_Location:String, end_Location:String)
    a.tail.foreach(r =>{
      if (r.start_time - last.end_time < 10 * 60 * 1000) {
        last = Out(
          last.count+r.count,
          last.start_time,
           r.end_time,
          (last.fuel+r.fuel)/2,
          last.gpsLen + r.gpsLen,last.elec_gps_len+r.elec_gps_len,last.gas_gps_len+r.gas_gps_len,
          last.odoLen + r.odoLen,last.elec_odo_len+r.elec_odo_len,last.gas_odo_len+r.gas_odo_len,
          last.start_location,
          r.end_location
        )
      } else {
        arr.append(last)
        last = r
      }
    })
    arr.append(last)
    arr.toArray[Out]
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
//        .filter($"vin".equalTo("LMGGN1S59G1002956"))
      .filter($"loc_lon84" > 72.004 && $"loc_lon84" < 137.8347 && $"loc_lat84" > 0.8293 && $"loc_lat84" < 55.8271)
      .filter($"icm_totalodometer" > 0)
      .select($"vin", $"ts", $"date_str",$"bcm_keyst", $"bcs_vehspd", $"ems_engspd",
        $"icm_totalodometer", $"hcu_avgfuelconsump", $"loc_lon84", $"loc_lat84")
      .withColumn("keyst", when(($"bcm_keyst".isNull) || $"bcm_keyst".equalTo(0), 0).otherwise(1))
      .withColumn("tripst", callUDF("tripst", collect_list(array($"ts", $"bcs_vehspd", $"keyst")).over(ws1)))
      .withColumn("tripId", callUDF("tripSegment", collect_list(concat($"vin",lit(","),$"tripst")).over(ws1)))
      .filter($"tripId" > 0)
      .groupBy($"vin", $"tripId")
      .agg(count($"*").as("cnt"),collect_list(array($"ts", $"ems_engspd", $"icm_totalodometer",
        $"hcu_avgfuelconsump", $"loc_lon84", $"loc_lat84")).as("collset"))
      .filter($"cnt">2)
      .select($"vin", callUDF("mileUdf", $"collset").as("res"), lit("AG").as("vintype"))
      .groupBy($"vin").agg(callUDF("merge",collect_list($"res")).as("m"))
      .select($"vin",lit("AG").as("vintype"), explode($"m").as("data"))
      .filter($"data.end_time" - $"data.start_time"  > 5*60*1000 && $"data.odoLen" > 0)
      .withColumn("ts", $"data.start_time")
      .withColumn("id", concat($"vin", lit("-"), $"data.start_time"))

    val es_cfg = cfg + ("es.mapping.timestamp" -> "ts", "es.mapping.id" -> "id")
    save(act, "wh/trip", es_cfg)


//        val writer = new PrintWriter("LMGGN1S59G1002956.csv")
//
//        act.collect().foreach(e => {
//          writer.write(e.mkString(",") + "\n")
//        })
//        writer.close()
  }
}
