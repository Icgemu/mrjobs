package cn.gaei.tools.jobs.ag

import java.io.PrintWriter

import cn.gaei.tools.api.Tools
import cn.gaei.tools.jobs.Job
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql.EsSparkSQL

class MileCal extends Job {

  case class InRow(ts: Long, engspd: Integer,
                   odo: Integer, fuel: Double, lon: Double, lat: Double)

  case class LonLat(lon: Double, lat: Double)

  case class Out(count: Long, fuel: Double, gpsLen: Double, elec_gps_len: Long,
                 gas_gps_len: Long, odoLen: Long, elec_odo_len: Long, gas_odo_len: Long)


  def finish(a: Seq[InRow]) = {
    val arr = a.filter(e => {
      e.odo < 1000000 && e.odo>0
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
              elecodo_leng += disodoDelta / 2.0
              gasodo_leng += disodoDelta / 2.0
            }
          }
        }
      }

      //}
      lastRow = e
    })

    val avgFuel = fuelComsumption_Day
    Out(arr.size, avgFuel, Math.round(length / 1000d),
      Math.round(elec_leng / 1000d), Math.round(gas_leng / 1000d),
      Math.round(sum_odometer), Math.round(elecodo_leng), Math.round(gasodo_leng))
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

  override def run(sc: SparkSession, df: DataFrame, cfg: Map[String, String]): Unit = {

    import sc.implicits._

    sc.udf.register("mileUdf", (in: Seq[Seq[Double]]) => {
      mile(in)
    })

    val act = df
//        .filter($"vin".equalTo("LMGGN1S56G1003899"))
      .filter($"loc_lon84" > 72.004 && $"loc_lon84" < 137.8347 && $"loc_lat84" > 0.8293 && $"loc_lat84" < 55.8271)
      .filter($"bcm_keyst" > 0 && $"icm_totalodometer" > 0)
      .select($"vin", $"ts", $"date_str", $"ems_engspd", $"icm_totalodometer", $"hcu_avgfuelconsump", $"loc_lon84", $"loc_lat84")
      .groupBy($"vin", $"date_str")
      .agg(count($"*").as("cnt"), collect_list(array($"ts", $"ems_engspd", $"icm_totalodometer", $"hcu_avgfuelconsump", $"loc_lon84", $"loc_lat84")).as("collset"))
      .filter($"cnt"> 2)
      .select($"vin", $"date_str".as("day"), callUDF("mileUdf", $"collset").as("data"), lit("AG").as("vintype"))
      .withColumn("ts", unix_timestamp($"day", "yyyyMMdd")*1000)
      .withColumn("id", concat($"vin", lit("-"), $"day"))

//    act.printSchema()
    val es_cfg = cfg + ("es.mapping.timestamp" -> "ts", "es.mapping.id" -> "id")
    save(act, "wh/mile", es_cfg)

//        val writer = new PrintWriter("LMGGN1S56G1003899.csv")
//
//        act.collect().foreach(e => {
//          writer.write(e.mkString(",") + "\n")
//        })
//        writer.close()
  }
}
