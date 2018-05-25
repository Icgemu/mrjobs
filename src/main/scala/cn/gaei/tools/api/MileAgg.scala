package cn.gaei.tools.api

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.{Aggregator}
import scala.collection.mutable.ArrayBuffer

case class InRow(ts: Long, key_st: Integer,engspd:Integer, odo:Integer,fuel:Double,lon :Double,lat:Double)
case class Buf(val arr:ArrayBuffer[InRow])
case class Out(count: Long, fuel:Double ,  gpsLen: Double, elec_gps_len:Double,gas_gps_len:Double,odoLen: Double, elec_odo_len:Double,gas_odo_len:Double)
case class LonLat(lon:Double,lat:Double)

object MileAgg extends Aggregator[InRow, Buf, Out] {
  override def zero = Buf(new ArrayBuffer)

  override def reduce(b: Buf, r: InRow) :Buf= {
    //val a = r.getSeq(0)
    //b.arr append(InRow(a.getLong(0), a.getInt(1), a.getInt(2), a.getInt(3), a.getDouble(4), a.getDouble(5), a.getDouble(6)))
    b.arr append(r)
    b
  }

  override def merge(b1: Buf, b2: Buf):Buf = {
    b1.arr.appendAll(b2.arr)
    b1
  }

  override def finish(reduction: Buf):Out = {
    val arr = reduction.arr.sortWith((a1,a2)=>a1.ts > a2.ts).filter(e=>{e.odo < 500000})
    var lastRow = arr.head
    val tail = arr.tail

    var length = 0d
    var time = 0l
    var fuelComsumption_Day = 0d

    //gps
    var con_leng = 0d
    var elec_leng = 0d
    var gas_leng = 0d

    //od0
    var conodo_leng = 0d
    var elecodo_leng =0d
    var gasodo_leng = 0d

    val sum_odometer = arr.last.odo - arr.head.odo

    tail.foreach(e =>{
      val t1 = lastRow.ts
      val t2 = e.ts

      val lonlat1 = LonLat(lastRow.lon, lastRow.lat)
      val lonlat2 = LonLat(e.lon, e.lat)

      val k1 = lastRow.key_st
      val k2 = e.key_st

      val ep1 = lastRow.engspd
      val ep2 = e.engspd

      val od1 = lastRow.odo
      val od2 = e.odo

      val fuelComsumption = (lastRow.fuel + e.fuel)/2
      val timeDelta = t2 - t1

      val disDelta = Tools.distance(lonlat1.lat, lonlat1.lon, lonlat2.lat, lonlat2.lon)
      val speed = disDelta*3600/timeDelta
      if (timeDelta < 5 * 60 * 1000 && speed < 220 && speed > 0 && fuelComsumption < 100) {
        length += disDelta //meter

        time += timeDelta / 1000 //second

        fuelComsumption_Day += fuelComsumption / 100000 * disDelta
        //判断GPS变动是否在熄火之间发生的或者在TSP信号丢失期间发生的，若是则无法判定动力类型
        if ((k1 == 0) && (k2 == 0)){
          con_leng += disDelta
        } else { //判断是否纯电：j及j+1点发动机转速为零
          if ((ep1 == 0) && (ep2 == 0)){
            elec_leng += disDelta
          } else { //判断是否混动：j及j+1点发动机转速不为零
            if ((ep1 != 0) && (ep2 != 0) && ep1 < 500000 && ep2 < 500000) {
              gas_leng += disDelta
            }else { //对临界点单独计算:（j点发动机转速为零，j+1不为零）或（j不为零，j+1为零）这类数据点，将s/2分别给gas及electric
              elec_leng += disDelta / 2
              gas_leng += disDelta / 2
            }
          }
        }

        if (od1 < 500000 && od2 < 500000) {
          val disodoDelta = od2 - od1
          //km
          val speedodo = disodoDelta * 3600 / timeDelta //kilometer/hour
          if (timeDelta < 5 * 60 * 1000 && speedodo < 300 && speedodo > 0) if ((k1 == 0) && (k2 == 0)) conodo_leng += disodoDelta
          else { //判断是否纯电：j及j+1点发动机转速为零
            if ((ep1 == 0) && (ep2 == 0)) elecodo_leng += disodoDelta
            else { //判断是否混动：j及j+1点发动机转速不为零
              if ((ep1 != 0) && (ep2 != 0) && ep1 < 500000 && ep2 < 500000) gasodo_leng += disodoDelta
              else { //对临界点单独计算:（j点发动机转速为零，j+1不为零）或（j不为零，j+1为零）这类数据点，将s/2分别给gas及electric
                elecodo_leng += disodoDelta / 2
                gasodo_leng += disodoDelta / 2
              }
            }
          }
        }
      }
      lastRow = e

    })
    Out(reduction.arr.size,fuelComsumption_Day,length/1000d,gas_leng/1000d,elec_leng/1000d,sum_odometer, elecodo_leng/1000d,gasodo_leng/1000d)
  }

  override def bufferEncoder:Encoder[Buf] = Encoders.product

  override def outputEncoder:Encoder[Out] = Encoders.product
}
