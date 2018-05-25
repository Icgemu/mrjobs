package cn.gaei.tools.jobs.ag

import java.io.PrintWriter

import cn.gaei.tools.jobs.Job
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.elasticsearch.spark.sql.EsSparkSQL

import scala.collection.mutable.ArrayBuffer

class SocStat extends Job {

  case class Segment(vin:String, st:Int)

  case class InRow(ts: Long, volt: Double, curr: Double, soc: Double, lon: Double, lat: Double)

  case class Out(st: Int, start_time: Long, end_time: Long,
                 start_soc: Double, end_soc: Double, change_time_in_second: Long,
                 charge_vol: Double, lon:Double,lat:Double)

  val map = collection.mutable.Map[String,Int]()
  def chargeSt(e: Seq[Seq[Double]]): Int = {
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

      if (tolerance && (s1 != 12) && (s2 == 12)) {
        f = 1
      } else if (tolerance && (s1 == 12) && (s2 == 12)) {
        f = 2
      } else if (tolerance && (s2 != 12) && (s1 == 12)) {
        f = 3
      } else if (!tolerance) {
        if(c2>0 && s2 == 12){f = 1}else{f = 3}
      }
    }
    //first point
    if (e.size == 1) {
      val s1 = e(0)(2).toInt
      val c1 = if (Option(e(0)(1)).isEmpty) 0 else e(0)(1).toLong
      if (s1 == 12 && c1 > 0) {
        f = 2
      }
    }
    f
  }

  def chargeSegment(e: Seq[Segment]): Int = {

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

  def chargeCal(a: Seq[InRow]) = {
    val ok = a.map(e=>e.soc).filter(_ > 1 ).size > 2
    if (a.size < 2 || !ok ) {
      Out(0, 0l, 0l, 0d, 0d, 0l, 0d, 0d, 0d)
    } else {
      val arr = a.sortWith((a1, a2) => a1.ts < a2.ts)
      val chargetime = (arr.last.ts - arr.head.ts) / 1000
      var lastRow = arr.head
      val tail = arr.tail
      var W_sum = 0d

      val soc1 = arr.map(e=>e.soc).filter(_ > 1 ).head
      val soc2 = arr.map(e=>e.soc).filter(_ > 1).last
      val lon= arr.filter(_.lon>0).map(_.lon).sum/arr.size
      val lat= arr.filter(_.lat>0).map(_.lat).sum/arr.size
      tail.foreach(e => {
        val t1 = lastRow.ts
        val t2 = e.ts

        val v1 = lastRow.volt
        val v2 = e.volt

        val c1 = lastRow.curr
        val c2 = e.curr

        val timeDelta = t2 - t1

        //计算charging
        val W1 = (v1 * c1 + v2 * c2) * timeDelta / 1000 / 2 / (3600 * 1000)
        //因时间间隔短，W1基本为0.0几，若大于1（暂定阈值）即为异常
        //必须加入timeDelta<1min判定  避免出现连续两条数据之间时间间隔过大，导致W1过大
        if (W1 != 0 && timeDelta > 0 && timeDelta < 1 * 60 * 1000 && W1 < 1) { //电功累加
          W_sum += W1
        }

        lastRow = e
      })
      Out(1, arr.head.ts, arr.last.ts, soc1, soc2, chargetime, W_sum, lon, lat)
    }
  }
  def to(row:Row) = {
    //val row = st.getStruct(0)
    Out(row.getInt(0),row.getLong(1),row.getLong(2),
      row.getDouble(3),row.getDouble(4),row.getLong(5),
      row.getDouble(6),row.getDouble(7),row.getDouble(8))
  }

  def merge(m: Seq[Row]):Array[Out] = {

    val a = m.map(to(_)).sortWith((a,b)=>{a.start_time < b.start_time})

    val arr = new ArrayBuffer[Out]()
    var last = a.head

    a.tail.foreach(r =>{
      if(last.st == 0 || r.st == 0) {
        arr.append(last)
        last = r
      } else{
        if (r.start_time - last.end_time < 10 * 60 * 1000) {
          last = Out(1, last.start_time, r.end_time, last.start_soc, r.end_soc,
            (r.end_time - last.start_time) / 1000, r.charge_vol + last.charge_vol,
            (r.lon+last.lon)/2, (r.lat+last.lat)/2
          )
        } else {
          arr.append(last)
          last = r
        }
      }

    })
    arr.append(last)
    arr.toArray[Out]
  }


  override def run(sc: SparkSession, df: DataFrame, cfg: Map[String, String]): Unit = {
    import sc.implicits._

    val br = sc.sparkContext.broadcast[collection.mutable.Map[String,Int]](collection.mutable.Map[String,Int]())

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
//    val ws2 = Window.partitionBy($"vin").orderBy($"ts").rowsBetween(Window.unboundedPreceding, 0)
    val orig = df
//      .filter($"vin".equalTo("LMGGN1S54H1005555"))
      .select($"vin", $"ts", $"ccs_chargecur", $"ccs_chargevolt", $"bms_battst", $"bms_battsoc",$"loc_lon84",$"loc_lat84")
      .withColumn("chargeSt", callUDF("chargeSt", collect_list(array($"ts", $"ccs_chargevolt", $"bms_battst")).over(ws1)))
      .withColumn("chargeId", callUDF("chargeSegment", collect_list(concat($"vin",lit(","),$"chargeSt")).over(ws1)))
//      .withColumn("chargeId", sum($"chargeSegment").over(ws2))
        .filter($"chargeId" > 0)
//    val writer = new PrintWriter("LMGGN1S54H1005555.csv")
//
//    orig.collect().foreach(e => {
//      writer.write(e.mkString(",") + "\n")
//    })
//    writer.close()
    val act = orig
      .groupBy($"vin", $"chargeId").agg(callUDF("chargeCal",
          collect_list(array($"ts", $"ccs_chargecur", $"ccs_chargevolt", $"bms_battsoc",$"loc_lon84",$"loc_lat84"))).as("res"))
      .filter($"res.st">0)
      .groupBy($"vin").agg(callUDF("merge",collect_list($"res")).as("m"))
      .select($"vin",explode($"m").as("data"))
      .filter($"data.st".equalTo(1) && $"data.change_time_in_second" > 5*60 )
      .withColumn("ts", $"data.start_time")
      .withColumn("id", concat($"vin", lit("-"), $"ts"))
      .withColumn("vintype",lit("AG"))

//    act.printSchema()
    val es_cfg = cfg + ("es.mapping.timestamp" -> "ts", "es.mapping.id" -> "id")
    save(act, "wh/soc", es_cfg)
  }
}
