package cn.gaei.tools

import java.text.SimpleDateFormat
import java.util.Calendar

import cn.gaei.tools.jobs.ag._
import cn.gaei.tools.jobs.gb._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.MapType

object Batch {

  def main(args: Array[String]): Unit = {

    //yyyMMdd
    val s_day = args(0)
    val e_day = args(1)
    var  dateFormat = new SimpleDateFormat("yyyyMMdd")
    val s_ts = dateFormat.parse(s_day).getTime
    val e_ts = dateFormat.parse(e_day).getTime + 24*60*60*1000

    val sc = SparkSession
      .builder()
      .appName("mr-jobs")
      .config("spark.executor.memory", "20G")
      .config("spark.executor.cores", "10")
      .getOrCreate()

    import sc.implicits._
    val ag= sc.read.parquet("/data/AG/parquet").filter($"ts">= s_ts && $"ts" < e_ts)

    val es_cfg = Map("es.nodes"->"slave1:19200,slave2:19200,168.168.5.1:19200,master2:19200","st"->s_ts.toString,"et"->e_ts.toString)

    new Jobs()
      .register(new ActiveVins())
      .register(new VinsCal())
      .register(new MileCal())
      .register(new ChargingVol())
      .register(new ErrStat())
      .register(new SocStat())
      .register(new TripCal())
      .register(new StopCal())
      .run(sc, ag, es_cfg)
    val gb= sc.read.parquet("/data/guobiao/parquet").filter($"ts">= s_ts && $"ts" < e_ts)
    new Jobs()
      .register(new GBActiveVins())
      .register(new GBVinsCal())
      .register(new GBMileCal())
      .register(new GBChargingVol())
      .register(new GBErrStat())
      .register(new GBSocStat())
      .register(new GBTripCal())
      .register(new GBStopCal())
      .run(sc, gb, es_cfg)
    sc.close()
  }

}
