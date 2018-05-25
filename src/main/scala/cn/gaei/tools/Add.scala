package cn.gaei.tools

import java.text.SimpleDateFormat
import java.util.Calendar

import cn.gaei.tools.jobs.ag._
import cn.gaei.tools.jobs.gb._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Add {

  def main(args: Array[String]): Unit = {

    val date_str = args(0)
    var  dateFormat = new SimpleDateFormat("yyyyMMdd")
    val ts = dateFormat.parse(date_str).getTime
    val start =Calendar.getInstance();
    val end =Calendar.getInstance();
    start.setTimeInMillis(ts-4*60*60*1000)
    end.setTimeInMillis(ts + 24*60*60*1000)
    val end_year = end.get(Calendar.YEAR)
    val start_year = start.get(Calendar.YEAR)

    val start_month = start.get(Calendar.MONTH)+1
    val end_month = end.get(Calendar.MONTH)+1

    val sc = SparkSession
      .builder()
      .appName("mr-jobs")
      .config("spark.executor.memory", "20G")
      .config("spark.executor.cores", "10")
      .getOrCreate()

    val ag_paths = Set(
      "/data/AG/parquet/d="+dateFormat.format(start.getTime),
      "/data/AG/parquet/d="+date_str
    ).toSeq

    val gb_paths = Set(
      "/data/guobiao/parquet/d="+dateFormat.format(start.getTime),
      "/data/guobiao/parquet/d="+date_str
    ).toSeq

    import sc.implicits._
    val ag= sc.read.parquet(ag_paths:_*)
      .filter($"ts">= start.getTimeInMillis && $"ts" < end.getTimeInMillis)
      .withColumn("d",from_unixtime($"ts"/1000,"yyyyMMdd"))
      .cache()


    val es_cfg = Map("es.nodes"->"slave1:19200,slave2:19200,168.168.5.1:19200,master2:19200","st"->ts.toString,"et"->(ts + 24*60*60*1000).toString)

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

    ag.unpersist()
    val gb= sc.read.parquet(gb_paths:_*)
      .filter($"ts">= start.getTimeInMillis && $"ts" < end.getTimeInMillis)
      .withColumn("d",from_unixtime($"ts"/1000,"yyyyMMdd"))
      .cache()

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
    gb.unpersist()
    sc.close()
  }

}
