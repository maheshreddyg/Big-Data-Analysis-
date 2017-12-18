package SparkRDDanalysis

import org.apache.spark.{SparkConf, SparkContext}

case class Rdd(day:Int,Doy:Int,month:Int,Year:Int,Precp:Double,snow:Double,tave:Double,tmax:Double,tmin:Double)
/**
  * Created by maheshtikky on 12/10/17.
  */
object Rdd {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test Data").setMaster("local[*]").set("spark.driver.host", "localhost")
    val sc = new SparkContext(conf)
        sc.setLogLevel("WARN")
    def toDoubleorneg(s:String):Double ={
      try{
        s.toDouble
      }catch {
        case _ : NumberFormatException => -1
      }
    }
    val lines = sc.textFile("/Users/maheshtikky/Documents/ppk/MN212142_9392.csv").filter(!_.contains("Day"))
    val data = lines.flatMap { line =>
      val p = line.split(",")
      if(p(7)=="."||p(8)=="."||p(9)==".")Seq.empty else
      Seq(Rdd(p(0).toInt, p(1).toInt, p(2).toInt, p(4).toInt, toDoubleorneg(p(5)), toDoubleorneg(p(6)), p(7).toDouble, p(8).toDouble, p(9).toDouble))

    }.cache()
    val maxtemp = data.map(_.tmax).max()                /** Finding Max Temp*/
    println(s"max temp are : $maxtemp ")
    val hotDAYS =data.filter(_.tmax == maxtemp)         /** Finding Hotdays */
    println(s"Hot days are : ${hotDAYS.collect.mkString(",")}")
    val mintemp = data.map(_.tmin).min()                /** Finding Min Temp*/
    println(s"min temp $mintemp ")
    val cooldays = data.filter(_.tmin == mintemp)
    println(s"cool days are: ${cooldays.collect().mkString(", ")}")

    println(data.max()(Ordering.by(_.tmax)))                                    /** Ordering the tmax(field)*/

    println(data.reduce((r1,r2) => if(r1.tmax >= r2.tmax) r1 else r2))          /** Reduce Function */


    val rainycount = data.filter(_.Precp >= 1.0).count()                                      /** Filtering*/
    println(s"There are $rainycount rainydaya. There is ${rainycount * 100.0/data.count()}")

    /** ####################################################################################################*/
    val (rainysum,rainycount2) = data.aggregate(0.0,0)({                                  /** (Aggregate Function) */
      case ((sum, cnt),td) => if(td.Precp < 1.0) (sum,cnt) else (sum + td.tmax, cnt + 1)
    } ,{
      case((s1,c1),(s2,c2))=> (s1+s2,c1+c2)
    })
    println(s"Aggregate:::: ---- Average rainy temps is ${rainysum/rainycount2}")


    val rainytemp =  data.flatMap(td => if(td.Precp < 1.0) Seq.empty else Seq(td.tmax))         /** (flatmap )*/
    println(s"Using Flatmap::: ------- Average rainy temp is ${rainytemp.sum/rainytemp.count}")

    /**#####################################################################################################*/


    val monthgroup = data.groupBy(_.month)
    val monthlyhightemp = monthgroup.map{
      case (m,days) =>
        m -> days.foldLeft(0.0)((sum,td) => sum+td.tmax / days.size)
    }
    //println(monthlyhightemp.collect.mkString(","))

    val monthlylowtemp = monthgroup.map{
      case (m, days) =>
        m -> days.foldLeft(0.0)((sum,td) => (sum + td.tmin)/(days.size))
    }
    //println(monthlylowtemp.collect.mkString(","))

    //monthlyhightemp.collect.sortBy(_._1) foreach println
    //println("###################")
    //monthlylowtemp.collect.sortBy(_._1) foreach println

  val keybyyear = data.map(td => td.Year -> td)
    println(s"this is keyby year${keybyyear.collect.take(5).mkString(",")}")
/**val averagetempbyyear = keybyyear.aggregateByKey(0.0 -> 0)({
  case ((sum, cnt),td) => (sum+td.tmax, cnt+1)
},{
  case ((s1,c1),(s2,c2)) => (s1+s2,c1+c2)
})
    println(averagetempbyyear.collect.mkString(" ,"))
*/
  }
}
