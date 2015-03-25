import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import java.util.Locale
import java.util.TimeZone
import java.text.DateFormat

import scala.collection.mutable.ArrayBuffer

object PV extends App {

    object Constants {
        val URL_NOT_NEEDED = List("apple-touch-icon-57x57.png", "static", "healthcheck", "favicon.ico", "ga", "facebook", "twitter", "proxy", "apple-touch-icon-72x72.png", "apple-touch-icon-114x114.png", "apple-touch-icon-144x144.png")

        val DATE_FORMAT : SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("GMT"));

        val MAXIMUM_SESSION_INACTIVITY : Int = 30 * 60
    }

    def incPageViews(path : String) : String = {
        // Remove URI query and fragment
        var hierarchical_path = path.split('?')(0)
        
        // split again on '/'
        var paths = hierarchical_path.split('/')
        if (paths.length < 2) {
            return "" 
        }

        // get root, all paths start with a '/', so item #1 is what is after the first slash 
        var root : String = paths(1).trim()
        if (Constants.URL_NOT_NEEDED.contains(root)) { 
            // we do not need to count this request as a pageview
            root = "" 
        } else {
            // profile is too vague so we need to add the 3rd component. 
            // The 2nd component is for the user_id like in /profile/17548/inbox,
            // /profile/17548/activities, /profile/17548/media
            if (root == "profile") {
                if (paths.length >= 4) {
                    root = paths(3).trim()
                }
            }
        }
        return root
    }
   
    def sessionFinder(session: Tuple2[String, Iterable[String]]) : ArrayBuffer[String] = {
        var lines = ArrayBuffer[String]()

        val id = session._1
        val unsorted_data = session._2 

        val sorted_data = unsorted_data.map(l => (l.split("\t")(1).toLong, l)).toSeq.sortBy(_._1)

        var s = ""

        var index : Int = 0
        var start_index : Int = 0
        var total_secs : Long = 0 
        var total_bytes : Int = 0
        var pages : collection.mutable.Map[String, Int] = collection.mutable.Map[String, Int]()
        while (index < sorted_data.length) {
            val data = sorted_data(index)
            if (index > 0) {
                val delta = data._1 - sorted_data(index - 1)._1
                if (delta > Constants.MAXIMUM_SESSION_INACTIVITY) {
                    s = "%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d".format(id, Constants.DATE_FORMAT.format(new Date(sorted_data(start_index)._1*1000)), total_secs, total_bytes, pages.getOrElse("inbox", 0), pages.getOrElse("mymedia", 0), pages.getOrElse("conversaciones", 0), pages.getOrElse("activities", 0), pages.getOrElse("home", 0), pages.getOrElse("friends", 0), pages.getOrElse("info", 0), pages.getOrElse("media", 0), pages.getOrElse("upload", 0), pages.getOrElse("avatar", 0), pages.getOrElse("publish", 0), pages.getOrElse("help", 0), pages.getOrElse("total_pages", 0))
                    lines += s

                    total_secs = 0
                    total_bytes = 0
                    start_index = index
                    pages.clear()

                } else {
                    total_secs += delta
                }
            }
            val d = data._2.split("\t")
            total_bytes += d(3).toInt
            index += 1
                
            val path = incPageViews(d(6))
            if (path.length() > 0) {    
                pages.put(path, pages.getOrElseUpdate(path, 0) + 1)
                pages.put("total_pages", pages.getOrElseUpdate("total_pages", 0) + 1)
            }
        }

        s = "%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d".format(id, Constants.DATE_FORMAT.format(new Date(sorted_data(start_index)._1*1000)), total_secs, total_bytes, pages.getOrElse("inbox", 0), pages.getOrElse("mymedia", 0), pages.getOrElse("conversaciones", 0), pages.getOrElse("activities", 0), pages.getOrElse("home", 0), pages.getOrElse("friends", 0), pages.getOrElse("info", 0), pages.getOrElse("media", 0), pages.getOrElse("upload", 0), pages.getOrElse("avatar", 0), pages.getOrElse("publish", 0), pages.getOrElse("help", 0), pages.getOrElse("total_pages", 0))
        lines += s
  
        return lines
    }


    override def main(args: Array[String]) {
        val input_filename = "20150223.a"

        val conf = new SparkConf().setAppName("PV")
        val sc = new SparkContext(conf)

        val lines = sc.textFile(input_filename+".pre").cache()

        lines.map(line => (line.split("\t")(0), line)).groupByKey().flatMap(sessionFinder).saveAsTextFile(input_filename+".pv3")

        sc.stop()
    }
}
