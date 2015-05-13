
import org.apache.spark._

object newClass {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("data")
      .setMaster("local[1]")                                       // Setting spark settings for 1 core
    val spark = new SparkContext(conf)                             // Creating a spark instance
    val textFile = spark.textFile("""C:\\urltest.txt""")           // Load in text file with million digits of pi
    val processed = textFile.flatMap(line => line.split(""))       // Create array of stringed characters
    val counts = processed.map(word => (word, 1))                  // Give each character a value of 1 mapped into tuple
      .reduceByKey(_ + _)                                          // Merge together same characters and value++
    counts.filter(x => x._1 != " " && x._1 != "")                  // Filter out spaces and null characters
      .sortByKey()                                                 // Sort the results by digit
      .foreach(counts => println(counts._1 + " : " + counts._2))   // Print the result
    spark.stop()
  }
}
