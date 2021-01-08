import org.apache.spark.sql.SparkSession

object HelloSpark {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("RecommendEngine")
      .config("spark.master", "local[*]")
      .getOrCreate()

    val data = spark.read
      .format("json")
      .option("inferSchema", "true")
      .load("src/main/resources/test-data-for-spark.json")

    data.show(truncate=false)
    data.printSchema()


  }
}