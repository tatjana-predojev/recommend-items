import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object HelloSpark {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("RecommendEngine")
      .config("spark.master", "local[*]")
      .getOrCreate()
    import spark.implicits._

    val data: Dataset[Article] = spark.read
      .format("json")
      .option("inferSchema", "true")
      .load("src/main/resources/test-data-for-spark.json")
      .as[Article]

    data.show(truncate=false)
    data.printSchema()

    val attribsToListUdf = udf(attribsToList _)
    val data2: DataFrame = data.withColumn("attributes", attribsToListUdf(col("attributes")))
    data2.show(truncate=false)
    data2.printSchema()
    //    val noSku = data2.filter(col("sku").isNull).count()
    //    val noAttribs = data2.filter(col("attributes").isNull).count()
    //    println(noSku + " " + noAttribs)

    // self join to get all pairs
    val data3 = data2.as("sku1").join(data2.as("sku2"))
    val data4 = data3.select(col("sku1.sku").as("sku1"),
      col("sku2.sku").as("sku2"),
      col("sku1.attributes").as("attributes1"),
      col("sku2.attributes").as("attributes2"))
    data4.show(truncate = false) // 400M

    // filter out duplicate pairs
    val data5 = data4.filter(col("sku1") < col("sku2"))
    data5.show(truncate = false) // 199.99M

    //data5.orderBy(col("sku1").desc, col("sku2").desc).show()
//    data5.select("*")
//      .where(col("sku1") === "sku-20000")
//      .orderBy(col("sku2").desc)
//      .show()
    //data5.orderBy(col("sku1"), col("sku2")).show()

    // create binary array of matches for each pair
    val data6 = data5.withColumn("comparison",
      zip_with(col("attributes1"), col("attributes2"), (x, y) => x === y))
    data6.show(truncate = false)

    // calculate number of matches
    val data7 = data6.withColumn("nrMatches",
      size(filter(col("comparison"), x => x)))
    data7.show(truncate = false)

//    data7.withColumn("binToDec",
//      aggregate(col("comparison"), lit(0), (acc, x) => acc + x))
    val binToDecArrayUdf = udf(binToDecArray _)
    val data8 = data7.withColumn("binToDecArray", binToDecArrayUdf(col("comparison")))

    val data9 = data8.withColumn("binToDec",
      aggregate(col("binToDecArray"), lit(0), (acc, x) => acc + x))
    data9.show(truncate = false)

    val testSku1 = "sku-17374"
    val topN = data9.filter(col("sku1") === testSku1 or col("sku2") === testSku1)
      .orderBy(col("nrMatches").desc, col("binToDec").desc)
      .limit(20)
      .select(col("sku1"), col("sku2"), col("binToDecArray"),
        col("nrMatches"), col("binToDec"))
      .show(truncate = false)
//      .select(col("sku1"), col("sku2"), col("comparison"),
//         col("nrMatches"))
//      .take(20)
//      topN.foreach(println)



  }

  case class Attributes(`att-a`: String, `att-b`: String, `att-c`: String, `att-d`: String, `att-e`: String,
                        `att-f`: String, `att-g`: String, `att-h`: String, `att-i`: String, `att-j`: String)
  case class Article(sku: String, attributes: Attributes)

  def attribsToList(attributes: Attributes): List[String] = {
    List(attributes.`att-a`, attributes.`att-b`, attributes.`att-c`, attributes.`att-d`,
      attributes.`att-e`, attributes.`att-f`, attributes.`att-g`, attributes.`att-h`,
      attributes.`att-i`, attributes.`att-j`)
  }

  def binToDecArray(arr: Seq[Boolean]): List[Int] = {
    val bin = arr.toList.map(b => if (b) 1 else 0)
    def go(current: List[Int], i: Int, result: List[Int]): List[Int] =
      current match {
        case hd :: tail => go(tail, i-1, (hd * math.pow(2, i).toInt) :: result)
        case Nil => result
      }
    go(bin, bin.length-1, Nil).reverse
  }

}