import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object HelloSparkPlayground extends App {

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

  //data.show(truncate=false)
  //data.printSchema()

  val attribsToListUdf = udf(attribsToList _)
  val data2: DataFrame = data.withColumn("attributes", attribsToListUdf(col("attributes")))
  //data2.show(truncate=false)
  //data2.printSchema()

  // self join to get all pairs
  val data3 = data2.as("sku1").join(data2.as("sku2"))
  val data4 = data3.select(col("sku1.sku").as("sku1"),
    col("sku2.sku").as("sku2"),
    col("sku1.attributes").as("attributes1"),
    col("sku2.attributes").as("attributes2"))
  //data4.show(truncate = false) // 400M

  // filter out duplicate pairs
  val data5 = data4.filter(col("sku1") < col("sku2"))
  //data5.show(truncate = false) // 199.99M

  // create boolean array of matches for each pair
  val data6 = data5.withColumn("comparison",
    zip_with(col("attributes1"), col("attributes2"), (x, y) => x === y))
  //data6.show(truncate = false)

  // calculate number of matches
  val data7 = data6.withColumn("nrMatches",
    size(filter(col("comparison"), x => x)))
  //data7.show(truncate = false)

  // weigh attributes
  val binToDecArrayUdf = udf(binToDecArray _)
  val data8 = data7.withColumn("binToDecArray", binToDecArrayUdf(col("comparison")))

  // calculate attribute precedence
  val data9 = data8.withColumn("attPrecedence",
    aggregate(col("binToDecArray"), lit(0), (acc, x) => acc + x))
  //data9.show(truncate = false)

  val data10 = data9.select("sku1", "sku2", "nrMatches", "attPrecedence")
  //data10.show()

  val testSku = "sku-123"
  val data11 = data10.filter(col("sku1") === testSku or col("sku2") === "sku-123")
    .orderBy(col("nrMatches").desc, col("attPrecedence").desc)
    .limit(10)

  val getPairSkuUdf = udf(getPairSku _)
  val data12 = data11.withColumn("sku",
    getPairSkuUdf(col("sku1"), col("sku2"), lit(testSku)))
    .drop("sku1", "sku2")
    .withColumn("weight", struct(col("nrMatches"), col("attPrecedence")))
    .drop("nrMatches", "attPrecedence")
    .as[Recommendation]
    .collectAsList()
  data12.forEach(println)

  def getPairSku(sku1: String, sku2: String, skuToRemove: String): String =
    if (sku1 == skuToRemove) sku2 else sku1

  //    println("start filtering out 0 matches")
  //    val data11 = data10.filter(col("nrMatches") =!= 0) // no need for these
  //    data11.count()
  //    println("end filtering out 0 matches")

  //    println("started writing")
  //    data10.write
  //      .mode(SaveMode.Overwrite)
  //      .parquet("src/main/resources/all-pairs-sim.parquet")

  //    data10.write
  //      .format("json")
  //      .mode(SaveMode.Overwrite)
  //      .save("src/main/resources/all-pairs-sim.json")

  //    val data9 = spark.read.load("src/main/resources/all-pairs-sim.parquet")
  //    println("sim scores read")

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
