import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.concurrent.{ExecutionContext, Future}

case class SparkRecommender(spark: SparkSession) extends SparkRoute {

  import spark.implicits._

  //val data: Dataset[Article] = readInputData("src/main/resources/test-data-for-spark.json")
  //val similarityScore = calculateSimilarityScore(data)

  override def recommend(sku: String)(implicit ec: ExecutionContext): Future[Int] = {
    Future.successful(5)
  }

  val attribsToListUdf = udf(attribsToList _)
  val binToDecArrayUdf = udf(binToDecArray _)

  def readInputData(path: String): Dataset[Article] = {
    spark.read
      .format("json")
      .option("inferSchema", "true")
      .load(path)
      .as[Article]
  }

  def calculateSimilarityScore(data: Dataset[Article]): DataFrame = {
    // Article is an array of attributes, switch to DataFrame interface
    val data1 = data.withColumn("attributes", attribsToListUdf(col("attributes")))

    // self join to get all pairs
    data1.as("sku1").join(data1.as("sku2")) // size 400M
      .select(col("sku1.sku").as("sku1"),
        col("sku2.sku").as("sku2"),
        col("sku1.attributes").as("attributes1"),
        col("sku2.attributes").as("attributes2"))
      // filter out duplicate pairs
      .filter(col("sku1") < col("sku2")) // size 199.99M
      // create boolean array of matches for each pair
      .withColumn("comparison",
        zip_with(col("attributes1"), col("attributes2"), (x, y) => x === y))
      // calculate number of matches
      .withColumn("nrMatches",
        size(filter(col("comparison"), x => x)))
      // weigh attributes
      .withColumn("binToDecArray", binToDecArrayUdf(col("comparison")))
      // calculate total attribute weight
      .withColumn("attWeight",
        aggregate(col("binToDecArray"), lit(0), (acc, x) => acc + x))
      .select("sku1", "sku2", "nrMatches", "attWeight")
  }

  def getRecommendations(sku: String, similarityScore: DataFrame): DataFrame = {
    similarityScore
      .filter(col("sku1") === sku or col("sku2") === sku)
      .orderBy(col("nrMatches").desc, col("attWeight").desc)
      .limit(11)
  }

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
