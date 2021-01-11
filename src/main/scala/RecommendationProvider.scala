import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

case class RecommendationProvider(hello: String) {

  def getRecommendations(sku: String, simScoreMatrix: DataFrame): DataFrame = {
    simScoreMatrix
      .filter(col("sku1") === sku or col("sku2") === sku)
      .orderBy(col("nrMatches").desc, col("attWeight").desc)
      .limit(11)
  }
}
