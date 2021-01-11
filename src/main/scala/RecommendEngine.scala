import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.{HttpApp, Route}
import akka.stream.ActorMaterializer
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext
import scala.util.Try

class RecommendEngine extends HttpApp {

  implicit lazy val actorSystem: ActorSystem = systemReference.get()
  implicit lazy val materializer: ActorMaterializer = ActorMaterializer()

  val spark = SparkSession.builder
    .appName("RecommendEngine")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val sparkRecommender = SparkRecommender(spark)

  //initSparkRecommender
//  val data: Dataset[Article] = sparkRecommender.readInputData("src/main/resources/test-data-for-spark.json")
//  val similarityScore: DataFrame = sparkRecommender.calculateSimilarityScore(data)
//  val recs: DataFrame = sparkRecommender.getRecommendations("sku-654", similarityScore)
//  recs.show()

  override def routes: Route = sparkRecommender.sparkRoute

  override protected def postHttpBinding(binding: Http.ServerBinding): Unit = {
    super.postHttpBinding(binding)
    implicit val executionContext = ExecutionContext.Implicits.global
  }

  override protected def postServerShutdown(attempt: Try[Done], system: ActorSystem): Unit = {
    Http().shutdownAllConnectionPools()
    super.postServerShutdown(attempt, system)
  }

}

object RecommendEngine extends App {
  try {
    val recommendEngine = new RecommendEngine().startServer("localhost", 8080)
  } catch {
    case ex: Throwable =>
      println("error when trying to start recommend engine server: " + ex.getMessage)
      sys.exit(1)
  }
}