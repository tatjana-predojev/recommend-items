import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json._

case class Recommendation(sku: String, weight: Tuple2[Int, Int]) extends Serializable

object RecommendationJsonProtocol extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val recFormat = jsonFormat2(Recommendation)
}

