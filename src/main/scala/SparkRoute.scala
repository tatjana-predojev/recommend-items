import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.{Directives, Route}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import RecommendationJsonProtocol._

trait SparkRoute extends Directives {

  def recommend(sku: String)(implicit ec: ExecutionContext): Future[List[Recommendation]]

  val sparkRoute: Route =
    pathPrefix("recommend") {
      (pathEndOrSingleSlash & get) {
        withRequestTimeout(60.seconds) {
          parameter("sku") { sku =>
            extractExecutionContext { implicit ec =>
              onComplete(recommend(sku)) {
                case Success(recommendations) => complete(recommendations)
                case Failure(ex) =>
                  complete(HttpResponse(StatusCodes.InternalServerError, entity = ex.getMessage))
              }
            }
          }
        }
      }
    }

}
