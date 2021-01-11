import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.{Directives, Route}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

trait SparkRoute extends Directives {

  def recommend(sku: String)(implicit ec: ExecutionContext): Future[Int] // Future[Recommendations]

  val sparkRoute: Route =
    pathPrefix("recommend") {
      (pathEndOrSingleSlash & get) {
        parameter("sku") { sku =>
          extractExecutionContext { implicit ec =>
            onComplete(recommend(sku)) {
              //TODO: complete(recommendations)
              case Success(recommendations) => complete(HttpResponse(StatusCodes.OK, entity = recommendations.toString))
              case Failure(ex) =>
                complete(HttpResponse(StatusCodes.InternalServerError, entity = ex.getMessage))
            }
          }
        }
      }
    }

}
