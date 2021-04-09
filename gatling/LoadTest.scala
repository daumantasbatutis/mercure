/** Load test for Mercure.
  *
  * 1. Grab Gatling 3 on https://gatling.io
  * 2. Run path/to/gatling/bin/gatling.sh --simulations-folder .
  *
  * Available environment variables (all optional):
  *   - HUB_URL: the URL of the hub to test
  *   - JWT: the JWT to use for authenticating the publisher, fallbacks to JWT if not set and PRIVATE_UPDATES set
  *   - INITIAL_SUBSCRIBERS: the number of concurrent subscribers initially connected
  *   - SUBSCRIBERS_RATE_FROM: minimum rate (per second) of additional subscribers to connect
  *   - SUBSCRIBERS_RATE_TO: maximum rate (per second) of additional subscribers to connect
  *   - PUBLISHERS_RATE_FROM: minimum rate (per second) of publications
  *   - PUBLISHERS_RATE_TO: maximum rate (per second) of publications
  *   - INJECTION_DURATION: duration of the publishers injection
  *   - CONNECTION_DURATION_FROM: min duration of subscribers' connection
  *   - CONNECTION_DURATION_TO: max duration of subscribers' connection
  *   - SUBSCRIBERS_WITH_HISTORY_PERCENT: how many percent of subscribers will ask for history
  */

package mercure

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import scala.concurrent.duration._
import scala.util.Properties

class LoadTest extends Simulation {

  /** The hub URL */
  val HubUrl =
    Properties.envOrElse("HUB_URL", "http://localhost:9080/.well-known/mercure")

  /** JWT to use to publish */
  val Jwt = Properties.envOrElse(
    "JWT",
    "eyJhbGciOiJIUzI1NiJ9.eyJtZXJjdXJlIjp7InB1Ymxpc2giOlsiKiJdLCJzdWJzY3JpYmUiOlsiKiJdfX0.Ws4gtnaPtM-R2-z9DnH-laFu5lDZrMnmyTpfU8uKyQo"
  )

  /** JWT to use to subscribe, fallbacks to JWT if not set and PRIVATE_UPDATES set */
  val SubscriberJwt = Properties.envOrElse("SUBSCRIBER_JWT", "eyJhbGciOiJIUzI1NiJ9.eyJtZXJjdXJlIjp7InB1Ymxpc2giOlsiKiJdLCJzdWJzY3JpYmUiOlsiKiJdfX0.Ws4gtnaPtM-R2-z9DnH-laFu5lDZrMnmyTpfU8uKyQo")

  /** Number of concurrent subscribers initially connected */
  val InitialSubscribers =
    Properties.envOrElse("INITIAL_SUBSCRIBERS", "100").toInt

  /** Additional subscribers rate (per second) */
  val SubscribersRateFrom =
    Properties.envOrElse("SUBSCRIBERS_RATE_FROM", "2").toInt
  val SubscribersRateTo =
    Properties.envOrElse("SUBSCRIBERS_RATE_TO", "20").toInt
  val SubscribersWithHistoryPercent =
    Properties.envOrElse("SUBSCRIBERS_WITH_HISTORY_PERCENT", "15").toInt

  /** Publishers rate (per second) */
  val PublishersRateFrom =
    Properties.envOrElse("PUBLISHERS_RATE_FROM", "2").toInt
  val PublishersRateTo = Properties.envOrElse("PUBLISHERS_RATE_TO", "20").toInt

  /** Duration of injection (in seconds) */
  val InjectionDuration =
    Properties.envOrElse("INJECTION_DURATION", "3600").toInt

  /** How long a subscriber can stay connected at max (in seconds) */
  val ConnectionDurationFrom =
    Properties.envOrElse("CONNECTION_DURATION", "10").toInt
  val ConnectionDurationTo =
    Properties.envOrElse("CONNECTION_DURATION", "60").toInt


  /** Send private updates with random topics instead of public ones always with the same topic */
  var PrivateUpdates =
    Properties.envOrElse("PRIVATE_UPDATES", "false").toBoolean

  val rnd = new scala.util.Random

  def getSubscriberRequestUrl() : String = {
    var topic = "https://example.com/my-private-topic"
    if (PrivateUpdates) {
      topic = topic + "/{id}"
    }

    var requestQuery = "?topic=" + topic

    if (rnd.nextInt(100) < SubscribersWithHistoryPercent) {
      requestQuery = requestQuery + "&Last-Event-ID=earliest"
    }

    return requestQuery
  }

  /** Subscriber test as a function to handle conditional Authorization header */
  def subscriberTest() = {
    var requestBuilder = sse("Subscribe").connect(session => getSubscriberRequestUrl())

    if (SubscriberJwt != null) {
      requestBuilder =
        requestBuilder.header("Authorization", "Bearer " + SubscriberJwt)
    } else if (PrivateUpdates) {
      requestBuilder = requestBuilder.header("Authorization", "Bearer " + Jwt)
    }

    requestBuilder.await(10)(
      sse.checkMessage("Check content").check(regex("""(.*)Hi(.*)"""))
    )
  }

  val httpProtocol = http
    .baseUrl(HubUrl)

  var topic = "https://example.com/my-private-topic"
  if (PrivateUpdates) {
    topic = topic + "/" + rnd.nextInt()
  }

  var data = Map("topic" -> topic, "data" -> "Hi")
  if (PrivateUpdates) {
    data = data + ("private" -> "true")
  }

  val scenarioPublish = scenario("Publish")
    .exec(
      http("Publish")
        .post("")
        .header("Authorization", "Bearer " + Jwt)
        .formParamMap(data)
        .check(status.is(200))
    )

  val scenarioSubscribe = scenario("Subscribe")
    .exec(
      subscriberTest()
    )
    .pause(
      ConnectionDurationFrom, ConnectionDurationTo
    )
    .exec(sse("Close").close)

  setUp(
    scenarioSubscribe
      .inject(
        atOnceUsers(InitialSubscribers),
        rampUsersPerSec(
          SubscribersRateFrom
        ) to SubscribersRateTo during (InjectionDuration seconds) randomized
      )
      .protocols(httpProtocol),
    scenarioPublish
      .inject(
        rampUsersPerSec(
          PublishersRateFrom
        ) to PublishersRateTo during (InjectionDuration + ConnectionDurationTo seconds) randomized
      )
      .protocols(httpProtocol)
  )
}
