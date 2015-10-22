import java.util.UUID

import kafka.consumer.Whitelist
import kafka.producer.KeyedMessage
import org.scalatest.{ ShouldMatchers, FunSpec }
import utils.{KafkaAdminUtils, KafkaProducerUtils, KafkaConsumerUtils}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import kafka.serializer.StringDecoder

class BasicWorkflowTest extends FunSpec with ShouldMatchers with AwaitCondition {
  describe("Basic Workflow") {
    it("should produce and consume messages") {

      val topic = s"topic-${UUID.randomUUID()}"
      KafkaAdminUtils.createTopic(topic)

      val producer = KafkaProducerUtils.create()
      val consumer = KafkaConsumerUtils.create(consumerTimeoutMs = 5000, autoOffsetReset = "smallest")

      val stream = consumer.createMessageStreamsByFilter(new Whitelist(topic), 1, new StringDecoder, new StringDecoder).head

      val producerFuture = Future {
        (1 to 10) foreach { number ⇒
          println(s"Producing Message $number")
          Thread.sleep(10)
          producer.send(new KeyedMessage[Array[Byte], Array[Byte]](topic, s"Message $number".getBytes("UTF-8")))
        }
      }.andThen { case _ ⇒ println(s"Finished producing messages") }

      var pickedUpTickets = 0

      val consumerFuture = Future {
        println("Consuming")
        stream foreach { item ⇒
          println(s"Consumed ${item.message()}")
          pickedUpTickets += 1
        }
      }.andThen { case _ ⇒ println(s"Shutting down Consumer"); consumer.shutdown() }

      awaitCondition("Didn't consume 10 messages!", 10.seconds) {
        pickedUpTickets shouldBe 10
      }

      KafkaAdminUtils.deleteTopic(topic)
      List(producerFuture, consumerFuture) foreach (Await.ready(_, 10.second))
    }
  }
}
