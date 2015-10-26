import java.util.UUID

import kafka.consumer.Whitelist
import kafka.producer.KeyedMessage
import kafka.serializer.StringDecoder
import org.scalatest.{FunSpec, ShouldMatchers}
import utils.{AwaitCondition, KafkaAdminUtils, KafkaConsumerUtils, KafkaProducerUtils}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class BasicAsyncWorkflowTest extends FunSpec with ShouldMatchers with AwaitCondition {
  describe("Basic Async Workflow") {
    it("should produce and consume messages without blocking the main thread") {

      /*
      Same as BasicWorkflowTest, but the Producer and the Consumer now live inside Futures. In this case, the
      consumer stream will not block execution, so we need to poll-check with `awaitCondition` until all messages
      are consumed. You may now try reordering the Producer and Consumer code, and it should still work.
       */

      val MessageCount = 10
      val topic = s"topic-${UUID.randomUUID()}"
      KafkaAdminUtils.createTopic(topic)


      val producer = KafkaProducerUtils.create()
      val producerFuture = Future {
        (1 to MessageCount) foreach { number ⇒
          println(s"Producing Message $number")
          producer.send(new KeyedMessage[Array[Byte], Array[Byte]](topic, s"Message $number".getBytes("UTF-8")))
        }
      }.andThen { case _ ⇒ println(s"Finished producing messages") }


      var consumedMessages = 0
      val consumer = KafkaConsumerUtils.create(consumerTimeoutMs = 5000, autoOffsetReset = "smallest")
      val stream = consumer.createMessageStreamsByFilter(new Whitelist(topic), 1, new StringDecoder, new StringDecoder).head
      val consumerFuture = Future {
        println("Consuming")
        stream foreach { item ⇒
          println(s"Consumed ${item.message()}")
          consumedMessages += 1

          if (consumedMessages >= MessageCount)
            consumer.shutdown()
        }
      }.andThen { case _ ⇒ println(s"Shutting down Consumer"); consumer.shutdown() }

      awaitCondition(s"Didn't consume $MessageCount messages!", 10.seconds) {
        consumedMessages shouldBe 10
      }

      producer.close()
      KafkaAdminUtils.deleteTopic(topic)
      List(producerFuture, consumerFuture) foreach (Await.ready(_, 10.second))
    }
  }
}
