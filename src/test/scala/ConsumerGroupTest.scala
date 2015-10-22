import java.util.UUID

import kafka.admin.AdminUtils
import kafka.consumer.Whitelist
import kafka.producer.KeyedMessage
import kafka.serializer.StringDecoder
import org.scalatest.{ FunSpec, ShouldMatchers }
import utils.{AwaitCondition, KafkaAdminUtils, KafkaConsumerUtils, KafkaProducerUtils}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class ConsumerGroupTest extends FunSpec with ShouldMatchers with AwaitCondition {
  describe("A consumer group") {
    it("should consume messages in a balanced fashion, using keys") {

      val topic = s"topic-${UUID.randomUUID()}"
      val consumerGroupId = UUID.randomUUID().toString

      KafkaAdminUtils.createTopic(topic, numPartitions = 3)

      val producer = KafkaProducerUtils.create()

      val producerFuture = Future {
        (1 to 25) foreach { number ⇒
          println(s"Producing Message $number")
          Thread.sleep(100)
          producer.send(new KeyedMessage[Array[Byte], Array[Byte]](topic, number.toString.getBytes("UTF-8"), s"Message $number".getBytes("UTF-8")))
        }
      }.andThen { case _ ⇒ println(s"Finished producing messages") }

      var consumedMessages = 0

      val consumerFutures = (1 to 3) map { consumerNumber ⇒
        val consumer = KafkaConsumerUtils.create(consumerTimeoutMs = 5000, autoOffsetReset = "smallest", groupId = consumerGroupId)
        Future {
          val stream = consumer.createMessageStreamsByFilter(new Whitelist(topic), 1, new StringDecoder, new StringDecoder).head

          println(s"Consumer Number $consumerNumber begins consuming")
          stream foreach { item ⇒
            println(s"Consumer Number $consumerNumber consumed ${item.message()}")
            Thread.sleep(100)

            consumedMessages += 1
          }
        }.andThen { case _ ⇒ println(s"Shutting down Consumer Number $consumerNumber"); consumer.shutdown() }
      }

      awaitCondition("Didn't consume 25 messages!", 10.seconds) {
        consumedMessages shouldBe 25
      }

      producer.close()
      (consumerFutures :+ producerFuture) foreach (Await.ready(_, 10.second))
      KafkaAdminUtils.deleteTopic(topic)
    }
  }
}
