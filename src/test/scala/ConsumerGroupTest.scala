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

    /*
    Almost identical to ConsumerGroupTheGhettoWayTest, but in this case instead of refreshing the topic
    metadata we actually produce a key using the message number. Use this solution instead of the previous
    one to jump start a consumer group use case.
    */

    it("should consume messages in a balanced fashion, using keys") {

      val MessageCount = 25

      val topic = s"topic-${UUID.randomUUID()}"
      val consumerGroupId = UUID.randomUUID().toString
      KafkaAdminUtils.createTopic(topic, numPartitions = 3)


      val producer = KafkaProducerUtils.create()
      val producerFuture = Future {
        (1 to MessageCount) foreach { number ⇒
          println(s"Producing Message $number")
          producer.send(new KeyedMessage[Array[Byte], Array[Byte]](topic, number.toString.getBytes("UTF-8"), s"Message $number".getBytes("UTF-8")))
          Thread.sleep(50) // N.B.: Unnecessary; it's here to show the parallelism in the tests
        }
      }.andThen { case _ ⇒
        println(s"Finished producing messages")
        producer.close()
      }

      var consumedMessages = 0
      val consumers = (1 to 3) map { n ⇒
        (n, KafkaConsumerUtils.create(consumerTimeoutMs = 5000, autoOffsetReset = "smallest", groupId = consumerGroupId))
      }

      val consumerFutures = consumers map { case (n, consumer) =>
        Future {
          val stream = consumer.createMessageStreamsByFilter(new Whitelist(topic), 1, new StringDecoder, new StringDecoder).head

          println(s"Consumer Number $n begins consuming")
          stream foreach { item ⇒
            println(s"Consumer Number $n consumed ${item.message()}")

            consumedMessages += 1
          }
        }.andThen { case _ ⇒ println(s"Shut down Consumer Number $n"); consumer.shutdown() }
      }

      awaitCondition(s"Didn't consume $MessageCount messages!", 10.seconds) {
        consumedMessages shouldBe MessageCount
      }

      val shutdownFutures = consumers map (t => Future ( t._2.shutdown() ) )
      KafkaAdminUtils.deleteTopic(topic)
      (consumerFutures ++ shutdownFutures :+ producerFuture) foreach (Await.ready(_, 10.second))
    }
  }
}
