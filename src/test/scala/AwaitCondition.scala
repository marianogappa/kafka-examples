import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.control.NoStackTrace

case class AwaitConditionFailedException(message: String, e: Throwable)
  extends RuntimeException(s"$message: [${e.getMessage}]") with NoStackTrace {
  this.setStackTrace(e.getStackTrace)
}

trait AwaitCondition {
  def awaitCondition[T](message: String, max: Duration = 10 seconds, interval: Duration = 70 millis)(predicate: ⇒ T) {

    def now: FiniteDuration = System.nanoTime.nanos
    val stop = now + max

    @tailrec
    def poll(nextSleepInterval: Duration) {
      try predicate
      catch {
        case e: Throwable ⇒
          if (now > stop)
            throw new AwaitConditionFailedException(message, e)
          else {
            Thread.sleep(nextSleepInterval.toMillis)
            poll((stop - now) min interval)
          }
      }
    }
    poll(max min interval)
  }
}
