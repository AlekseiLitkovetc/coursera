package kvstore

import akka.actor.{Actor, ActorRef, PoisonPill, Props, ReceiveTimeout}
import kvstore.Persistence.{Persist, Persisted}
import kvstore.Replica.OperationFailed
import kvstore.Replicator.{Snapshot, SnapshotAck}

import scala.concurrent.duration._

object Sender {
  def props(recipient: ActorRef, message: AnyRef) = Props(new Sender(recipient, message))
}

class Sender(recipient: ActorRef, message: AnyRef) extends Actor {
  import context.dispatcher

  private val scheduler = context.system.scheduler.schedule(Duration.Zero, 100 milliseconds, recipient, message)

  context.setReceiveTimeout(1 second)

  override def receive: Receive = message match {
    case _: Snapshot => replicaRecipient
    case _: Persist => storageRecipient
    case _ => throw new NotImplementedError(s"The ${message.getClass} is not supported in the Sender actor")
  }

  private def replicaRecipient: Receive = {
    case ack: SnapshotAck => cancellation(context.parent ! ack)
    case ReceiveTimeout => cancellation {
      message match { case m: Snapshot => context.parent ! OperationFailed(m.seq) }
    }
  }

  private def storageRecipient: Receive = {
    case persisted: Persisted => cancellation(context.parent ! persisted)
    case ReceiveTimeout =>
      scheduler.cancel()
      message match { case m: Persist => context.parent ! OperationFailed(m.id) }
      self ! PoisonPill
  }

  private def cancellation(doWork: => Any): Unit = {
    scheduler.cancel()
    doWork
    self ! PoisonPill
  }

}
