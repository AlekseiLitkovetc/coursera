package kvstore

import akka.actor.{Actor, ActorRef, PoisonPill, Props, ReceiveTimeout}
import kvstore.Persistence.{Persist, Persisted}
import kvstore.Replica.{OperationAck, OperationFailed}
import kvstore.Replicator.{Replicate, Replicated}

import scala.concurrent.duration._

object PrimaryModifier {
  def props(id: Long, persistence: ActorRef, persistMessage: Persist,
            replicators: Set[ActorRef], replicateMessage: Replicate) = {
    Props(new PrimaryModifier(id, persistence, persistMessage, replicators, replicateMessage))
  }
}

class PrimaryModifier(id: Long, persistence: ActorRef, persistMessage: Persist,
                      replicators: Set[ActorRef], replicateMessage: Replicate) extends Actor {

  val primaryNotifier = context.actorOf(Sender.props(persistence, persistMessage))
  var recipients = replicators + primaryNotifier
  replicators.foreach { r => r ! replicateMessage}

  context.setReceiveTimeout(1 second)

  override def receive: Receive = {
    case Persisted(key: String, id: Long) =>
      excludeRecipient()
    case Replicated(key: String, id: Long) =>
      excludeRecipient()
    case ReceiveTimeout =>
      context.parent ! OperationFailed(id)
      self ! PoisonPill
  }

  private def excludeRecipient(): Unit = {
    if (recipients.contains(sender)) recipients -= sender
    if (recipients.isEmpty) {
      context.parent ! OperationAck(id)
      self ! PoisonPill
    }
  }
}
