package kvstore

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorContext, ActorRef, OneForOneStrategy, PoisonPill, Props}
import kvstore.Arbiter._

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  case class RemoveReplicator(replicator: ActorRef)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  // the expected sequence number for second replica
  var seqNum = 0L
  // persistence
  var persistence: ActorRef = context.actorOf(persistenceProps, "persistence")
  // a map from modifiers to senders
  var waitingSenders = Map.empty[ActorRef, ActorRef]

  arbiter ! Join

  def receive: Receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  val leader: Receive = {
    case Replicas(kvNodes) =>
      val replicas = kvNodes - self
      secondaries = secondaries.foldLeft(secondaries) { (acc, secondary) =>
        if (!replicas.contains(secondary._1)) {
          waitingSenders.keys.foreach(_ ! RemoveReplicator(secondary._2))
          secondary._2 ! PoisonPill
          acc - secondary._1
        } else {
          acc
        }
      }
      secondaries = replicas.foldLeft(secondaries) { (acc, replica) =>
        if (!acc.contains(replica)) {
          val replicator = context.actorOf(Replicator.props(replica))
          kv.foldLeft(-1L){ (acc, data) =>
            replicator ! Replicate(data._1, Some(data._2), acc)
            acc - 1
          }
          acc + (replica -> replicator)
        } else {
          acc
        }
      }
      replicators = secondaries.values.toSet
    case Insert(key: String, value: String, id: Long) =>
      kv = kv + (key -> value)
      val modifier = context.actorOf(PrimaryModifier.props(id, persistence, Persist(key, Some(value), id), replicators, Replicate(key, Some(value), id)))
      waitingSenders += (modifier -> sender)
    case Remove(key: String, id: Long) =>
      kv = kv - key
      val modifier = context.actorOf(PrimaryModifier.props(id, persistence, Persist(key, None, id), replicators, Replicate(key, None, id)))
      waitingSenders += (modifier -> sender)
    case Get(key: String, id: Long) =>
      sender ! GetResult(key, kv.get(key), id)
    case operationAck: OperationAck =>
      val responseTracker = sender
      waitingSenders(responseTracker) ! operationAck
      waitingSenders -= responseTracker
    case operationFailed: OperationFailed =>
      val responseTracker = sender
      waitingSenders(responseTracker) ! operationFailed
      waitingSenders -= responseTracker

  }

  val replica: Receive = {
    case Get(key: String, id: Long) =>
      sender ! GetResult(key, kv.get(key), id)
    case Snapshot(key: String, valueOption: Option[String], seq: Long) =>
      replicators = replicators + sender
      if (seq == seqNum) {
        valueOption match {
          case Some(value) => kv = kv + (key -> value)
          case None => kv = kv - key
        }
        seqNum += 1
        context.actorOf(Sender.props(persistence, Persist(key, valueOption, seq)))
      } else if (seq < seqNum) {
        context.sender() ! SnapshotAck(key, seq)
      }
    case Persisted(key: String, seq: Long) =>
      if (replicators.nonEmpty) replicators.head ! SnapshotAck(key, seq)
    case _: OperationFailed =>
  }

}

