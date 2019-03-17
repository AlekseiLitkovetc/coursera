package kvstore

import akka.actor.{Actor, ActorRef, Cancellable, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated}
import kvstore.Arbiter._

import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart

import scala.annotation.tailrec
import akka.pattern.{ask, pipe}

import scala.concurrent.duration._
import akka.util.Timeout

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

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

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
  // a map from request ids to senders
  var waitingSenders = Map.empty[Long, ActorRef]

  arbiter ! Join

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for the leader role. */
  val leader: Receive = {
    case Replicas(kvNodes) =>
      val replicas = kvNodes - self
      secondaries = secondaries.foldLeft(secondaries) { (acc, secondary) =>
        if (!replicas.contains(secondary._1)) {
          secondary._2 ! PoisonPill
          acc - secondary._1
        } else {
          acc
        }
      }
      secondaries = replicas.foldLeft(secondaries) { (acc, replica) =>
        if (!acc.contains(replica)) {
          acc + (replica -> context.actorOf(Replicator.props(replica)))
        } else {
          acc
        }
      }
      replicators = secondaries.values.toSet
    case Insert(key: String, value: String, id: Long) =>
      kv = kv + (key -> value)
      waitingSenders += (id -> sender)
      context.actorOf(PrimaryModifier.props(id, persistence, Persist(key, Some(value), id), replicators, Replicate(key, Some(value), id)))
    case Remove(key: String, id: Long) =>
      kv = kv - key
      waitingSenders += (id -> sender)
      context.actorOf(PrimaryModifier.props(id, persistence, Persist(key, None, id), replicators, Replicate(key, None, id)))
    case Get(key: String, id: Long) =>
      sender ! GetResult(key, kv.get(key), id)
    case operationAck @ OperationAck(id: Long) =>
      waitingSenders(id) ! operationAck
      waitingSenders -= id
    case operationFailed @ OperationFailed(id: Long) =>
      waitingSenders(id) ! operationFailed
      waitingSenders -= id

  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key: String, id: Long) =>
      sender ! GetResult(key, kv.get(key), id)
    case Snapshot(key: String, valueOption: Option[String], seq: Long) =>
      replicators = replicators + sender
      if (seq <= seqNum) {
        if (seq == seqNum) {
          valueOption match {
            case Some(value) => kv = kv + (key -> value)
            case None => kv = kv - key
          }
          seqNum += 1
        }
        context.actorOf(Sender.props(persistence, Persist(key, valueOption, seq)))
      }
    case Persisted(key: String, seq: Long) =>
      replicators.head ! SnapshotAck(key, seq)
  }

}

