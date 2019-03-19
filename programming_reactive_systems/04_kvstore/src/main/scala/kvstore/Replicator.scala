package kvstore

import akka.actor.{Actor, ActorContext, ActorRef, Props}
import kvstore.Replica.OperationFailed

import scala.language.postfixOps

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  
  def receive: Receive = {
    case replicate @ Replicate(key: String, valueOption: Option[String], id: Long) =>
      val seq = nextSeq()
      acks = acks + (seq -> Tuple2(sender, replicate))
      context.actorOf(Sender.props(replica, Snapshot(key, valueOption, seq)))
    case SnapshotAck(key: String, seq: Long) =>
      acks.get(seq) match {
        case Some((requester, request)) =>
          acks = acks - seq
          requester ! Replicated(key, request.id)
        case _ =>
      }
    case OperationFailed(seq: Long) =>
      acks.get(seq) match {
        case Some(_) =>
          acks = acks - seq
        case _ =>
      }
  }

}
