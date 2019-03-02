/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case insert @ Insert(requester, id, elem) => root ! insert
    case contains @ Contains(requester, id, elem) => root ! contains
    case remove @ Remove(requester, id, elem) => root ! remove
    case GC =>
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case message: Operation => pendingQueue = pendingQueue.enqueue(message)
    case CopyFinished =>
      root ! PoisonPill
      root = newRoot
      pendingQueue.foreach(newRoot ! _)
      pendingQueue = Queue.empty[Operation]
      context.unbecome()
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = insert orElse
    contains orElse
    remove orElse
    copyTo

  private def insert: Receive = {
    case Insert(req, id, e) =>
      if (e == elem) {
        removed = false
        req ! OperationFinished(id)
      } else {
        val branchSide = if (e < elem) Left else Right
        subtrees.get(branchSide) match {
          case Some(child) => child ! Insert(req, id, e)
          case None =>
            val child = context.actorOf(BinaryTreeNode.props(e, false))
            subtrees = subtrees + (branchSide -> child)
            req ! OperationFinished(id)
        }
      }
  }

  private def contains: Receive = {
    case Contains(req, id, e) =>
      if (e == elem && !removed) {
        req ! ContainsResult(id, true)
      } else {
        val branchSide = if (e < elem) Left else Right
        subtrees.get(branchSide) match {
          case Some(child) => child ! Contains(req, id, e)
          case None => req ! ContainsResult(id, false)
        }
      }
  }

  private def remove: Receive = {
    case Remove(req, id, e) =>
      if (e == elem && !removed) {
        removed = true
        req ! OperationFinished(id)
      } else {
        val branchSide = if (e < elem) Left else Right
        subtrees.get(branchSide) match {
          case Some(child) => child ! Remove(req, id, e)
          case None => req ! OperationFinished(id)
        }
      }
  }

  private def copyTo: Receive = {
    case CopyTo(newRoot) =>
      val children = subtrees.values.toSet
      if (removed && children.isEmpty) {
        context.parent ! CopyFinished
      } else {
        context.become(copying(children, removed))
        if (!removed) newRoot ! Insert(self, elem, elem)
        children.foreach(_ ! CopyTo(newRoot))
      }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(`elem`) =>
      context.become(copying(expected, true))
      self ! CopyFinished
    case CopyFinished =>
      val actual = expected - sender
      if (insertConfirmed && actual.isEmpty) {
        context.parent ! CopyFinished
      } else {
        context.become(copying(actual, insertConfirmed))
      }
  }

}
