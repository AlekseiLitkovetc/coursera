package protocols

import akka.actor.typed._
import akka.actor.typed.ActorContext
import akka.actor.typed.scaladsl._

import scala.concurrent.duration._

object Transactor {

    sealed trait PrivateCommand[T] extends Product with Serializable
    final case class Committed[T](session: ActorRef[Session[T]], value: T) extends PrivateCommand[T]
    final case class RolledBack[T](session: ActorRef[Session[T]]) extends PrivateCommand[T]

    sealed trait Command[T] extends PrivateCommand[T]
    final case class Begin[T](replyTo: ActorRef[ActorRef[Session[T]]]) extends Command[T]

    sealed trait Session[T] extends Product with Serializable
    final case class Extract[T, U](f: T => U, replyTo: ActorRef[U]) extends Session[T]
    final case class Modify[T, U](f: T => T, id: Long, reply: U, replyTo: ActorRef[U]) extends Session[T]
    final case class Commit[T, U](reply: U, replyTo: ActorRef[U]) extends Session[T]
    final case class Rollback[T]() extends Session[T]

    /**
      * @return A behavior that accepts public [[Command]] messages. The behavior
      *         should be wrapped in a [[SelectiveReceive]] decorator (with a capacity
      *         of 30 messages) so that beginning new sessions while there is already
      *         a currently running session is deferred to the point where the current
      *         session is terminated.
      * @param value Initial value of the transactor
      * @param sessionTimeout Delay before rolling back the pending modifications and
      *                       terminating the session
      */
    def apply[T](value: T, sessionTimeout: FiniteDuration): Behavior[Command[T]] =
        SelectiveReceive(30, idle(value, sessionTimeout).narrow)

    /**
      * @return A behavior that defines how to react to any [[PrivateCommand]] when the transactor
      *         has no currently running session.
      *         [[Committed]] and [[RolledBack]] messages should be ignored, and a [[Begin]] message
      *         should create a new session.
      *
      * @param value Value of the transactor
      * @param sessionTimeout Delay before rolling back the pending modifications and
      *                       terminating the session
      *
      * Hints:
      *   - When a [[Begin]] message is received, an anonymous child actor handling the session should be spawned,
      *   - In case the child actor is terminated, the session should be rolled back,
      *   - When `sessionTimeout` expires, the session should be rolled back,
      *   - After a session is started, the next behavior should be [[inSession]],
      *   - Messages other than [[Begin]] should not change the behavior.
      */
    private def idle[T](value: T, sessionTimeout: FiniteDuration): Behavior[PrivateCommand[T]] =
        Behaviors.receive[PrivateCommand[T]] {
            case (ctx: ActorContext[T], Begin(replyTo: ActorRef[ActorRef[Session[T]]])) =>
                val session: ActorRef[Session[T]] = ctx.spawnAnonymous(sessionHandler(value, ctx.self, Set.empty))
                ctx.watchWith(session, RolledBack(session))
                replyTo ! session
                ctx.setReceiveTimeout(sessionTimeout, RolledBack(session))
                inSession(value, sessionTimeout, session)
            case _ =>
                Behavior.ignore
        }
        
    /**
      * @return A behavior that defines how to react to [[PrivateCommand]] messages when the transactor has
      *         a running session.
      *         [[Committed]] and [[RolledBack]] messages should commit and rollback the session, respectively.
      *         [[Begin]] messages should be unhandled (they will be handled by the [[SelectiveReceive]] decorator).
      *
      * @param rollbackValue Value to rollback to
      * @param sessionTimeout Timeout to use for the next session
      * @param sessionRef Reference to the child [[Session]] actor
      */
    private def inSession[T](rollbackValue: T, sessionTimeout: FiniteDuration, sessionRef: ActorRef[Session[T]]): Behavior[PrivateCommand[T]] =
        Behaviors.receive[PrivateCommand[T]] { (ctx, msg: PrivateCommand[T]) =>
            msg match {
                case Committed(session: ActorRef[Session[T]], value: T) =>
                    idle(value, sessionTimeout)
                case RolledBack(session: ActorRef[Session[T]]) =>
                    ctx.stop(session)
                    idle(rollbackValue, sessionTimeout)
                case _ =>
                    Behavior.unhandled
            }
        }
        
    /**
      * Messages description:
      *
      *     Extract:  apply the given projector function to the current Transactor value (possibly modified by the current
      *               session) and return the result to the given ActorRef; if the projector function throws an exception
      *               the session is terminated and all its modifications are rolled back
      *     Modify:   calculate a new value for the Transactor by applying the given function to the current value
      *               (possibly modified by the current session already) and return the given reply value to the given
      *               replyTo ActorRef; if the function throws an exception the session is terminated and all its
      *               modifications are rolled back; the id argument serves as a deduplication identifier: sending the a
      *               Modify command with an id previously used within the current session will not apply the modification
      *               function but send the reply immediately
      *     Commit:   terminate the current session, committing the performed modifications and thus making the modified
      *               value available to the next session; the given reply is sent to the given ActorRef as confirmation
      *     Rollback: terminate the current session rolling back all modifications, i.e. the next session will see
      *               the same value that this session saw when it started
      *
      * @return A behavior handling [[Session]] messages. See in the instructions
      *         the precise semantics that each message should have.
      * @param currentValue The session’s current value
      * @param commit Parent actor reference, to send the [[Committed]] message to
      * @param done Set of already applied [[Modify]] messages
      */
    private def sessionHandler[T](currentValue: T, commit: ActorRef[Committed[T]], done: Set[Long]): Behavior[Session[T]] =
        Behaviors.receive[Session[T]] { (ctx, msg) =>
            msg match {
                case Extract(f, replyTo: ActorRef[Any]) =>
                    replyTo ! f(currentValue)
                    Behavior.same
                case Modify(f, id, reply, replyTo: ActorRef[Any]) if done.isEmpty || id > done.max =>
                    replyTo ! reply
                    sessionHandler(f(currentValue), commit, done + id)
                case Modify(f, id, reply, replyTo: ActorRef[Any]) =>
                    replyTo ! reply
                    Behavior.same
                case Commit(reply, replyTo: ActorRef[Any]) =>
                    commit ! Committed(ctx.self, currentValue)
                    replyTo ! reply
                    Behavior.stopped
                case Rollback() =>
                    Behavior.stopped
            }
        }
        
}
