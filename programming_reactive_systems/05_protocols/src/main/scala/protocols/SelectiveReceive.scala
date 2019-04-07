package protocols

import akka.actor.typed.Behavior._
import akka.actor.typed.{ActorContext, ExtensibleBehavior, _}
import akka.actor.typed.scaladsl._

object SelectiveReceive {
    /**
      * @return A behavior that stashes incoming messages unless they are handled
      *         by the underlying `initialBehavior`
      * @param bufferSize Maximum number of messages to stash before throwing a `StashOverflowException`
      *                   Note that 0 is a valid size and means no buffering at all (ie all messages should
      *                   always be handled by the underlying behavior)
      * @param initialBehavior Behavior to decorate
      * @tparam T Type of messages
      *
      * Hint: Implement an [[ExtensibleBehavior]], use a [[StashBuffer]] and [[Behavior]] helpers such as `start`,
      * `validateAsInitial`, `interpretMessage`,`canonicalize` and `isUnhandled`.
      */
    def apply[T](bufferSize: Int, initialBehavior: Behavior[T]): Behavior[T] = {
        val buffer: StashBuffer[T] = StashBuffer(bufferSize)

        implicit class BufferOpts(buffer: StashBuffer[T]) {
            def unstash(ctx: ActorContext[T], behavior: Behavior[T]): Behavior[T] = {
                buffer.unstash(ctx.asScala, behavior, numberOfMessages = 1, m => m)
            }
        }

        def unstashing(behavior: Behavior[T], size: Int, wasApplied: Boolean = false): Behavior[T] =
            Behaviors.receive { (ctx: ActorContext[T], msg: T) =>
                if (wasApplied) {
                    buffer.stash(msg)
                    if (size <= 1) {
                        buffer.unstash(ctx.asScala, unstashing(behavior, buffer.size))
                    } else {
                        buffer.unstash(ctx.asScala, unstashing(behavior, size - 1, wasApplied))
                    }
                } else {
                    val next: Behavior[T] = interpretMessage(behavior, ctx, msg)
                    val nextc: Behavior[T] = canonicalize(behavior = next, current = behavior, ctx)
                    if (nextc == behavior) {
                        buffer.stash(msg)
                        if (size <= 1) {
                            receiving(behavior)
                        } else {
                            buffer.unstash(ctx.asScala, unstashing(behavior, size - 1, wasApplied))
                        }
                    } else {
                        if (size <= 1) {
                            buffer.unstash(ctx.asScala, unstashing(next, buffer.size))
                        } else {
                            buffer.unstash(ctx.asScala, unstashing(next, size - 1, wasApplied = true))
                        }
                    }
                }
            }

        def receiving(behavior: Behavior[T]): Behavior[T] =
            Behaviors.receive { (ctx: ActorContext[T], msg: T) =>
                val next: Behavior[T] = interpretMessage(behavior, ctx, msg)
                val nextc: Behavior[T] = canonicalize(behavior = next, current = behavior, ctx)
                if (nextc == behavior) {
                    buffer.stash(msg)
                    receiving(behavior)
                } else {
                    buffer.unstash(ctx.asScala, unstashing(next, buffer.size))
                }
            }

        Behaviors.receive { (ctx: ActorContext[T], msg: T) =>
            buffer.stash(msg)
            buffer.unstashAll(ctx.asScala, receiving(initialBehavior))
        }.receiveSignal { case (ctx: ActorContext[T], msg: T) =>
            Behavior.stopped
        }
    }


}
