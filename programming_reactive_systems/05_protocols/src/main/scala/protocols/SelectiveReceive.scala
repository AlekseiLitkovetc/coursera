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

        def receiving(behavior: Behavior[T], stashBuffer: StashBuffer[T]): Behavior[T] =
            Behaviors.receive { (ctx: ActorContext[T], msg: T) =>
                val started = validateAsInitial(start(behavior, ctx))
                val next = interpretMessage(started, ctx, msg)
                val nextCanonicalized = canonicalize(next, started, ctx)
                if (Behavior.isUnhandled(next)) {
                    stashBuffer.stash(msg)
                    receiving(nextCanonicalized, stashBuffer)
                } else if (stashBuffer.nonEmpty) {
                    stashBuffer.unstashAll(ctx.asScala, receiving(nextCanonicalized, StashBuffer(bufferSize)))
                } else {
                    receiving(nextCanonicalized, stashBuffer)
                }
            }

        receiving(initialBehavior, StashBuffer[T](bufferSize))
    }


}
