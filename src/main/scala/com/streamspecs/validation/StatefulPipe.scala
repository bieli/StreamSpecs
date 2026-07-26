package com.streamspecs.validation

import cats.effect.IO
import com.streamspecs.domain.{StatefulAlert, ValidationOutcome}
import fs2.{Pipe, Stream}

/** Compose stateful validators that carry accumulated alerts alongside the outcome. */
object StatefulPipe:

  /** Stage over `(outcome, alertsSoFar)` - preserves per-validator state across the stream. */
  type Stage =
    Pipe[IO, (ValidationOutcome, List[StatefulAlert]), (ValidationOutcome, List[StatefulAlert])]

  type PipeLegacy =
    Stream[IO, ValidationOutcome] => Stream[IO, (ValidationOutcome, List[StatefulAlert])]

  val identity: Stage = identityPipe

  private def identityPipe: Stage = in => in

  def liftOption(maybe: Option[Stage]): Stage =
    maybe.getOrElse(identity)

  def sequenceStages(stages: List[Stage]): Stage =
    stages.foldLeft(identity)(_ andThen _)

  /** Compose classic pipes via their [[stage]] equivalents (continuous state). */
  def sequence(stages: List[Stage]): PipeLegacy =
    in => sequenceStages(stages)(in.map(o => (o, Nil)))

  val identityLegacy: PipeLegacy =
    _.map(o => (o, Nil))
end StatefulPipe
