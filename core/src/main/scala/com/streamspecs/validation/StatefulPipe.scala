package com.streamspecs.validation

import cats.effect.IO
import com.streamspecs.core.{EngineOutcome, StatefulAlert}
import fs2.{Pipe, Stream}

object StatefulPipe:

  type Stage[T] =
    Pipe[IO, (EngineOutcome[T], List[StatefulAlert]), (EngineOutcome[T], List[StatefulAlert])]

  type PipeLegacy[T] =
    Stream[IO, EngineOutcome[T]] => Stream[IO, (EngineOutcome[T], List[StatefulAlert])]

  def identity[T]: Stage[T] = in => in

  def sequenceStages[T](stages: List[Stage[T]]): Stage[T] =
    stages.foldLeft(identity[T])(_ andThen _)

  def sequence[T](stages: List[Stage[T]]): PipeLegacy[T] =
    in => sequenceStages(stages)(in.map(o => (o, Nil)))
end StatefulPipe
