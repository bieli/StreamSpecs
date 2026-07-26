package com.streamspecs.validation

import scala.collection.immutable.Queue

/** Immutable count-based rolling window over numeric observations. */
final case class RollingWindowState(values: Queue[Double]):

  def add(value: Double, maxSize: Int): RollingWindowState =
    val enqueued = values.enqueue(value)
    if enqueued.size > maxSize then RollingWindowState(enqueued.dequeue._2)
    else RollingWindowState(enqueued)

  def size: Int = values.size

  def isFull(maxSize: Int): Boolean = values.size >= maxSize

  def average: Double =
    if values.isEmpty then 0.0
    else values.sum / values.size
end RollingWindowState

object RollingWindowState:
  val empty: RollingWindowState = RollingWindowState(Queue.empty)
