package com.streamspecs.validation

import scala.collection.immutable.Queue

final case class RollingWindowState(values: Queue[Double]):
  def add(value: Double, maxSize: Int): RollingWindowState =
    val enqueued = values.enqueue(value)
    if enqueued.size > maxSize then RollingWindowState(enqueued.dequeue._2)
    else RollingWindowState(enqueued)

  def size: Int                     = values.size
  def isFull(maxSize: Int): Boolean = values.size >= maxSize
  def average: Double               = if values.isEmpty then 0.0 else values.sum / values.size

object RollingWindowState:
  val empty: RollingWindowState = RollingWindowState(Queue.empty)

final case class TimedSample(epochMs: Long, value: Double)

final case class TimeRollingWindowState(samples: Queue[TimedSample]):
  def add(epochMs: Long, value: Double, windowMs: Long): TimeRollingWindowState =
    val enqueued = samples.enqueue(TimedSample(epochMs, value))
    TimeRollingWindowState(enqueued.filter(s => epochMs - s.epochMs <= windowMs))

  def size: Int                         = samples.size
  def isReady(minSamples: Int): Boolean = samples.size >= minSamples
  def average: Double =
    if samples.isEmpty then 0.0 else samples.map(_.value).sum / samples.size

object TimeRollingWindowState:
  val empty: TimeRollingWindowState = TimeRollingWindowState(Queue.empty)
