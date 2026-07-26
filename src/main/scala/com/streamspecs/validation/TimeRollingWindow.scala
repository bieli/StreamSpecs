package com.streamspecs.validation

import scala.collection.immutable.Queue

/** One observation inside a time-based rolling window. */
final case class TimedSample(epochMs: Long, value: Double)

/** Immutable sliding window keyed by wall-clock / event time. */
final case class TimeRollingWindowState(samples: Queue[TimedSample]):

  def add(epochMs: Long, value: Double, windowMs: Long): TimeRollingWindowState =
    val enqueued = samples.enqueue(TimedSample(epochMs, value))
    TimeRollingWindowState(enqueued.filter(s => epochMs - s.epochMs <= windowMs))

  /** Drop samples older than `nowMs - windowMs` without adding. */
  def expire(nowMs: Long, windowMs: Long): TimeRollingWindowState =
    TimeRollingWindowState(samples.filter(s => nowMs - s.epochMs <= windowMs))

  def size: Int = samples.size

  def isReady(minSamples: Int): Boolean = samples.size >= minSamples

  def average: Double =
    if samples.isEmpty then 0.0
    else samples.map(_.value).sum / samples.size

  def minEpoch: Option[Long] = samples.headOption.map(_.epochMs)
  def maxEpoch: Option[Long] = samples.lastOption.map(_.epochMs)
end TimeRollingWindowState

object TimeRollingWindowState:
  val empty: TimeRollingWindowState = TimeRollingWindowState(Queue.empty)
