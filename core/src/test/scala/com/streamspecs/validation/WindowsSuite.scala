package com.streamspecs.validation

import munit.FunSuite

class WindowsSuite extends FunSuite:

  test("count window trims to max size and averages") {
    val state =
      RollingWindowState.empty
        .add(100.0, 3)
        .add(50.0, 3)
        .add(30.0, 3)
        .add(10.0, 3) // drops 100

    assertEquals(state.size, 3)
    assert(state.isFull(3))
    assertEquals(state.average, (50.0 + 30.0 + 10.0) / 3.0)
  }

  test("empty count window average is zero and not full") {
    assertEquals(RollingWindowState.empty.average, 0.0)
    assertEquals(RollingWindowState.empty.size, 0)
    assert(!RollingWindowState.empty.isFull(1))
  }

  test("time window expires old samples") {
    val state =
      TimeRollingWindowState.empty
        .add(1000L, 100.0, windowMs = 500)
        .add(1200L, 80.0, windowMs = 500)
        .add(1600L, 10.0, windowMs = 500) // drops 1000

    assertEquals(state.size, 2)
    assertEquals(state.average, (80.0 + 10.0) / 2.0)
    assert(state.isReady(2))
    assert(!state.isReady(3))
  }

  test("empty time window average is zero") {
    assertEquals(TimeRollingWindowState.empty.average, 0.0)
    assert(!TimeRollingWindowState.empty.isReady(1))
  }

  test("time window keeps sample exactly at window boundary") {
    val state =
      TimeRollingWindowState.empty
        .add(1000L, 1.0, 500)
        .add(1500L, 2.0, 500) // age of first = 500 → kept

    assertEquals(state.size, 2)
  }
end WindowsSuite
