/*
 * Copyright (C) 2026 Square, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package app.cash.turbine

import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest

class WindowTest {
  @Test
  fun eventsConsumedAcrossSourcesWithoutInterSourceOrder() = runTest {
    turbineScope {
      val a = Turbine<Int>(name = "a")
      val b = Turbine<String>(name = "b")
      // Queue events in an order no single global await sequence would accept.
      b.add("first")
      a.add(1)
      b.add("second")
      a.add(2)
      a.close()
      b.close()

      val consumed = awaitWindow {
        source(a).apply {
          expectItem("one") { it == 1 }
          expectItem("two") { it == 2 }
          expectComplete()
        }
        source(b).apply {
          expectItems(2)
          expectComplete()
        }
      }

      // Consumption order follows arrival order, which no global-order assertion could express.
      assertEquals(
        setOf(
          WindowEvent("b", Event.Item("first")),
          WindowEvent("a", Event.Item(1)),
          WindowEvent("b", Event.Item("second")),
          WindowEvent("a", Event.Item(2)),
          WindowEvent("a", Event.Complete),
          WindowEvent("b", Event.Complete),
        ),
        consumed.toSet(),
      )
    }
  }

  @Test
  fun windowWorksWithTestInTurbinesAndReusesTheirNames() = runTest {
    turbineScope {
      val a = flowOf(1, 2).testIn(backgroundScope, name = "ints")
      val b = flowOf("x").testIn(backgroundScope, name = "strings")

      awaitWindow {
        source(a).apply {
          expectItems(2)
          expectComplete()
        }
        source(b).apply {
          expectItem { it == "x" }
          expectComplete()
        }
      }
    }
  }

  @Test
  fun sourceNameDefaultsToTurbineName() = runTest {
    turbineScope {
      val named = Turbine<Int>(name = "named turbine")
      val actual =
        assertFailsWith<AssertionError> {
          awaitWindow(timeout = 10.milliseconds) { source(named).expectItem() }
        }
      assertContains(actual.message!!, "named turbine: item")
    }
  }

  @Test
  fun sameSourceOrderIsEnforced() = runTest {
    turbineScope {
      val a = Turbine<Int>(name = "a")
      a.add(2)
      a.add(1)
      a.close()

      val actual =
        assertFailsWith<AssertionError> {
          awaitWindow {
            source(a).apply {
              expectItem("one") { it == 1 }
              expectItem("two") { it == 2 }
            }
          }
        }
      assertContains(actual.message!!, "unexpected Item(2) for a; expected item matching one")
    }
  }

  @Test
  fun anyOfIsSatisfiedByFirstMatchingAlternative() = runTest {
    turbineScope {
      val a = Turbine<Int>(name = "a")
      val b = Turbine<Int>(name = "b")
      a.close()
      b.add(42)
      b.close()

      awaitWindow {
        val sourceA = source(a)
        val sourceB = source(b)
        sourceA.expectComplete()
        sourceB.expectComplete()
        expectAnyOf(sourceA.matchingItem { it == 42 }, sourceB.matchingItem { it == 42 })
      }
    }
  }

  @Test
  fun terminalExpectations() = runTest {
    turbineScope {
      val completing = Turbine<Int>(name = "completing")
      val failing = Turbine<Int>(name = "failing")
      val either = Turbine<Int>(name = "either")
      val throwable = CustomThrowable("boom")
      completing.close()
      failing.close(throwable)
      either.close()

      awaitWindow {
        source(completing).expectComplete()
        source(failing).expectError { it == throwable }
        source(either).expectTerminal()
      }
    }
  }

  @Test
  fun errorEventsAreNeverPassedToItemMatchers() = runTest {
    turbineScope {
      val a = Turbine<Int>(name = "a")
      val throwable = CustomThrowable("boom")
      a.close(throwable)

      var matcherInvoked = false
      val actual =
        assertFailsWith<AssertionError> {
          awaitWindow {
            source(a).expectItem {
              matcherInvoked = true
              true
            }
          }
        }
      assertFalse(matcherInvoked, "item matcher must not be invoked with an error event")
      assertContains(actual.message!!, "a: item")
      assertContains(actual.message!!, "Impossible expectations")
      assertSame(throwable, actual.cause)
    }
  }

  @Test
  fun failureListsConsumedUnsatisfiedAndImpossible() = runTest {
    turbineScope {
      val a = Turbine<Int>(name = "a")
      val b = Turbine<Int>(name = "b")
      a.add(1)
      a.close()

      val actual =
        assertFailsWith<AssertionError> {
          awaitWindow {
            source(a).apply {
              expectItem { it == 1 }
              expectItem { it == 2 }
            }
            source(b).expectItem()
          }
        }
      val message = actual.message!!
      assertContains(message, "expectations can no longer be satisfied")
      assertContains(message, "Consumed events:\n - a: Item(1)\n - a: Complete")
      assertContains(message, "Unsatisfied expectations:\n - b: item")
      assertContains(message, "Impossible expectations (source terminated):\n - a: item")
    }
  }

  @Test
  fun timeoutFailureListsUnsatisfiedExpectations() = runTest {
    turbineScope {
      val a = Turbine<Int>(name = "a")
      val actual =
        assertFailsWith<AssertionError> {
          awaitWindow(timeout = 10.milliseconds) {
            source(a).expectItem("a number") { it > 0 }
          }
        }
      val message = actual.message!!
      assertContains(message, "timed out after 10ms")
      assertContains(message, "Consumed events:\n - (none)")
      assertContains(message, "Unsatisfied expectations:\n - a: item matching a number")
    }
  }

  @Test
  fun withTurbineTimeoutAppliesToWindow() = runTest {
    turbineScope {
      val a = Turbine<Int>(name = "a")
      val actual =
        assertFailsWith<AssertionError> {
          withTurbineTimeout(10.milliseconds) { awaitWindow { source(a).expectItem() } }
        }
      assertContains(actual.message!!, "timed out after 10ms")
    }
  }

  @Test
  fun consumedEventsAreNotRestoredOnFailure() = runTest {
    turbineScope {
      val a = Turbine<Int>(name = "a")
      a.add(1)
      a.add(2)
      a.close()

      assertFailsWith<AssertionError> {
        awaitWindow {
          source(a).apply {
            expectItem { it == 1 }
            expectItem { it == 3 }
          }
        }
      }

      // The window is consuming, not transactional: Item(1) and Item(2) are gone, and only the
      // terminal event remains.
      a.awaitComplete()
    }
  }

  @Test
  fun cancellingWaitingWindowEndsItDeterministically() = runTest {
    turbineScope {
      val a = Turbine<Int>(name = "a")
      val events = mutableListOf<WindowEvent>()
      val job = launch { events += awaitWindow { source(a).expectItem() } }
      testScheduler.runCurrent()
      job.cancel()
      testScheduler.runCurrent()
      assertTrue(job.isCancelled)
      assertEquals(emptyList(), events)
    }
  }

  @Test
  fun duplicateSourceNameThrows() = runTest {
    turbineScope {
      val a = Turbine<Int>()
      val b = Turbine<Int>()
      val actual =
        assertFailsWith<IllegalArgumentException> {
          awaitWindow {
            source(a, name = "same")
            source(b, name = "same")
          }
        }
      assertEquals("Duplicate source name in window: 'same'", actual.message)
    }
  }

  @Test
  fun sameTurbineBoundTwiceThrows() = runTest {
    turbineScope {
      val a = Turbine<Int>(name = "a")
      val actual =
        assertFailsWith<IllegalArgumentException> {
          awaitWindow {
            source(a)
            source(a)
          }
        }
      assertEquals("Turbine 'a' is already bound to this window", actual.message)
    }
  }

  @Test
  fun expectAnyOfRequiresBoundSources() = runTest {
    turbineScope {
      val a = Turbine<Int>(name = "a")
      val b = Turbine<Int>(name = "b")
      val actual =
        assertFailsWith<IllegalArgumentException> {
          awaitWindow {
            val sourceA = source(a)
            val window = TurbineWindow()
            val unbound = window.source(b)
            expectAnyOf(sourceA.matchingItem { true }, unbound.matchingItem { true })
          }
        }
      assertEquals(
        "expectAnyOf alternative uses a source which was not bound to this window",
        actual.message,
      )
    }
  }

  @Test
  fun cancellationOfParentScopeEndsWindowWaiters() = runTest {
    val a = Turbine<Int>(name = "a")
    val result = kotlin.runCatching {
      turbineScope {
        launch { awaitWindow { source(a).expectItem() } }
        testScheduler.runCurrent()
        throw CancellationException("scope cancelled")
      }
    }
    assertTrue(result.exceptionOrNull() is CancellationException)
  }
}
