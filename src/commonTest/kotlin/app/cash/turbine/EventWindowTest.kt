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
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest

class EventWindowTest {
  private fun <T> turbineWith(vararg events: Event<T>): Turbine<T> {
    val turbine = Turbine<T>()
    for (event in events) {
      when (event) {
        is Event.Item -> turbine.add(event.value)
        Event.Complete -> turbine.close()
        is Event.Error -> turbine.close(event.throwable)
      }
    }
    return turbine
  }

  @Test
  fun crossSourceOrderIsNotRequired() = runTest {
    val a = turbineWith(Event.Item(1))
    val b = turbineWith(Event.Item("two"))
    // Events arrive in reverse declaration order; the window still succeeds.
    expectEventWindow(b named "b", a named "a") {
      expectItem("a")
      expectItem("b")
    }
    a.cancel()
    b.cancel()
  }

  @Test
  fun exactItemCountIsEnforced() = runTest {
    val a = turbineWith(Event.Item(1), Event.Item(2), Event.Item(3))
    expectEventWindow(a named "a") {
      expectItems("a", count = 3) { it is Int }
    }
    a.cancel()
  }

  @Test
  fun exactItemCountFailsWhenTooFewBecauseSourceCompletes() = runTest {
    val a = turbineWith(Event.Item(1), Event.Complete)
    val error =
      assertFailsWith<AssertionError> {
        expectEventWindow(a named "a") {
          expectItems("a", count = 2)
        }
      }
    assertContains(error.message!!, "Event window failed")
    assertContains(error.message!!, "Remaining to be met:")
    assertContains(error.message!!, "No longer satisfiable (source terminated):")
    assertContains(error.message!!, "#2 any item from \"a\"")
    assertContains(error.message!!, "Events consumed during the window (not restored):")
  }

  @Test
  fun anyOfResolvesFromEitherSource() = runTest {
    val a = turbineWith<Int>()
    val b = turbineWith(Event.Item("winner"))
    expectEventWindow(a named "a", b named "b") {
      expectAnyOf {
        item("a") { it == 1 }
        item("b") { it == "winner" }
      }
    }
    // The losing turbine must still be cleaned up.
    a.cancel()
  }

  @Test
  fun heldEventIsBufferedUntilAnyOfResolvesFromAnotherSource() = runTest {
    val a = turbineWith(Event.Item(1), Event.Item(2))
    val b = turbineWith(Event.Item("b"))
    expectEventWindow(a named "a", b named "b") {
      expectAnyOf {
        item("a") { it == 2 }
        item("b")
      }
      // The held Item(1) from "a" must now be replayed against the next expectation.
      expectItem("a", value = 1)
    }
    a.cancel()
    b.cancel()
  }

  @Test
  fun sameSourceOrderIsEnforcedAcrossInterleavedSources() = runTest {
    val a = turbineWith(Event.Item("a1"), Event.Item("a2"))
    val b = turbineWith(Event.Item("b1"), Event.Item("b2"))
    expectEventWindow(a named "a", b named "b") {
      expectItem("a", value = "a1")
      expectItem("b", value = "b1")
      expectItem("a", value = "a2")
      expectItem("b", value = "b2")
    }
    a.cancel()
    b.cancel()
  }

  @Test
  fun sameSourceOutOfOrderFailsImmediately() = runTest {
    val a = turbineWith(Event.Item("a2"), Event.Item("a1"))
    val b = turbineWith(Event.Item("b1"))
    val error =
      assertFailsWith<AssertionError> {
        expectEventWindow(a named "a", b named "b") {
          expectItem("a", value = "a1")
          expectItem("b", value = "b1")
        }
      }
    assertContains(error.message!!, "Found Item(a2) from \"a\" but expected")
    assertContains(error.message!!, "Events consumed during the window (not restored):")
    assertContains(error.message!!, " - \"a\": Item(a2)")
  }

  @Test
  fun completeExpectationConsumesTerminalEvent() = runTest {
    val a = turbineWith(Event.Item(1), Event.Complete)
    val b = turbineWith(Event.Item(2), Event.Complete)
    expectEventWindow(a named "a", b named "b") {
      expectItem("a")
      expectItem("b")
      expectComplete("a")
      expectComplete("b")
    }
  }

  @Test
  fun terminalMarksOtherExpectationsImpossible() = runTest {
    val a = turbineWith(Event.Complete)
    val b = turbineWith(Event.Item(1))
    val error =
      assertFailsWith<AssertionError> {
        expectEventWindow(a named "a", b named "b") {
          expectItem("b")
          expectItem("a")
        }
      }
    assertContains(error.message!!, "No longer satisfiable (source terminated):")
    assertContains(error.message!!, "#2 any item from \"a\"")
    // Non-transactional: the terminal event selected first is reported as
    // consumed-and-not-restored.
    assertContains(error.message!!, "Events consumed during the window (not restored):")
    assertContains(error.message!!, " - \"a\": Complete")
    b.cancel()
  }

  @Test
  fun errorIsNeverPassedToItemMatcher() = runTest {
    val throwable = CustomThrowable("boom")
    val a = turbineWith<Nothing>(Event.Error(throwable))
    var matcherCalled = false
    val error =
      assertFailsWith<AssertionError> {
        expectEventWindow(a named "a") {
          expectItem("a") {
            matcherCalled = true
            true
          }
        }
      }
    assertEquals(false, matcherCalled)
    assertContains(error.message!!, "Found Error(CustomThrowable) from \"a\"")
    assertSame(throwable, error.cause)
  }

  @Test
  fun errorMatcherReceivesThrowable() = runTest {
    val throwable = CustomThrowable("boom")
    val a = turbineWith<Nothing>(Event.Error(throwable))
    expectEventWindow(a named "a") {
      expectError("a") { it.message == "boom" }
    }
  }

  @Test
  fun timeoutFailureListsAllThreeSections() = runTest {
    val a = turbineWith(Event.Item(1))
    val b = turbineWith<Int>()
    val error =
      assertFailsWith<AssertionError> {
        expectEventWindow(a named "a", b named "b", timeout = 10.milliseconds) {
          expectItem("a")
          expectItem("b")
        }
      }
    assertContains(error.message!!, "No matching event window produced in 10ms")
    assertContains(error.message!!, "Consumed:")
    assertContains(error.message!!, "#1 any item from \"a\" <- Item(1)")
    assertContains(error.message!!, "Remaining to be met:")
    assertContains(error.message!!, "#2 any item from \"b\"")
    assertContains(error.message!!, "No longer satisfiable (source terminated):")
    assertContains(error.message!!, " - (none)")
    b.cancel()
  }

  @Test
  fun windowHonorsWithTurbineTimeoutContext() = runTest {
    val a = Turbine<Int>()
    val error =
      assertFailsWith<AssertionError> {
        withTurbineTimeout(10.milliseconds) {
          expectEventWindow(a named "a") { expectItem("a") }
        }
      }
    assertContains(error.message!!, "No matching event window produced in 10ms")
  }

  @Test
  fun cancellingParentScopeEndsWaitDeterministically() = runTest {
    val turbine = turbineWith<Int>()
    val result = runCatching {
      withTurbineTimeout(250.milliseconds) {
        coroutineScope {
          val waiter =
            launch(start = CoroutineStart.UNDISPATCHED) {
              expectEventWindow(turbine named "never") { expectItem("never") }
            }
          waiter.cancel()
          waiter.join()
        }
      }
    }
    assertTrue(result.isSuccess)
  }

  @Test
  fun failureIsNonTransactionalConsumedEventsRemainConsumed() = runTest {
    val a = turbineWith(Event.Item(1), Event.Complete)
    val error =
      assertFailsWith<AssertionError> {
        expectEventWindow(a named "a", timeout = 10.milliseconds) {
          expectItems("a", count = 2)
        }
      }
    assertContains(error.message!!, "Events consumed during the window (not restored):")
    assertContains(error.message!!, " - \"a\": Item(1)")
    // Item(1) stays consumed; the turbine is now sitting on its terminal event.
    assertEquals(Event.Complete, a.awaitEvent())
  }

  @Test
  fun unknownSourceNameFailsWithBoundNames() = runTest {
    val a = turbineWith(Event.Item(1))
    val error =
      assertFailsWith<AssertionError> {
        expectEventWindow(a named "a") { expectItem("c") }
      }
    assertContains(error.message!!, "Unknown window source \"c\"")
    assertContains(error.message!!, "[a]")
    a.cancel()
  }

  @Test
  fun duplicateSourceNamesAreRejected() = runTest {
    val a = turbineWith(Event.Item(1))
    val b = turbineWith(Event.Item(2))
    val error =
      assertFailsWith<IllegalArgumentException> {
        expectEventWindow(a named "same", b named "same") {
          expectItem("same")
        }
      }
    assertContains(error.message!!, "bound twice")
    a.cancel()
    b.cancel()
  }

  @Test
  fun worksWithFlowTurbinesInsideTurbineScope() = runTest {
    turbineScope {
      val a = flowOf(1).testIn(this, name = "flow a")
      val b = flowOf(2).testIn(this, name = "flow b")
      expectEventWindow(b named "b", a named "a") {
        expectItem("a", value = 1)
        expectItem("b", value = 2)
        expectComplete("a")
        expectComplete("b")
      }
    }
  }

  @Test
  fun failureMessageIncludesUnderlyingTurbineNameWhenItDiffers() = runTest {
    val error =
      assertFailsWith<AssertionError> {
        turbineScope {
          val a =
            flow<Int> { throw CustomThrowable("named flow boom") }.testIn(this, name = "flow-a")
          expectEventWindow(a named "window-a") { expectItem("window-a") }
        }
      }
    assertContains(error.message!!, "window-a")
    assertContains(error.message!!, "turbine \"flow-a\"")
  }

  @Test
  fun failureContextIncludesWindowLabels() = runTest {
    val a = turbineWith(Event.Complete)
    val error =
      assertFailsWith<AssertionError> {
        expectEventWindow(a named "events") {
          expectItem("events")
        }
      }
    assertContains(error.message!!, "for window of \"events\"")
    assertContains(error.message!!, "from \"events\"")
  }

  @Test
  fun anyOfTerminalAlternativeMarksRemainingImpossible() = runTest {
    val a = turbineWith(Event.Complete)
    val b = turbineWith<Int>()
    val error =
      assertFailsWith<AssertionError> {
        expectEventWindow(a named "a", b named "b") {
          expectAnyOf {
            item("b")
            complete("a")
          }
          expectItem("a")
        }
      }
    assertContains(error.message!!, "No longer satisfiable (source terminated):")
    assertContains(error.message!!, "#2 any item from \"a\"")
    b.cancel()
  }

  @Test
  fun emptyExpectationBlockIsRejected() = runTest {
    val a = turbineWith(Event.Item(1))
    val error =
      assertFailsWith<IllegalStateException> {
        expectEventWindow(a named "a") {}
      }
    assertContains(error.message!!, "at least one expectation")
    a.cancel()
  }

  @Test
  fun terminalExpectationMustBeLastForSource() = runTest {
    val a = turbineWith(Event.Item(1), Event.Complete)
    val error =
      assertFailsWith<IllegalStateException> {
        expectEventWindow(a named "a") {
          expectComplete("a")
          expectItem("a")
        }
      }
    assertContains(error.message!!, "terminal event is always the last event")
  }
}
