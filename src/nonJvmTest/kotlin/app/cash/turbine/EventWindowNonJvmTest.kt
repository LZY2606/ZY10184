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
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.test.runTest

@OptIn(ExperimentalCoroutinesApi::class)
class EventWindowNonJvmTest {
  @Test
  fun windowSucceedsOnJsLikePlatforms() = runTest {
    val a = flowOf(1)
    val b = flowOf("two")
    turbineScope {
      val ta = a.testIn(this, name = "numbers")
      val tb = b.testIn(this, name = "words")
      expectEventWindow(tb named "words", ta named "numbers") {
        expectItem("numbers", value = 1)
        expectItem("words", value = "two")
        expectComplete("numbers")
        expectComplete("words")
      }
    }
  }

  @Test
  fun failureReportReadableOnJsLikePlatforms() = runTest {
    val error =
      assertFailsWith<AssertionError> {
        turbineScope {
          val a = flow<Int> { throw CustomThrowable("js boom") }.testIn(this)
          val b = flowOf(1).testIn(this)
          expectEventWindow(a named "a", b named "b") {
            expectItem("a")
            expectItem("b")
          }
        }
      }
    assertContains(error.message!!, "Event window failed")
    assertContains(error.message!!, "Consumed:")
    assertContains(error.message!!, "Remaining to be met:")
    assertContains(error.message!!, "No longer satisfiable (source terminated):")
    assertContains(error.message!!, "Events consumed during the window (not restored):")
    assertContains(error.message!!, "Error(CustomThrowable)")
  }

  @Test
  fun sharedFlowsInterleaveWithoutSourceOrdering() = runTest {
    val events = MutableSharedFlow<Int>(extraBufferCapacity = 4)
    val signals = MutableSharedFlow<String>(extraBufferCapacity = 4)
    turbineScope {
      val e = events.testIn(this)
      val s = signals.testIn(this)
      // Emit into both sources before declaring the window; buffering guarantees availability.
      events.emit(1)
      events.emit(2)
      signals.emit("go")
      expectEventWindow(s named "signals", e named "events") {
        expectItem("events", value = 1)
        expectItem("signals", value = "go")
        expectItem("events", value = 2)
      }
      e.cancel()
      s.cancel()
    }
  }

  @Test
  fun timeoutEndsWindowOnJsLikePlatforms() = runTest {
    val a = Turbine<Int>()
    val error =
      assertFailsWith<AssertionError> {
        expectEventWindow(a named "a", timeout = 10.milliseconds) {
          expectItem("a")
        }
      }
    assertTrue(error.message!!.contains("No matching event window produced in 10ms"))
  }
}
