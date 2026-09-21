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

import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.junit.Test

class EventWindowJvmTest {
  @Test
  fun failureMessageKeepsUnderlyingTurbineNameContext() = runTest {
    val turbine =
      ChannelTurbine(
        Channel<Int>(UNLIMITED).also { it.close(CustomThrowable("named boom")) },
        null,
        null,
        "real turbine name",
      )
    val actual =
      assertFailsWith<AssertionError> {
        expectEventWindow(turbine named "window label") {
          expectItem("window label")
        }
      }
    assertTrue(actual.message!!.contains("window label"))
    assertTrue(actual.message!!.contains("turbine \"real turbine name\""))
  }

  @Test
  fun timeoutFailurePointsAtCallSite() = runTest {
    val turbine = Turbine<Int>()
    val actual =
      assertFailsWith<AssertionError> {
        expectEventWindow(turbine named "never", timeout = 10.milliseconds) {
          expectItem("never")
        }
      }
    assertTrue(actual.message!!.contains("No matching event window produced in 10ms"))
    // The expectation-block lambda adds a synthetic invokeSuspend frame, so only the public entry
    // point's presence in the stack trace is asserted.
    assertTrue(
      actual.stackTraceToString().contains("app.cash.turbine.EventWindowKt.expectEventWindow"),
      actual.stackTraceToString(),
    )
  }

  @Test
  fun acceptsInterleavedEventsFromRealCoroutines() = runTest {
    // Two channels populated by real coroutines on the Default dispatcher, with deliberately
    // swapped emission order relative to the declaration order inside the window.
    val first = Channel<Int>(UNLIMITED)
    val second = Channel<String>(UNLIMITED)
    coroutineScope {
      val j1 =
        launch(Dispatchers.Default) {
          first.send(1)
        }
      val j2 =
        launch(Dispatchers.Default) {
          second.send("two")
        }
      j1.join()
      j2.join()
    }
    first.close()
    second.close()

    val firstTurbine: ReceiveTurbine<Int> = ChannelTurbine(first, null, null, "first channel")
    val secondTurbine: ReceiveTurbine<String> = ChannelTurbine(second, null, null, "second channel")

    expectEventWindow(secondTurbine named "second", firstTurbine named "first") {
      expectItem("first", value = 1)
      expectItem("second", value = "two")
      expectComplete("first")
      expectComplete("second")
    }
  }

  @Test
  fun failureCarriesFlowErrorAsCause() = runTest {
    val boom = CustomThrowable("window boom")
    val turbine = Turbine<Nothing>()
    turbine.close(boom)
    val actual =
      assertFailsWith<AssertionError> {
        expectEventWindow(turbine named "errored source") {
          expectItem("errored source")
        }
      }
    assertTrue(actual.cause === boom)
    assertTrue(actual.message!!.contains("Error(CustomThrowable) from \"errored source\""))
  }
}
