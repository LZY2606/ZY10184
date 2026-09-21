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
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.test.runTest

class WindowJvmTest {
  @Test
  fun interleavedJvmFlowsSatisfyWindow() = runTest {
    turbineScope {
      val a = flow {
        delay(1)
        emit(1)
        emit(2)
      }
        .testIn(backgroundScope, name = "a")
      val b = flow {
        emit("x")
        delay(1)
        emit("y")
      }
        .testIn(backgroundScope, name = "b")

      val consumed = awaitWindow {
        source(a).apply {
          expectItems(2)
          expectComplete()
        }
        source(b).apply {
          expectItems(2)
          expectComplete()
        }
      }

      assertEquals(6, consumed.size)
    }
  }

  @Test
  fun failureMessageUsesSourceNamesOnJvm() = runTest {
    turbineScope {
      val a = flow { emit(1) }.testIn(backgroundScope, name = "jvm source")
      val actual =
        assertFailsWith<AssertionError> {
          awaitWindow(timeout = 10.milliseconds) {
            source(a).apply {
              expectItem { it == 1 }
              expectItem { it == 2 }
            }
          }
        }
      assertEquals(
        """
        |Window assertion failed: expectations can no longer be satisfied
        |Consumed events:
        | - jvm source: Item(1)
        | - jvm source: Complete
        |Unsatisfied expectations:
        | - (none)
        |Impossible expectations (source terminated):
        | - jvm source: item
        """
          .trimMargin(),
        actual.message,
      )
    }
  }
}
