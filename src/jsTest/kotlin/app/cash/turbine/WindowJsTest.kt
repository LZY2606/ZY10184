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
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.test.runTest

class WindowJsTest {
  @Test
  fun windowSucceedsAcrossSourcesOnJs() = runTest {
    turbineScope {
      val a = flowOf(1, 2).testIn(backgroundScope, name = "a")
      val b = flowOf("x").testIn(backgroundScope, name = "b")

      val consumed = awaitWindow {
        source(a).apply {
          expectItems(2)
          expectComplete()
        }
        source(b).apply {
          expectItem { it == "x" }
          expectComplete()
        }
      }

      assertEquals(5, consumed.size)
    }
  }

  @Test
  fun windowFailureReportsContextOnJs() = runTest {
    turbineScope {
      val a = Turbine<Int>(name = "js source")
      val actual =
        assertFailsWith<AssertionError> {
          awaitWindow(timeout = 10.milliseconds) { source(a).expectItem() }
        }
      assertContains(actual.message!!, "timed out after 10ms")
      assertContains(actual.message!!, "js source: item")
    }
  }
}
