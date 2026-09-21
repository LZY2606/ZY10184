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

import kotlin.time.Duration
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.selects.select

/**
 * An event which was consumed from a source within an [awaitWindow] window.
 *
 * [awaitWindow] is a *consuming* API: every event it reads is removed from the source
 * [ReceiveTurbine] and is never put back, whether the window succeeds or fails. The consumed events
 * are returned so a failed window never silently swallows test evidence.
 */
public class WindowEvent
internal constructor(
  /** The stable name of the source this event was consumed from. */
  public val sourceName: String,
  /** The consumed event. */
  public val event: Event<*>,
) {
  override fun equals(other: Any?): Boolean =
    other is WindowEvent && sourceName == other.sourceName && event == other.event

  override fun hashCode(): Int = 31 * sourceName.hashCode() + event.hashCode()

  override fun toString(): String = "$sourceName: $event"
}

/**
 * A single expectation within an [awaitWindow] window. [matches] is only ever invoked with the
 * event kinds it declared interest in: item matchers never see [Event.Error] or [Event.Complete],
 * and error matchers never see items.
 */
internal class Slot(val description: String, val matches: (Event<*>) -> Boolean)

/** A source-bound expectation consumed by [awaitWindow]. */
public class WindowSource<T>
internal constructor(
  /** The stable name of this source, used in all failure reports. */
  public val name: String,
  internal val state: SourceState,
) {
  /**
   * Expect exactly one item from this source, matched in declaration order against this source's
   * event stream. [matcher] is only invoked with item values; terminal events are never passed to
   * it.
   */
  public fun expectItem(description: String? = null, matcher: (T) -> Boolean = { true }) {
    state.slots.add(Slot(itemDescription(description)) { event -> event.matchesItem(matcher) })
  }

  /**
   * Expect exactly [count] items from this source, matched in declaration order. Each item is
   * checked with [matcher], which is only invoked with item values.
   */
  public fun expectItems(
    count: Int,
    description: String? = null,
    matcher: (T) -> Boolean = { true },
  ) {
    require(count > 0) { "count must be positive: $count" }
    repeat(count) { expectItem(description, matcher) }
  }

  /** Expect this source to complete. */
  public fun expectComplete() {
    state.slots.add(Slot("complete") { event -> event is Event.Complete })
  }

  /**
   * Expect this source to terminate with an error. [matcher] is only invoked with the error's
   * [Throwable].
   */
  public fun expectError(description: String? = null, matcher: (Throwable) -> Boolean = { true }) {
    state.slots.add(
      Slot(description?.let { "error matching $it" } ?: "error") { event ->
        event is Event.Error && matcher(event.throwable)
      }
    )
  }

  /** Expect this source to terminate, either by completing or by failing with an error. */
  public fun expectTerminal() {
    state.slots.add(Slot("terminal (complete or error)") { event -> event.isTerminal })
  }

  /**
   * An [expectAnyOf] alternative satisfied by an item from this source. [matcher] is only invoked
   * with item values.
   */
  public fun matchingItem(description: String? = null, matcher: (T) -> Boolean): WindowAlternative =
    WindowAlternative(
      state,
      Slot(itemDescription(description)) { event -> event.matchesItem(matcher) },
    )

  /** An [expectAnyOf] alternative satisfied by this source terminating. */
  public fun matchingTerminal(): WindowAlternative =
    WindowAlternative(state, Slot("terminal (complete or error)") { event -> event.isTerminal })

  private fun itemDescription(description: String?): String =
    description?.let { "item matching $it" } ?: "item"

  @Suppress("UNCHECKED_CAST")
  private fun Event<*>.matchesItem(matcher: (T) -> Boolean): Boolean =
    this is Event.Item<*> && matcher(value as T)
}

/**
 * One alternative of an [expectAnyOf] expectation. Create with [WindowSource.matchingItem] or
 * [WindowSource.matchingTerminal].
 */
public class WindowAlternative
internal constructor(internal val state: SourceState, internal val slot: Slot)

/**
 * Builder for the multi-source event window asserted by [awaitWindow].
 *
 * Expectations declared on the same [WindowSource] are matched in declaration order against that
 * source's event stream ("same-source ordering"). No ordering is imposed between different sources:
 * events are consumed from whichever source produces them first.
 */
public class TurbineWindow internal constructor() {
  internal val states = mutableListOf<SourceState>()
  internal val anyOfs = mutableListOf<AnyOfState>()

  /**
   * Bind [turbine] to this window under a stable [name]. If [name] is null, the turbine's own name
   * (see `testIn(name = ...)`) is used, falling back to `source N`.
   *
   * Every event a bound source produces while the window is open must be accounted for by an
   * expectation; an unaccounted item fails the window. Bind only sources whose in-window events you
   * fully describe.
   */
  public fun <T> source(turbine: ReceiveTurbine<T>, name: String? = null): WindowSource<T> {
    val resolvedName = name ?: (turbine as? ChannelTurbine)?.name ?: "source ${states.size + 1}"
    require(states.none { it.turbine === turbine }) {
      "Turbine '$resolvedName' is already bound to this window"
    }
    require(states.none { it.name == resolvedName }) {
      "Duplicate source name in window: '$resolvedName'"
    }
    val state = SourceState(resolvedName, turbine, turbine.asChannel())
    states += state
    return WindowSource(resolvedName, state)
  }

  /**
   * Expect exactly one of [alternatives] to occur. The first event matching any alternative
   * satisfies this expectation; the matching event is consumed from whichever source produced it.
   */
  public fun expectAnyOf(vararg alternatives: WindowAlternative) {
    require(alternatives.isNotEmpty()) { "expectAnyOf requires at least one alternative" }
    for (alternative in alternatives) {
      require(alternative.state in states) {
        "expectAnyOf alternative uses a source which was not bound to this window"
      }
    }
    anyOfs += AnyOfState(alternatives.toList())
  }
}

internal class SourceState(
  val name: String,
  val turbine: ReceiveTurbine<*>,
  val channel: ReceiveChannel<*>,
  val slots: ArrayDeque<Slot> = ArrayDeque(),
  var terminated: Boolean = false,
  var terminalCause: Throwable? = null,
)

internal class AnyOfState(
  val alternatives: List<WindowAlternative>,
  var satisfied: Boolean = false,
) {
  val description: String
    get() =
      alternatives.joinToString(prefix = "anyOf(", postfix = ")") {
        "${it.state.name}: ${it.slot.description}"
      }
}

/**
 * Assert a bounded multi-source event window over several [ReceiveTurbine]s without imposing an
 * order between sources.
 *
 * Within the window, events are consumed from all bound sources as they arrive, in any
 * interleaving, until every expectation declared in [expect] is satisfied. Expectations on the same
 * source are matched in declaration order; no ordering is imposed across sources. The window is
 * bounded: it ends as soon as all expectations are satisfied, and fails if the timeout ([timeout],
 * or the current [withTurbineTimeout] context, defaulting to 3 seconds) elapses, if a source
 * terminates leaving expectations that can never be satisfied, or if a source produces an event no
 * pending expectation accounts for.
 *
 * **Consumption semantics: [awaitWindow] is consuming, not transactional.** Events read inside the
 * window stay consumed even when the window fails; they are never returned to the source turbines.
 * This holds uniformly for every failure path (timeout, unexpected event, or termination). The
 * consumed events are included in the failure message and, on success, returned as a list of
 * [WindowEvent]s.
 *
 * Failure messages reuse the turbines' names and list the events already consumed, the expectations
 * still unsatisfied, and the expectations which became impossible because their source terminated.
 *
 * Cancelling the calling coroutine (or its parent scope) deterministically ends the window: the
 * pending wait is cancelled and the [kotlinx.coroutines.CancellationException] propagates.
 *
 * Complexity: each event is consumed exactly once and matched in O(1) against its source's head
 * expectation plus the pending `expectAnyOf` alternatives for that source, for a total of
 * O(events + alternatives).
 *
 * ```kotlin
 * turbineScope {
 *   val a = flowA.testIn(backgroundScope, name = "a")
 *   val b = flowB.testIn(backgroundScope, name = "b")
 *   awaitWindow {
 *     source(a).apply {
 *       expectItem("one") { it == 1 }
 *       expectItem("two") { it == 2 } // 'a' emits 1 before 2
 *       expectComplete()
 *     }
 *     source(b).expectTerminal()
 *     expectAnyOf(a.matchingItem { it == 1 }, b.matchingTerminal())
 *   }
 * }
 * ```
 *
 * @param timeout If non-null, overrides the current Turbine timeout for this window. See also:
 *   [withTurbineTimeout].
 * @return the events consumed by the window, in consumption order.
 * @throws AssertionError if the window's expectations are not met.
 */
public suspend fun TurbineContext.awaitWindow(
  timeout: Duration? = null,
  expect: TurbineWindow.() -> Unit,
): List<WindowEvent> {
  if (timeout != null) {
    // Eager check to throw early rather than in a subsequent wait.
    checkTimeout(timeout)
  }
  val window = TurbineWindow()
  window.expect()
  val engine = WindowEngine(window.states, window.anyOfs)
  return engine.run(timeout ?: contextTimeout())
}

private class WindowEngine(
  private val states: List<SourceState>,
  private val anyOfs: List<AnyOfState>,
) {
  private val consumed = mutableListOf<WindowEvent>()

  suspend fun run(timeout: Duration): List<WindowEvent> {
    try {
      withAppropriateTimeout(timeout) { loop() }
    } catch (e: TimeoutCancellationException) {
      throw failure("timed out after $timeout", e)
    } catch (e: TurbineTimeoutCancellationException) {
      throw failure("timed out after $timeout", e)
    }
    return consumed
  }

  private suspend fun loop() {
    while (true) {
      if (isSatisfied()) return
      val impossible = computeImpossible()
      if (impossible.isNotEmpty()) {
        throw failure("expectations can no longer be satisfied", null, impossible)
      }
      // All sources are active here: if every source had terminated and expectations remained,
      // computeImpossible() would have failed the window above.
      val active = states.filter { !it.terminated }
      val (state, event) = selectEvent(active)
      consumed += WindowEvent(state.name, event)
      dispatch(state, event)
      if (event.isTerminal) {
        state.terminated = true
        state.terminalCause = (event as? Event.Error)?.throwable
        // The select clause bypasses the turbine's terminal-event tracking; record it manually.
        (state.turbine as? ChannelTurbine)?.markTerminalEventConsumed()
      }
    }
  }

  private fun isSatisfied(): Boolean =
    states.all { it.slots.isEmpty() } && anyOfs.all { it.satisfied }

  @OptIn(ExperimentalCoroutinesApi::class) // onReceiveCatching is still experimental.
  private suspend fun selectEvent(active: List<SourceState>): Pair<SourceState, Event<*>> = select {
    for (state in active) {
      state.channel.onReceiveCatching { result ->
        val event =
          checkNotNull(result.toEvent()) {
            "Channel result was neither an item nor a terminal event"
          }
        state to event
      }
    }
  }

  private fun dispatch(state: SourceState, event: Event<*>) {
    val head = state.slots.firstOrNull()
    if (head != null && head.matches(event)) {
      state.slots.removeFirst()
      return
    }
    for (anyOf in anyOfs) {
      if (anyOf.satisfied) continue
      if (anyOf.alternatives.any { it.state === state && it.slot.matches(event) }) {
        anyOf.satisfied = true
        return
      }
    }
    if (event.isTerminal) {
      // An unmatched terminal event ends its source. The next loop iteration reports every
      // expectation this termination made impossible.
      return
    }
    val expectation = head?.let { "; expected ${it.description}" } ?: ""
    throw failure(
      "unexpected $event for ${state.name}$expectation",
      (event as? Event.Error)?.throwable,
    )
  }

  private fun computeImpossible(): List<String> {
    val impossible = mutableListOf<String>()
    for (state in states) {
      if (state.terminated) {
        for (slot in state.slots) {
          impossible += "${state.name}: ${slot.description}"
        }
      }
    }
    for (anyOf in anyOfs) {
      if (!anyOf.satisfied && anyOf.alternatives.all { it.state.terminated }) {
        impossible += anyOf.description
      }
    }
    return impossible
  }

  private fun failure(
    header: String,
    cause: Throwable?,
    impossible: List<String> = computeImpossible(),
  ): TurbineAssertionError {
    val impossibleSet = impossible.toSet()
    val unsatisfied = buildList {
      for (state in states) {
        for (slot in state.slots) {
          val description = "${state.name}: ${slot.description}"
          if (description !in impossibleSet) add(description)
        }
      }
      for (anyOf in anyOfs) {
        if (!anyOf.satisfied && anyOf.description !in impossibleSet) add(anyOf.description)
      }
    }
    val failureCause = cause ?: states.firstNotNullOfOrNull { it.terminalCause }
    return TurbineAssertionError(
      buildString {
        append("Window assertion failed: ").append(header)
        append("\nConsumed events:")
        if (consumed.isEmpty()) {
          append("\n - (none)")
        } else {
          for (windowEvent in consumed) {
            append("\n - ").append(windowEvent.sourceName).append(": ").append(windowEvent.event)
          }
        }
        append("\nUnsatisfied expectations:")
        if (unsatisfied.isEmpty()) {
          append("\n - (none)")
        } else {
          for (description in unsatisfied) {
            append("\n - ").append(description)
          }
        }
        if (impossible.isNotEmpty()) {
          append("\nImpossible expectations (source terminated):")
          for (description in impossible) {
            append("\n - ").append(description)
          }
        }
      },
      failureCause,
    )
  }
}
