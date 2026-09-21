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
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.selects.select

/** Marks receivers scoped to the [expectEventWindow] expectation DSL so scopes do not leak. */
@DslMarker
@Retention(AnnotationRetention.BINARY)
@Target(AnnotationTarget.CLASS)
public annotation class TurbineWindowDsl

/**
 * Binds a [ReceiveTurbine] to a stable [name] for use inside an [expectEventWindow] block.
 *
 * The [name] identifies the source in failure reports. It does not need to match the turbine's own
 * name (as supplied to `testIn`, `test`, or the `Turbine` factory); both names are shown in errors.
 */
public infix fun <T> ReceiveTurbine<T>.named(name: String): NamedReceiveTurbine<T> {
  require(name.isNotBlank()) { "A named turbine must have a non-blank name." }
  return NamedReceiveTurbine(this, name)
}

/**
 * A [ReceiveTurbine] bound to a stable [name].
 *
 * Create instances using the [named] infix function, for example `turbine named "events"`.
 *
 * @property turbine The source turbine.
 * @property name The stable label used to refer to this source inside an expectation block.
 */
public class NamedReceiveTurbine<T>
internal constructor(
  public val turbine: ReceiveTurbine<T>,
  public val name: String,
)

/**
 * Assert that a window of events is consumed from the given [sources], without imposing any
 * ordering between the sources.
 *
 * Events are consumed by [expectEventWindow] from the moment the call starts. Expectations are
 * declared in the [block] and matched against events as they arrive:
 *
 * * Ordering is only enforced between expectations on the **same** source, in declaration order.
 *   Events from different sources may arrive and be consumed in any order.
 * * Expectations that have already consumed an event are reported as consumed.
 * * Expectations that are still waiting are reported as remaining to be met.
 * * Expectations that can no longer be met because their source terminated are reported separately
 *   as no longer satisfiable.
 *
 * ### Consumption is non-transactional
 *
 * Event consumption is **not** rolled back when the window fails. Events are consumed as soon as a
 * matching expectation takes them, and those events stay consumed whether the window ultimately
 * succeeds or fails. This matches the behavior of the existing `await*` methods (for example
 * [ReceiveTurbine.awaitItem] also never puts an item back on failure). A failing assertion lists
 * every event consumed during the window so that the state of each turbine is unambiguous.
 *
 * ### Timeouts and cancellation
 *
 * If [timeout] is non-null it bounds the whole window; otherwise the current [withTurbineTimeout]
 * context value is used (3 seconds by default). Timeouts honor the existing Turbine timeout
 * semantics: when a `kotlinx.coroutines.test.TestCoroutineScheduler` is present the timeout is
 * measured on a wall clock so it is unaffected by virtual time, and no real sleeping happens on the
 * test dispatcher. If the calling coroutine is cancelled while the window is waiting, the wait ends
 * deterministically with a [CancellationException].
 *
 * [Event.Error] events are never passed to an item matcher. Use [EventWindowBuilder.expectError]
 * (or an error alternative inside [EventWindowBuilder.expectAnyOf]) to assert on them.
 *
 * This function runs on every supported Kotlin Multiplatform target and does not move any logic to
 * an external service.
 *
 * ## Complexity
 *
 * With `E` events consumed and `R` remaining alternatives inside `expectAnyOf` groups, matching
 * takes at most `O(E * R)` time and `O(E + R)` memory. In the common case (no `expectAnyOf` groups)
 * matching is `O(E)`.
 *
 * @param sources The bound turbines participating in the window. Each source must be bound with a
 *   unique name, and every name referenced from the [block] must be present here.
 * @param timeout Bounds how long the window may wait for the remaining expectations to be met. When
 *   null the timeout from the enclosing [withTurbineTimeout] context is used.
 * @param block The expectation block describing the events to consume.
 * @throws AssertionError if an unexpected event arrives, a source terminates before its
 *   expectations are met, the window times out, or [block] is invalid.
 */
public suspend fun expectEventWindow(
  vararg sources: NamedReceiveTurbine<*>,
  timeout: Duration? = null,
  block: EventWindowBuilder.() -> Unit,
) {
  val boundSources = sources.toList()
  require(boundSources.isNotEmpty()) { "expectEventWindow requires at least one named source." }
  val uniqueNames = mutableSetOf<String>()
  for (source in boundSources) {
    require(uniqueNames.add(source.name)) {
      "expectEventWindow sources must have unique names, but \"${source.name}\" was bound twice."
    }
  }
  val byName = boundSources.associateBy { it.name }

  val builder = EventWindowBuilder(byName).apply(block)
  check(builder.nodes.isNotEmpty()) {
    "expectEventWindow block must declare at least one expectation."
  }

  val effectiveTimeout = timeout ?: contextTimeout()
  val matcher = EventWindowMatcher(byName, builder.nodes)
  try {
    withAppropriateTimeout(effectiveTimeout) { matcher.run() }
  } catch (e: TimeoutCancellationException) {
    matcher.failOnTimeout(effectiveTimeout)
  } catch (e: TurbineTimeoutCancellationException) {
    matcher.failOnTimeout(effectiveTimeout)
  }
}

/**
 * Declares the expectations consumed by an [expectEventWindow] block.
 *
 * Every `expect*` call adds one expectation. Expectations are consumed in declaration order
 * **within a single source**; expectations on different sources are independent and may be met in
 * any order.
 */
@TurbineWindowDsl
public class EventWindowBuilder
@PublishedApi
internal constructor(sources: Map<String, NamedReceiveTurbine<*>>) {
  internal val nodes = mutableListOf<WindowNode>()
  private val sourceByName = sources

  private fun requireSource(sourceName: String): NamedReceiveTurbine<*> {
    return sourceByName[sourceName]
      ?: throw AssertionError(
        "Unknown window source \"$sourceName\". Bound sources: ${sourceByName.keys.sorted()}."
      )
  }

  private fun addNode(node: WindowCondition) {
    val sourceName = node.sourceName
    val priorTerminal = nodes.any { existing ->
      existing is WindowCondition &&
        existing.sourceName == sourceName &&
        existing.kind != EventWindowKind.Item
    }
    check(!priorTerminal) {
      "No expectations for \"$sourceName\" may be declared after its complete/error expectation; " +
        "a terminal event is always the last event of a source."
    }
    nodes += node
  }

  /**
   * Expect exactly one item from [sourceName] to be consumed.
   *
   * @param matcher Optional predicate applied to the item value. When null, any item matches.
   * @param description Optional human-readable description used in failure messages instead of the
   *   generated description.
   */
  public fun expectItem(
    sourceName: String,
    description: String? = null,
    matcher: ((Any?) -> Boolean)? = null,
  ) {
    requireSource(sourceName)
    addNode(
      WindowCondition(
        index = nodes.size + 1,
        sourceName = sourceName,
        kind = EventWindowKind.Item,
        itemMatcher = matcher,
        throwableMatcher = null,
        description = description,
      )
    )
  }

  /**
   * Expect exactly one item from [sourceName] whose value is [value] (using `==`).
   *
   * @param description Optional human-readable description used in failure messages.
   */
  public fun expectItem(
    sourceName: String,
    value: Any?,
    description: String? = null,
  ) {
    expectItem(sourceName, description) { it == value }
  }

  /**
   * Expect exactly [count] items from [sourceName] to be consumed, in source order.
   *
   * @param count Must be at least 1.
   * @param matcher Optional predicate applied to every item value. When null, any items match.
   * @param description Optional human-readable description used in failure messages.
   */
  public fun expectItems(
    sourceName: String,
    count: Int,
    description: String? = null,
    matcher: ((Any?) -> Boolean)? = null,
  ) {
    requireSource(sourceName)
    require(count >= 1) { "expectItems count must be at least 1 but was $count." }
    repeat(count) {
      addNode(
        WindowCondition(
          index = nodes.size + 1,
          sourceName = sourceName,
          kind = EventWindowKind.Item,
          itemMatcher = matcher,
          throwableMatcher = null,
          description = description,
        )
      )
    }
  }

  /**
   * Expect [Event.Complete] as the terminal event of [sourceName].
   *
   * This must be the last expectation declared for the source.
   *
   * @param description Optional human-readable description used in failure messages.
   */
  public fun expectComplete(sourceName: String, description: String? = null) {
    requireSource(sourceName)
    addNode(
      WindowCondition(
        index = nodes.size + 1,
        sourceName = sourceName,
        kind = EventWindowKind.Complete,
        itemMatcher = null,
        throwableMatcher = null,
        description = description,
      )
    )
  }

  /**
   * Expect an [Event.Error] terminating [sourceName].
   *
   * This must be the last expectation declared for the source. [Event.Error] is matched here and is
   * never delivered to an item matcher.
   *
   * @param matcher Optional predicate applied to the terminating [Throwable]. When null any error
   *   matches.
   * @param description Optional human-readable description used in failure messages.
   */
  public fun expectError(
    sourceName: String,
    description: String? = null,
    matcher: ((Throwable) -> Boolean)? = null,
  ) {
    requireSource(sourceName)
    addNode(
      WindowCondition(
        index = nodes.size + 1,
        sourceName = sourceName,
        kind = EventWindowKind.Error,
        itemMatcher = null,
        throwableMatcher = matcher,
        description = description,
      )
    )
  }

  /**
   * Expect exactly one of the alternatives declared in [block] to be consumed.
   *
   * The alternatives may be on different sources, in which case the first alternative to see a
   * matching event wins and no order between those sources is imposed. Multiple alternatives on the
   * same source are allowed and are tried in declaration order. As soon as one alternative is
   * consumed the whole `expectAnyOf` expectation is satisfied and its other alternatives are
   * abandoned.
   */
  public fun expectAnyOf(
    description: String? = null,
    block: EventWindowAnyOfBuilder.() -> Unit,
  ) {
    val group = EventWindowAnyOfBuilder(sourceByName).apply(block)
    check(group.alternatives.isNotEmpty()) {
      "expectAnyOf must declare at least one alternative."
    }
    val groupNode =
      WindowAnyOf(
        index = nodes.size + 1,
        description = description,
        alternatives = group.alternatives,
      )
    // Validate per-source terminal ordering using the flattened alternatives.
    for (alternative in group.alternatives) {
      val sourceName = alternative.sourceName
      val priorTerminal = nodes.any { existing ->
        existing is WindowCondition &&
          existing.sourceName == sourceName &&
          existing.kind != EventWindowKind.Item
      }
      check(!priorTerminal) {
        "No expectations for \"$sourceName\" may be declared after its complete/error " +
          "expectation; a terminal event is always the last event of a source."
      }
    }
    nodes += groupNode
  }
}

/**
 * Declares the alternatives of an [EventWindowBuilder.expectAnyOf] expectation. Exactly one
 * alternative will be consumed.
 */
@TurbineWindowDsl
public class EventWindowAnyOfBuilder
@PublishedApi
internal constructor(sources: Map<String, NamedReceiveTurbine<*>>) {
  internal val alternatives = mutableListOf<WindowCondition>()
  private val sourceByName = sources

  private fun requireSource(sourceName: String): NamedReceiveTurbine<*> {
    return sourceByName[sourceName]
      ?: throw AssertionError(
        "Unknown window source \"$sourceName\". Bound sources: ${sourceByName.keys.sorted()}."
      )
  }

  /** An item alternative from [sourceName], optionally matched by [matcher]. */
  public fun item(
    sourceName: String,
    description: String? = null,
    matcher: ((Any?) -> Boolean)? = null,
  ) {
    requireSource(sourceName)
    alternatives +=
      WindowCondition(
        index = alternatives.size + 1,
        sourceName = sourceName,
        kind = EventWindowKind.Item,
        itemMatcher = matcher,
        throwableMatcher = null,
        description = description,
      )
  }

  /** An item alternative from [sourceName] whose value is [value]. */
  public fun item(sourceName: String, value: Any?, description: String? = null) {
    item(sourceName, description) { it == value }
  }

  /** A completion alternative from [sourceName]. */
  public fun complete(sourceName: String, description: String? = null) {
    requireSource(sourceName)
    alternatives +=
      WindowCondition(
        index = alternatives.size + 1,
        sourceName = sourceName,
        kind = EventWindowKind.Complete,
        itemMatcher = null,
        throwableMatcher = null,
        description = description,
      )
  }

  /** An error alternative from [sourceName], optionally matched by [matcher]. */
  public fun error(
    sourceName: String,
    description: String? = null,
    matcher: ((Throwable) -> Boolean)? = null,
  ) {
    requireSource(sourceName)
    alternatives +=
      WindowCondition(
        index = alternatives.size + 1,
        sourceName = sourceName,
        kind = EventWindowKind.Error,
        itemMatcher = null,
        throwableMatcher = matcher,
        description = description,
      )
  }
}

/** The three Turbine event kinds, kept distinct so errors are never matched as items. */
public enum class EventWindowKind {
  Item,
  Complete,
  Error,
}

internal sealed interface WindowNode {
  public val index: Int
  public val description: String?
}

internal class WindowCondition(
  override val index: Int,
  val sourceName: String,
  val kind: EventWindowKind,
  val itemMatcher: ((Any?) -> Boolean)?,
  val throwableMatcher: ((Throwable) -> Boolean)?,
  override val description: String?,
) : WindowNode

internal class WindowAnyOf(
  override val index: Int,
  override val description: String?,
  val alternatives: List<WindowCondition>,
) : WindowNode

private class HeldEvent(val sourceName: String, val event: Event<*>)

internal class EventWindowMatcher(
  private val sources: Map<String, NamedReceiveTurbine<*>>,
  nodes: List<WindowNode>,
) {
  private val nodes = nodes.toList()

  private val perSource: Map<String, ArrayDeque<WindowNode>> = run {
    val map = mutableMapOf<String, ArrayDeque<WindowNode>>()
    for (node in this.nodes) {
      when (node) {
        is WindowCondition -> map.getOrPut(node.sourceName) { ArrayDeque() }.addLast(node)
        is WindowAnyOf ->
          node.alternatives.forEach { alt ->
            map.getOrPut(alt.sourceName) { ArrayDeque() }.addLast(node)
          }
      }
    }
    map
  }

  private val satisfied = LinkedHashMap<WindowNode, MutableList<Event<*>>>()
  private val terminated = mutableSetOf<String>()
  private val held = ArrayDeque<HeldEvent>()
  private val consumed = LinkedHashMap<String, MutableList<Event<*>>>()

  private val channels: Map<String, ReceiveChannel<*>> = sources.mapValues {
    it.value.turbine.asChannel()
  }

  suspend fun run() {
    drainHeld()
    if (allSatisfied()) return
    checkImpossible()

    while (true) {
      val readable = activeChannels()
      if (readable.isEmpty()) fail("No active sources remain")
      val (sourceName, event) = selectEvent(readable)
      recordConsumed(sourceName, event)
      markTerminalIfNeeded(sourceName, event)
      process(sourceName, event)
      drainHeld()
      if (allSatisfied()) return
      checkImpossible()
    }
  }

  fun failOnTimeout(timeout: Duration): Nothing {
    fail("No matching event window produced in $timeout")
  }

  private fun allSatisfied(): Boolean = nodes.all { it in satisfied }

  private fun activeChannels(): List<Pair<String, ReceiveChannel<*>>> {
    return sources.keys
      .filter { name ->
        name !in terminated && perSource[name].orEmpty().any { it !in satisfied }
      }
      .map { it to channels.getValue(it) }
  }

  private suspend fun selectEvent(
    readable: List<Pair<String, ReceiveChannel<*>>>
  ): Pair<String, Event<*>> = select {
    for ((name, channel) in readable) {
      @Suppress("UNCHECKED_CAST")
      (channel as ReceiveChannel<Any?>).onReceiveCatching { result ->
        val event = result.toEvent()!!
        name to event
      }
    }
  }

  private fun recordConsumed(sourceName: String, event: Event<*>) {
    consumed.getOrPut(sourceName) { mutableListOf() }.add(event)
  }

  /**
   * `SelectClause1` registration bypasses the delegating channel returned by `asChannel`, so the
   * terminal-event bookkeeping performed by its `tryReceive`/`receiveCatching` overrides does not
   * run. Terminal results are sticky on a closed channel, so re-reading via `tryReceive` performs
   * that bookkeeping without consuming an additional event.
   */
  private fun markTerminalIfNeeded(sourceName: String, event: Event<*>) {
    if (event.isTerminal) {
      channels.getValue(sourceName).tryReceive()
    }
  }

  private fun process(sourceName: String, event: Event<*>) {
    val queue = perSource.getValue(sourceName)
    trimSatisfied(queue)
    if (queue.isEmpty()) {
      if (event.isTerminal) terminated += sourceName
      failUnexpected(sourceName, event)
    }
    when (val head = queue.first()) {
      is WindowCondition -> matchCondition(sourceName, head, event)
      is WindowAnyOf -> matchAnyOf(sourceName, head, event)
    }
  }

  private fun trimSatisfied(queue: ArrayDeque<WindowNode>) {
    while (queue.isNotEmpty() && queue.first() in satisfied) queue.removeFirst()
  }

  private fun matchCondition(sourceName: String, condition: WindowCondition, event: Event<*>) {
    if (!event.matches(condition)) {
      if (event.isTerminal) terminated += sourceName
      failUnexpected(sourceName, event)
    }
    satisfied.getOrPut(condition) { mutableListOf() }.add(event)
    if (event.isTerminal) terminated += sourceName
  }

  private fun matchAnyOf(sourceName: String, group: WindowAnyOf, event: Event<*>) {
    val alternative =
      group.alternatives.firstOrNull { it.sourceName == sourceName && event.matches(it) }
    if (alternative == null) {
      if (event.isTerminal) {
        terminated += sourceName
        failUnexpected(sourceName, event)
      }
      held.addLast(HeldEvent(sourceName, event))
      return
    }
    satisfied.getOrPut(group) { mutableListOf() }.add(event)
    if (event.isTerminal) terminated += sourceName
  }

  /**
   * Retry held events against the current heads of their source queues.
   *
   * Held events have already been consumed from their turbines and are never restored, so a held
   * event that no longer matches the new head is an immediate failure.
   */
  private fun drainHeld() {
    var progressed = true
    while (progressed && held.isNotEmpty()) {
      progressed = false
      val snapshot = held.toList()
      held.clear()
      for (heldEvent in snapshot) {
        val sourceName = heldEvent.sourceName
        val event = heldEvent.event
        val queue = perSource.getValue(sourceName)
        trimSatisfied(queue)
        if (queue.isEmpty()) {
          if (event.isTerminal) terminated += sourceName
          failUnexpected(sourceName, event)
        }
        when (val head = queue.first()) {
          is WindowCondition -> {
            if (!event.matches(head)) {
              if (event.isTerminal) terminated += sourceName
              failUnexpected(sourceName, event)
            }
            satisfied.getOrPut(head) { mutableListOf() }.add(event)
            if (event.isTerminal) terminated += sourceName
            progressed = true
          }
          is WindowAnyOf -> {
            val alternative =
              head.alternatives.firstOrNull { it.sourceName == sourceName && event.matches(it) }
            if (alternative != null) {
              satisfied.getOrPut(head) { mutableListOf() }.add(event)
              if (event.isTerminal) terminated += sourceName
              progressed = true
            } else {
              if (event.isTerminal) {
                terminated += sourceName
                failUnexpected(sourceName, event)
              }
              // Group is still unresolved by another source; keep holding.
              held.addLast(heldEvent)
            }
          }
        }
      }
    }
  }

  private fun checkImpossible() {
    if (collectImpossible().isNotEmpty()) {
      fail("Sources terminated before all expectations were met")
    }
  }

  private fun collectImpossible(): List<WindowNode> {
    val result = mutableListOf<WindowNode>()
    for (node in nodes) {
      if (node in satisfied) continue
      val possibleSources =
        when (node) {
          is WindowCondition -> listOf(node.sourceName)
          is WindowAnyOf -> node.alternatives.map { it.sourceName }.distinct()
        }
      if (possibleSources.all { it in terminated }) result += node
    }
    return result
  }

  private fun failUnexpected(sourceName: String, event: Event<*>): Nothing {
    val queue = perSource[sourceName]
    val head = queue?.firstOrNull { it !in satisfied }
    val expected = head?.let { describeNode(it) } ?: "no further events"
    fail(
      "Found ${describeEvent(event)} from ${describeSource(sourceName)} but expected $expected",
      cause = (event as? Event.Error)?.throwable,
    )
  }

  /**
   * Renders the bound label, and also the turbine's own name (from `testIn`/`test`/`Turbine`) when
   * the two differ, so existing Turbine name context is preserved in window failures.
   */
  private fun describeSource(sourceName: String): String {
    val ownName =
      (sources.getValue(sourceName).turbine as? NamedEventWindowSource)?.windowTurbineName
    return if (ownName != null && ownName != sourceName) {
      "\"$sourceName\" (turbine \"$ownName\")"
    } else {
      "\"$sourceName\""
    }
  }

  private fun fail(reason: String, cause: Throwable? = null): Nothing {
    val impossible = collectImpossible()
    val impossibleSet = impossible.toSet()
    val pending = nodes.filter { it !in satisfied && it !in impossibleSet }

    val message = buildString {
      append("Event window failed".qualifiedBy(windowName()))
      append(": ")
      appendLine(reason)

      appendSection("Consumed", nodes.filter { it in satisfied }) { node ->
        val where = satisfied.getValue(node).joinToString { describeEvent(it) }
        append(" - ").append(describeNode(node)).append(" <- ").appendLine(where)
      }
      appendSection("Remaining to be met", pending) { node ->
        appendLine(" - ${describeNode(node)}")
      }
      appendSection("No longer satisfiable (source terminated)", impossible) { node ->
        appendLine(" - ${describeNode(node)}")
      }

      if (consumed.isNotEmpty()) {
        appendLine("Events consumed during the window (not restored):")
        for ((sourceName, events) in consumed) {
          for (event in events) {
            append(" - ")
              .append(describeSource(sourceName))
              .append(": ")
              .appendLine(describeEvent(event))
          }
        }
      }
    }
    throw TurbineAssertionError(message.trimEnd(), cause)
  }

  private inline fun StringBuilder.appendSection(
    title: String,
    sectionNodes: List<WindowNode>,
    render: StringBuilder.(WindowNode) -> Unit,
  ) {
    appendLine("$title:")
    if (sectionNodes.isEmpty()) {
      appendLine(" - (none)")
    } else {
      for (node in sectionNodes) render(node)
    }
  }

  private fun windowName(): String {
    val labels = sources.keys.sorted().joinToString { "\"$it\"" }
    return "window of $labels"
  }

  private fun describeNode(node: WindowNode): String {
    val base = node.description ?: defaultNodeDescription(node)
    return "#${node.index} $base"
  }

  private fun defaultNodeDescription(node: WindowNode): String {
    return when (node) {
      is WindowCondition -> describeCondition(node)
      is WindowAnyOf ->
        "any one of " +
          node.alternatives.joinToString(prefix = "{", postfix = "}") { describeCondition(it) }
    }
  }

  private fun describeCondition(condition: WindowCondition): String {
    val kind =
      when (condition.kind) {
        EventWindowKind.Item ->
          if (condition.itemMatcher == null) "any item" else "item matching predicate"
        EventWindowKind.Complete -> "complete"
        EventWindowKind.Error ->
          if (condition.throwableMatcher == null) "any error" else "error matching predicate"
      }
    return "$kind from \"${condition.sourceName}\""
  }

  private fun describeEvent(event: Event<*>): String = event.toString()
}

private fun Event<*>.matches(condition: WindowCondition): Boolean {
  return when (condition.kind) {
    EventWindowKind.Item -> this is Event.Item<*> && (condition.itemMatcher?.invoke(value) ?: true)
    EventWindowKind.Complete -> this === Event.Complete
    EventWindowKind.Error ->
      this is Event.Error && (condition.throwableMatcher?.invoke(throwable) ?: true)
  }
}

/**
 * Exposes the internal Turbine name to the event window implementation without widening the public
 * [ReceiveTurbine] API.
 */
internal interface NamedEventWindowSource {
  public val windowTurbineName: String?
}
