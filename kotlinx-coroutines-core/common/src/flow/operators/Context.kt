/*
 * Copyright 2016-2019 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

@file:JvmMultifileClass
@file:JvmName("FlowKt")

package kotlinx.coroutines.flow

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.channels.Channel.Factory.CONFLATED
import kotlinx.coroutines.channels.Channel.Factory.DEFAULT
import kotlinx.coroutines.channels.Channel.Factory.OPTIONAL_CHANNEL
import kotlinx.coroutines.internal.threadContextElements
import kotlin.coroutines.*
import kotlin.coroutines.intrinsics.*
import kotlin.jvm.*

/**
 * Buffers flow emissions via channel of a specified capacity and runs collector in a separate coroutine.
 *
 * Normally, [flows][Flow] are _sequential_. It means that the code of all operators is executed in the
 * same coroutine. For example, consider the following code using [onEach] and [collect] operators:
 *
 * ```
 * flowOf("A", "B", "C")
 *     .onEach  { println("1$it") }
 *     .collect { println("2$it") }
 * ```
 *
 * It is going to be executed in the following order by the coroutine `Q` that calls this code:
 *
 * ```
 * Q : -->-- [1A] -- [2A] -- [1B] -- [2B] -- [1C] -- [2C] -->--
 * ```
 *
 * So if the operator's code takes considerable time to execute, then the total execution time is going to be
 * the sum of execution times for all operators.
 *
 * The `buffer` operator creates a separate coroutine during execution for the flow it applies to.
 * Consider the following code:
 *
 * ```
 * flowOf("A", "B", "C")
 *     .onEach  { println("1$it") }
 *     .buffer()  // <--------------- buffer between onEach and collect
 *     .collect { println("2$it") }
 * ```
 *
 * It will use two coroutines for execution of the code. A coroutine `Q` that calls this code is
 * going to execute `collect`, and the code before `buffer` will be executed in a separate
 * new coroutine `P` concurrently with `Q`:
 *
 * ```
 * P : -->-- [1A] -- [1B] -- [1C] ---------->--  // flowOf(...).onEach { ... }
 *
 *                       |
 *                       | channel               // buffer()
 *                       V
 *
 * Q : -->---------- [2A] -- [2B] -- [2C] -->--  // collect
 * ```
 *
 * When operator's code takes time to execute this decreases the total execution time of the flow.
 * A [channel][Channel] is used between the coroutines to send elements emitted by the coroutine `P` to
 * the coroutine `Q`. If the code before `buffer` operator (in the coroutine `P`) is faster than the code after
 * `buffer` operator (in the coroutine `Q`), then this channel will become full at some point and will suspend
 * the producer coroutine `P` until the consumer coroutine `Q` catches up.
 * The [capacity] parameter defines the size of this buffer.
 *
 * Adjacent applications of [channelFlow], [flowOn], [buffer], [produceIn], and [broadcastIn] are
 * always fused so that only one properly configured channel is used for execution.
 *
 * @param capacity capacity of the buffer between coroutines. Allowed values are the same as in `Channel(...)`
 * factory function: [DEFAULT][Channel.DEFAULT] (by default), [CONFLATED][Channel.CONFLATED],
 * [RENDEZVOUS][Channel.RENDEZVOUS], [UNLIMITED][Channel.UNLIMITED] or a non-negative value indicating
 * a requested size.
 */
@FlowPreview
public fun <T> Flow<T>.buffer(capacity: Int = DEFAULT): Flow<T> {
    require(capacity >= 0 || capacity == DEFAULT || capacity == CONFLATED) {
        "Buffer size should be non-negative, DEFAULT, or CONFLATED, but was $capacity"
    }
    return if (this is ChannelFlow)
        update(capacity = capacity)
    else
        ChannelFlowOperatorImpl(this, capacity = capacity)
}

// todo: conflate would be a useful operator only when Channel.CONFLATE is changed to always deliver the last send value
//@FlowPreview
//public fun <T> Flow<T>.conflate(): Flow<T> = buffer(CONFLATED)

/**
 * The operator that changes the context where this flow is executed to the given [context].
 * This operator is composable and affects only preceding operators that do not have its own context.
 * This operator is context preserving: [context] **does not** leak into the downstream flow.
 *
 * For example:
 *
 * ```
 * withContext(Dispatchers.Main) {
 *     val singleValue = intFlow // will be executed on IO if context wasn't specified before
 *         .map { ... } // Will be executed in IO
 *         .flowOn(Dispatchers.IO)
 *         .filter { ... } // Will be executed in Default
 *         .flowOn(Dispatchers.Default)
 *         .single() // Will be executed in the Main
 * }
 * ```
 *
 * For more explanation of context preservation please refer to [Flow] documentation.
 *
 * This operators retains a _sequential_ nature of flow if changing the context does not call for changing
 * the [dispatcher][CoroutineDispatcher]. Otherwise, if changing dispatcher is required, it collects
 * flow emissions in one coroutine that is run using a specified [context] and emits them from another coroutines
 * with the original collector's context using a channel with a [default][Channel.DEFAULT] buffer size
 * between two coroutines similarly to [buffer] operator, unless [buffer] operator is explicitly called
 * before or after `flowOn`, which requests buffering behavior and specifies channel size.
 *
 * Adjacent applications of [channelFlow], [flowOn], [buffer], [produceIn], and [broadcastIn] are
 * always fused so that only one properly configured channel is used for execution.
 *
 * @throws [IllegalArgumentException] if provided context contains [Job] instance.
 */
@FlowPreview
public fun <T> Flow<T>.flowOn(context: CoroutineContext): Flow<T> {
    checkFlowContext(context)
    return when {
        context == EmptyCoroutineContext -> this
        this is ChannelFlow -> update(context = context)
        else -> ChannelFlowOperatorImpl(this, context = context)
    }
}

/**
 * The operator that changes the context where all transformations applied to the given flow within a [builder] are executed.
 * This operator is context preserving and does not affect the context of the preceding and subsequent operations.
 *
 * Example:
 *
 * ```
 * flow // not affected
 *     .map { ... } // Not affected
 *     .flowWith(Dispatchers.IO) {
 *         map { ... } // in IO
 *         .filter { ... } // in IO
 *     }
 *     .map { ... } // Not affected
 * ```
 *
 * For more explanation of context preservation please refer to [Flow] documentation.
 *
 * `flowWith` operator has similar optimizations to [flowOn] (see) and can be also
 * explicitly configured with [buffer]. Adjacent applications of [channelFlow],
 * [flowOn], [flowWith], [buffer], [produceIn], and [broadcastIn] are
 * always fused so that only one properly configured channel is used for execution.
 *
 * @throws [IllegalArgumentException] if provided context contains [Job] instance.
 */
@FlowPreview
public fun <T, R> Flow<T>.flowWith(
    context: CoroutineContext,
    builder: Flow<T>.() -> Flow<R>
): Flow<R> {
    checkFlowContext(context)
    return FlowWithOperator(this, context, builder)
}

// FlowWith lazily applies a builder during collection time taking the actual collectContext into account
private class FlowWithOperator<S, T>(
    private val flow: Flow<S>,
    private val context: CoroutineContext,
    private val builder: Flow<S>.() -> Flow<T>
) : Flow<T> {
    fun buildFlow(collectContext: CoroutineContext): Flow<T> {
        // we only restore context elements that are explicitly specified in the context
        val restoredContext = context.fold<CoroutineContext>(EmptyCoroutineContext) { acc, element ->
            val restoredElement = collectContext[element.key]
            if (restoredElement == null) acc else acc + restoredElement
        }
        // todo: what if the element was not present in the context? shall we remove it?
        return flow.flowOn(restoredContext).builder().flowOn(context)
    }

    override suspend fun collect(collector: FlowCollector<T>) =
        buildFlow(coroutineContext).collect(collector)

    // debug toString
    override fun toString(): String =
        "$flow -> flowWith[context=$context, builder=$builder]"
}

// ChannelFlow implementation that operates on another flow before it
internal abstract class ChannelFlowOperator<S, T>(
    @JvmField val flow: Flow<S>,
    context: CoroutineContext,
    capacity: Int
) : ChannelFlow<T>(context, capacity) {
    protected abstract suspend fun flowCollect(collector: FlowCollector<T>)

    // Changes collecting context upstream to the specified newContext, while collecting in the original context
    private suspend fun collectWithContextUndispatched(collector: FlowCollector<T>, newContext: CoroutineContext) {
        val originalContextCollector = collector.withUndispatchedContextCollector(coroutineContext)
        // invoke flowCollect(originalContextCollector) in the newContext
        val flowCollectRef: suspend (FlowCollector<T>) -> Unit = { flowCollect(it) }
        return withContextUndispatched(newContext, block = flowCollectRef, value = originalContextCollector)
    }

    // Slow path when output channel is required
    protected override suspend fun collectTo(scope: ProducerScope<T>) =
        flowCollect(SendingCollector(scope))

    // Optimizations for fast-path when channel creation is optional
    override suspend fun collect(collector: FlowCollector<T>) {
        // Fast-path: When channel creation is optional (flowOn/flowWith operators without buffer)
        if (capacity == OPTIONAL_CHANNEL) {
            val collectContext = coroutineContext
            val newContext = collectContext + context // compute resulting collect context
            // #1: If the resulting context happens to be the same as it was -- fallback to plain collect
            if (newContext == collectContext)
                return flowCollect(collector)
            // #2: If we don't need to change the dispatcher we can go without channels
            if (newContext[ContinuationInterceptor] == collectContext[ContinuationInterceptor])
                return collectWithContextUndispatched(collector, newContext)
        }
        // Slow-path: create the actual channel
        super.collect(collector)
    }

    // debug toString
    override fun toString(): String = "$flow -> ${super.toString()}"
}

internal class ChannelFlowOperatorImpl<T>(
    flow: Flow<T>,
    context: CoroutineContext = EmptyCoroutineContext,
    capacity: Int = OPTIONAL_CHANNEL
) : ChannelFlowOperator<T, T>(flow, context, capacity) {
    override fun create(context: CoroutineContext, capacity: Int): ChannelFlow<T> =
        ChannelFlowOperatorImpl(flow, context, capacity)

    override suspend fun flowCollect(collector: FlowCollector<T>) =
        flow.collect(collector)

    override suspend fun collect(collector: FlowCollector<T>) {
        // fuse with upstream flowWith operator
        if (flow is FlowWithOperator<*, T>) {
            val upstreamContext = coroutineContext + context
            // materialize upstream flow now with our effective upstream context context and see what we get
            val fused = when (val upstream = flow.buildFlow(upstreamContext)) {
                is ChannelFlow -> upstream.update(context, capacity)
                else -> ChannelFlowOperatorImpl(upstream, context, capacity)
            }
            // now collect from the resulting fused flow
            return fused.collect(collector)
        }
        // otherwise collect normally
        super.collect(collector)
    }
}

internal class SendingCollector<T>(
    private val channel: SendChannel<T>
) : ConcurrentFlowCollector<T> {
    override suspend fun emit(value: T) = channel.send(value)
}

// Now if the underlying collector was accepting concurrent emits, then this one is too
// todo: we might need to generalize this pattern for "thread-safe" operators that can fuse with channels
private fun <T> FlowCollector<T>.withUndispatchedContextCollector(emitContext: CoroutineContext): FlowCollector<T> = when (this) {
    // SendingCollector does not care about the context at all so can be used as it
    is SendingCollector -> this
    // Original collector is concurrent, so wrap into UndispatchedContextCollector and tag the result with ConcurrentFlowCollector interface
    is ConcurrentFlowCollector ->
        object : UndispatchedContextCollector<T>(this, emitContext), ConcurrentFlowCollector<T> {}
    // Otherwise just wrap into UndispatchedContextCollector interface implementation
    else -> UndispatchedContextCollector(this, emitContext)
}

private open class UndispatchedContextCollector<T>(
    downstream: FlowCollector<T>,
    private val emitContext: CoroutineContext
) : FlowCollector<T> {
    private val countOrElement = threadContextElements(emitContext) // precompute for fast withContextUndispatched
    private val emitRef: suspend (T) -> Unit = { downstream.emit(it) } // allocate suspend function ref once on creation

    override suspend fun emit(value: T): Unit =
        withContextUndispatched(emitContext, countOrElement, emitRef, value)
}

// Efficiently computes block(value) in the newContext
private suspend fun <T, V> withContextUndispatched(
    newContext: CoroutineContext,
    countOrElement: Any = threadContextElements(newContext), // can be precomputed for speed
    block: suspend (V) -> T, value: V
): T =
    suspendCoroutineUninterceptedOrReturn sc@{ uCont ->
        withCoroutineContext(newContext, countOrElement) {
            block.startCoroutineUninterceptedOrReturn(value, Continuation(newContext) {
                uCont.resumeWith(it)
            })
        }
    }

private fun checkFlowContext(context: CoroutineContext) {
    require(context[Job] == null) {
        "Flow context cannot contain job in it. Had $context"
    }
}
