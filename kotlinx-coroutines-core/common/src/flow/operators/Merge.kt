/*
 * Copyright 2016-2019 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

@file:JvmMultifileClass
@file:JvmName("FlowKt")
@file:Suppress("unused")

package kotlinx.coroutines.flow

import kotlinx.atomicfu.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.channels.Channel.Factory.OPTIONAL_CHANNEL
import kotlinx.coroutines.flow.internal.*
import kotlinx.coroutines.internal.systemProp
import kotlin.coroutines.*
import kotlin.jvm.*
import kotlinx.coroutines.flow.unsafeFlow as flow

/**
 * Transforms elements emitted by the original flow by applying [transform], that returns another flow,
 * and then concatenating and flattening these flows.
 *
 * This method is is a shortcut for `map(transform).flattenConcat()`. See [flattenConcat].
 *
 * Note that even though this operator looks very familiar, we discourage its usage in a regular application-specific flows.
 * Most likely, suspending operation in [map] operator will be sufficient and linear transformations are much easier to reason about.
 */
@FlowPreview
public fun <T, R> Flow<T>.flatMapConcat(transform: suspend (value: T) -> Flow<R>): Flow<R> =
    map(transform).flattenConcat()

/**
 * Transforms elements emitted by the original flow by applying [transform], that returns another flow,
 * and then merging and flattening these flows.
 *
 * This operator calls [transform] *sequentially* and then merges the resulting flows with a [concurrency]
 * limit on the number of concurrently collected flows.
 * It is a shortcut for `map(transform).flattenMerge(concurrency)`.
 * See [flattenMerge] for details.
 *
 * Note that even though this operator looks very familiar, we discourage its usage in a regular application-specific flows.
 * Most likely, suspending operation in [map] operator will be sufficient and linear transformations are much easier to reason about.
 *
 * @param concurrency controls the number of in-flight flows, at most [concurrency] flows are collected
 * at the same time. By default it is equal to [DEFAULT_CONCURRENCY].
 */
@FlowPreview
public fun <T, R> Flow<T>.flatMapMerge(
    concurrency: Int = DEFAULT_CONCURRENCY,
    transform: suspend (value: T) -> Flow<R>
): Flow<R> =
    map(transform).flattenMerge(concurrency)

/**
 * Flattens the given flow of flows into a single flow in a sequentially manner, without interleaving nested flows.
 * This method is conceptually identical to `flattenMerge(concurrency = 1)` but has faster implementation.
 *
 * Inner flows are collected by this operator *sequentially*.
 */
@FlowPreview
public fun <T> Flow<Flow<T>>.flattenConcat(): Flow<T> = flow {
    collect { value ->
        value.collect { innerValue ->
            emit(innerValue)
        }
    }
}

/**
 * Flattens the given flow of flows into a single flow with a [concurrency] limit on the number of
 * concurrently collected flows.
 *
 * If [concurrency] is more than 1, then inner flows are be collected by this operator *concurrently*.
 * With `concurrency == 1` this operator is identical to [flattenConcat].
 *
 * @param concurrency controls the number of in-flight flows, at most [concurrency] flows are collected
 * at the same time. By default it is equal to [DEFAULT_CONCURRENCY].
 */
@FlowPreview
public fun <T> Flow<Flow<T>>.flattenMerge(concurrency: Int = DEFAULT_CONCURRENCY): Flow<T> {
    require(concurrency > 0) { "Expected positive concurrency level, but had $concurrency" }
    return if (concurrency == 1) flattenConcat() else ChannelFlowMerge(this, concurrency)
}

/**
 * Returns a flow that switches to a new flow produced by [transform] function every time the original flow emits a value.
 * When switch on the a flow is performed, the previous one is cancelled.
 *
 * For example, the following flow:
 * ```
 * flow {
 *     emit("a")
 *     delay(100)
 *     emit("b")
 * }.switchMap { value ->
 *     flow {
 *         emit(value + value)
 *         delay(200)
 *         emit(value + "_last")
 *     }
 * }
 * ```
 * produces `aa bb b_last`
 */
@FlowPreview
public fun <T, R> Flow<T>.switchMap(transform: suspend (value: T) -> Flow<R>): Flow<R> = flow {
    coroutineScope {
        var previousFlow: Job? = null
        collect { value ->
            // Linearize calls to emit as alternative to the channel. Bonus points for never-overlapping channels.
            previousFlow?.cancelAndJoin()
            // Undispatched to have better user experience in case of synchronous flows
            previousFlow = launch(start = CoroutineStart.UNDISPATCHED) {
                transform(value).collect { innerValue ->
                    emit(innerValue)
                }
            }
        }
    }
}

/**
 * Name of the property that defines the value of [DEFAULT_CONCURRENCY].
 */
@FlowPreview
public const val DEFAULT_CONCURRENCY_PROPERTY_NAME = "kotlinx.coroutines.flow.defaultConcurrency"

/**
 * Default concurrency limit that is used by [flattenMerge] and [flatMapMerge] operators.
 * It is 16 by default and can be changed on JVM using [DEFAULT_CONCURRENCY_PROPERTY_NAME] property.
 */
@FlowPreview
public val DEFAULT_CONCURRENCY = systemProp(DEFAULT_CONCURRENCY_PROPERTY_NAME,
    16, 1, Int.MAX_VALUE
)

// Flow collector that supports concurrent emit calls.
// It is internal for now but may be public in the future.
internal interface ConcurrentFlowCollector<T> : FlowCollector<T>

private fun <T> FlowCollector<T>.asConcurrentFlowCollector(): ConcurrentFlowCollector<T> =
    this as? ConcurrentFlowCollector<T> ?: SerializingCollector(this)

// Effectively serializes access to downstream collector for merging
// This is basically a converted from FlowCollector interface to ConcurrentFlowCollector
private class SerializingCollector<T>(
    private val downstream: FlowCollector<T>
) : ConcurrentFlowCollector<T> {
    // Let's try to leverage the fact that merge is never contended
    // Should be Any, but KT-30796
    private val _channel = atomic<ArrayChannel<Any?>?>(null)
    private val inProgressLock = atomic(false)

    private val channel: ArrayChannel<Any?>
        get() = _channel.updateAndGet { value ->
            if (value != null) return value
            ArrayChannel(CHANNEL_DEFAULT_CAPACITY)
        }!!

    public override suspend fun emit(value: T) {
        if (!inProgressLock.tryAcquire()) {
            channel.send(value ?: NullSurrogate)
            if (inProgressLock.tryAcquire()) {
                helpEmit()
            }
            return
        }
        downstream.emit(value)
        helpEmit()
    }

    @Suppress("UNCHECKED_CAST")
    private suspend fun helpEmit() {
        while (true) {
            while (true) {
                val element = _channel.value?.poll() ?: break // todo: pollOrClosed
                downstream.emit(NullSurrogate.unbox(element))
            }
            inProgressLock.release()
            // Enforce liveness
            if (_channel.value?.isEmpty != false || !inProgressLock.tryAcquire()) break
        }
    }
}

private class ChannelFlowMerge<T>(
    flow: Flow<Flow<T>>,
    private val concurrency: Int,
    context: CoroutineContext = EmptyCoroutineContext,
    capacity: Int = OPTIONAL_CHANNEL
) : ChannelFlowOperator<Flow<T>, T>(flow, context, capacity) {
    override fun create(context: CoroutineContext, capacity: Int): ChannelFlow<T> =
        ChannelFlowMerge(flow, concurrency, context, capacity)

    // The actual merge implementation with concurrency limit
    private suspend fun mergeImpl(scope: CoroutineScope, collector: ConcurrentFlowCollector<T>) {
        val semaphore = Channel<Unit>(concurrency)
        @Suppress("UNCHECKED_CAST")
        flow.collect { inner ->
            // TODO real semaphore (#94)
            semaphore.send(Unit) // Acquire concurrency permit
            scope.launch {
                try {
                    inner.collect(collector)
                } finally {
                    semaphore.receive() // Release concurrency permit
                }
            }
        }
    }

    // Fast path in ChannelFlowOperator calls this function (channel was not created yet)
    override suspend fun flowCollect(collector: FlowCollector<T>) {
        // this function should not have been invoked when channel was explicitly requested
        check(capacity == OPTIONAL_CHANNEL)
        coroutineScope { // todo: flowScope
            mergeImpl(this, collector.asConcurrentFlowCollector())
        }
    }

    // Slow path when output channel is required (and was created)
    override suspend fun collectTo(scope: ProducerScope<T>) =
        mergeImpl(scope, SendingCollector(scope))

    override fun additionalToStringProps(): String =
        "concurrency=$concurrency, "
}

@Suppress("NOTHING_TO_INLINE")
private inline fun AtomicBoolean.tryAcquire(): Boolean = compareAndSet(false, true)

@Suppress("NOTHING_TO_INLINE")
private inline fun AtomicBoolean.release() {
    value = false
}
