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
import kotlin.coroutines.*
import kotlin.jvm.*

/**
 * Represents the given broadcast channel as a hot flow.
 * Every flow collector will trigger a new broadcast channel subscription.
 *
 * ### Cancellation semantics
 * 1) Flow consumer is cancelled when the original channel is cancelled.
 * 2) Flow consumer completes normally when the original channel completes (~is closed) normally.
 * 3) If the flow consumer fails with an exception, subscription is cancelled.
 */
@FlowPreview
public fun <T> BroadcastChannel<T>.asFlow(): Flow<T> = flow {
    val subscription = openSubscription()
    subscription.consumeEach { value ->
        emit(value)
    }
}

/**
 * Creates a [broadcast] coroutine that collects the given flow.
 *
 * This transformation is **stateful**, it launches a [broadcast] coroutine
 * that collects the given flow and thus resulting channel should be properly closed or cancelled.
 *
 * A channel with [default][Channel.Factory.DEFAULT] buffer size is created.
 * Use [buffer] operator on the flow before calling `produce` to specify a value other than
 * default and to control what happens when data is produced faster than it is consumed,
 * that is to control backpressure behavior.
 */
@FlowPreview
public fun <T> Flow<T>.broadcastIn(
    scope: CoroutineScope,
    start: CoroutineStart = CoroutineStart.LAZY
): BroadcastChannel<T> =
    asChannelFlow().broadcastImpl(scope, start)


/**
 * Creates a [produce] coroutine that collects the given flow.
 *
 * This transformation is **stateful**, it launches a [produce] coroutine
 * that collects the given flow and thus resulting channel should be properly closed or cancelled.
 *
 * A channel with [default][Channel.Factory.DEFAULT] buffer size is created.
 * Use [buffer] operator on the flow before calling `produce` to specify a value other than
 * default and to control what happens when data is produced faster than it is consumed,
 * that is to control backpressure behavior.
 */
@FlowPreview
public fun <T> Flow<T>.produceIn(
    scope: CoroutineScope
): ReceiveChannel<T> =
    asChannelFlow().produceImpl(scope)

internal fun <T> Flow<T>.asChannelFlow() =
    this as? ChannelFlow ?: ChannelFlowOperatorImpl(this)

// Operators that use channels extend this ChannelFlow and are always fused with each other
internal abstract class ChannelFlow<T>(
    // upstream context
    @JvmField val context: CoroutineContext,
    // buffer capacity between upstream and downstream context
    @JvmField val capacity: Int
) : Flow<T> {
    public fun update(
        context: CoroutineContext = EmptyCoroutineContext,
        capacity: Int = OPTIONAL_CHANNEL
    ): ChannelFlow<T> {
        // note: previous upstream context (specified before) takes precedence
        val newContext = context + this.context
        val newCapacity = when {
            this.capacity == OPTIONAL_CHANNEL -> capacity
            capacity == OPTIONAL_CHANNEL -> this.capacity
            this.capacity == DEFAULT -> capacity
            capacity == DEFAULT -> this.capacity
            this.capacity == CONFLATED -> CONFLATED
            capacity == CONFLATED -> CONFLATED
            else -> {
                check(this.capacity >= 0)
                check(capacity >= 0)
                val sum = this.capacity + capacity
                if (sum >= 0) sum else Channel.UNLIMITED // unlimited on int overflow
            }
        }
        if (newContext == this.context && newCapacity == this.capacity) return this
        return create(newContext, newCapacity)
    }

    protected abstract fun create(context: CoroutineContext, capacity: Int): ChannelFlow<T>

    protected abstract suspend fun collectTo(scope: ProducerScope<T>)

    // shared code to create a suspend lambda from collectTo function in one place
    private val collectToFun: suspend (ProducerScope<T>) -> Unit
        get() = { collectTo(it) }

    private val produceCapacity: Int
        get() = if (capacity == OPTIONAL_CHANNEL) DEFAULT else capacity

    fun broadcastImpl(scope: CoroutineScope, start: CoroutineStart): BroadcastChannel<T> =
        scope.broadcast(context, produceCapacity, start, block = collectToFun)

    fun produceImpl(scope: CoroutineScope): ReceiveChannel<T> =
        scope.produce(context, produceCapacity, block = collectToFun)

    override suspend fun collect(collector: FlowCollector<T>) =
        coroutineScope { // todo: flowScope
            val channel = produceImpl(this)
            channel.consumeEach { collector.emit(it) }
        }

    // debug toString
    override fun toString(): String =
        "$classSimpleName[${additionalToStringProps()}context=$context, capacity=$capacity]"

    open fun additionalToStringProps() = ""
}

