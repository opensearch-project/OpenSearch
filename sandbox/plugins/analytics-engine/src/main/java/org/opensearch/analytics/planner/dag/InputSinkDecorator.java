/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.analytics.spi.ExchangeSink;

/**
 * Wraps a stage's incoming child sink with feature-specific decoration before
 * the producer's batches reach it. Set on the {@link Stage} at DAG-build time
 * (today only by {@code DAGBuilder.cutAtLateMaterialization}); applied at
 * sink-resolution time inside the parent execution's {@code inputSink(...)}.
 *
 * <p>Implementations are stateless factories: each invocation builds a fresh
 * wrapper around the supplied sink. The wrapper does not own the sink's
 * lifecycle — close semantics delegate to the wrapped sink.
 *
 * <p>Today's only producer of this hook is the QTF (late-materialization) DAG
 * cut, which installs an {@code OrdinalAppendingSink} so each shard's batches
 * carry a {@code ___ugsi} ordinal before reduce. The interface is generic so
 * future cross-cutting concerns can reuse it without touching reducer code.
 *
 * @opensearch.internal
 */
@FunctionalInterface
public interface InputSinkDecorator {

    /**
     * Build a new {@link ExchangeSink} that wraps {@code sink} with this decorator's
     * behavior. The returned sink delegates lifecycle (close) to the wrapped sink.
     *
     * @param sink      the producer-facing sink to wrap (already resolved per-child if
     *                  the underlying sink is a {@link org.opensearch.analytics.spi.MultiInputExchangeSink})
     * @param allocator allocator the decorator may use for any buffers it allocates
     */
    ExchangeSink decorate(ExchangeSink sink, BufferAllocator allocator);
}
