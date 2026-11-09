/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.spi.ExchangeSink;

/**
 * Pure decorator over an {@link ExchangeSink} that stamps a per-source ordinal
 * onto every batch as a constant Int32 column. Used by Late Materialization
 * (Query-Then-Fetch) to mark each shard's batches with their {@code ___ugsi}
 * before the rows hit the reduce sink, so post-reduce code can group by source
 * shard for fan-out fetches.
 *
 * <p>Lifecycle is delegated entirely to the wrapped sink — this decorator only
 * adds a column on the {@code feed(VSR, int)} path.
 *
 * <p>Append is zero-copy: the new VSR shares existing {@link org.apache.arrow.vector.FieldVector}s
 * with the input by reference; only the constant ordinal column is freshly
 * allocated. See {@link VectorUtils#appendConstantInt}.
 *
 * @opensearch.internal
 */
public final class OrdinalAppendingSink implements ExchangeSink {

    private final ExchangeSink delegate;
    private final BufferAllocator allocator;
    private final String columnName;

    public OrdinalAppendingSink(ExchangeSink delegate, BufferAllocator allocator, String columnName) {
        this.delegate = delegate;
        this.allocator = allocator;
        this.columnName = columnName;
    }

    @Override
    public void feed(VectorSchemaRoot batch, int sourceOrdinal) {
        VectorSchemaRoot withOrdinal = VectorUtils.appendConstantInt(batch, columnName, sourceOrdinal, allocator);
        delegate.feed(withOrdinal);
    }

    @Override
    public void feed(VectorSchemaRoot batch) {
        delegate.feed(batch);
    }

    @Override
    public void close() {
        delegate.close();
    }
}
