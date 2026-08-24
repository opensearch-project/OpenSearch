/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.opensearch.analytics.spi.ArrowBatchSource;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory.InputColumn;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.tasks.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/** Reusable factory for independent Lucene doc-values cursors. */
@SuppressForbidden(reason = "reference counting keeps the reader alive for the factory and source lifetimes")
public final class DocValuesBatchSourceFactory implements ArrowBatchSourceFactory {

    private static final Logger LOGGER = LogManager.getLogger(DocValuesBatchSourceFactory.class);

    private final IndexSearcher searcher;
    private final Weight weight;
    private final List<InputColumn> columns;
    private final BufferAllocator allocator;
    private final Task task;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final LongAdder directBatches = new LongAdder();
    private final LongAdder fallbackBatches = new LongAdder();
    private final LongAdder batches = new LongAdder();
    private final LongAdder rows = new LongAdder();
    private final LongAdder nullValues = new LongAdder();

    public DocValuesBatchSourceFactory(IndexSearcher searcher, Query query, List<InputColumn> columns, BufferAllocator allocator, Task task)
        throws java.io.IOException {
        searcher.getIndexReader().incRef();
        boolean success = false;
        try {
            IndexSearcher uncachedSearcher = new IndexSearcher(searcher.getIndexReader());
            uncachedSearcher.setSimilarity(searcher.getSimilarity());
            uncachedSearcher.setQueryCache(null);
            this.searcher = uncachedSearcher;
            this.weight = uncachedSearcher.createWeight(uncachedSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
            this.columns = List.copyOf(columns);
            this.allocator = allocator;
            this.task = task;
            success = true;
        } finally {
            if (success == false) {
                searcher.getIndexReader().decRef();
            }
        }
    }

    @Override
    public synchronized ArrowBatchSource open(int[] projection) throws Exception {
        if (closed.get()) {
            throw new IllegalStateException("doc-values source factory is closed");
        }
        List<InputColumn> projected = new ArrayList<>(projection.length);
        for (int index : projection) {
            if (index < 0 || index >= columns.size()) {
                throw new IllegalArgumentException("projection index [" + index + "] outside input schema of size " + columns.size());
            }
            projected.add(columns.get(index));
        }
        searcher.getIndexReader().incRef();
        boolean success = false;
        try {
            DocValuesBatchSource source = new DocValuesBatchSource(
                searcher,
                weight,
                projected,
                allocator,
                task,
                directBatches,
                fallbackBatches,
                batches,
                rows,
                nullValues
            );
            success = true;
            return source;
        } finally {
            if (success == false) {
                searcher.getIndexReader().decRef();
            }
        }
    }

    @Override
    public Map<String, Long> metrics() {
        return Map.of(
            "direct_batches",
            directBatches.sum(),
            "fallback_batches",
            fallbackBatches.sum(),
            "batches",
            batches.sum(),
            "rows",
            rows.sum(),
            "null_values",
            nullValues.sum()
        );
    }

    @Override
    public synchronized void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                searcher.getIndexReader().decRef();
            } catch (java.io.IOException e) {
                LOGGER.warn("failed to release doc-values source factory reader", e);
            }
        }
    }
}
