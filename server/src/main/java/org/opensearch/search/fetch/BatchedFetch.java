/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.fetch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.common.Nullable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.lucene.index.BatchPlan;
import org.opensearch.common.lucene.index.BatchRequest;
import org.opensearch.common.lucene.index.BatchedDocumentValues;
import org.opensearch.common.lucene.index.BatchedDocumentValuesProvider;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.profile.Timer;
import org.opensearch.search.profile.fetch.FetchProfileBreakdown;
import org.opensearch.search.profile.fetch.FetchTimingType;

import java.io.IOException;
import java.util.Arrays;

/**
 * Reads the hits of one fetch in one batch per segment, through each segment's {@link BatchedDocumentValuesProvider},
 * instead of one document at a time. A segment whose reader has no provider, or whose provider declines, is read one
 * document at a time, as before. A batch that fails to open fails the fetch.
 *
 * <p>The fetch walks its hits in doc id order, so at most one segment's batch is open at a time: moving to a segment
 * closes the previous segment's batch, and {@link #close} closes the last one.
 *
 * <p>Not thread-safe: one instance belongs to one execution of the fetch phase, on one thread.
 *
 * @opensearch.internal
 */
final class BatchedFetch implements Releasable {

    private static final Logger logger = LogManager.getLogger(BatchedFetch.class);

    private final SearchContext context;
    private boolean enabled;
    @Nullable
    private Timer timer;
    private BatchPlan plan;
    private BatchedDocumentValues batch;

    BatchedFetch(SearchContext context) {
        this.context = context;
    }

    /**
     * Decides whether this fetch batches. The values a batch serves are the ones source derivation reads, so the
     * fetch must load {@code _source}. A mapping with nested fields is left alone: a nested hit reads its root
     * document, which need not be among the hits.
     *
     * @param loadsSource whether the fetch loads {@code _source}
     * @param breakdown   the fetch's profile, if profiled; a batch's read is timed as loading stored fields
     */
    void start(boolean loadsSource, @Nullable FetchProfileBreakdown breakdown) {
        enabled = loadsSource && context.mapperService().hasNested() == false;
        timer = breakdown == null ? null : breakdown.getTimer(FetchTimingType.LOAD_STORED_FIELDS);
    }

    /**
     * Moves to the segment that holds {@code docs[from]}: closes the previous segment's batch and, when the segment's
     * reader can batch, opens one over the segment's hits. If opening fails, the exception fails the fetch and
     * {@link #close} releases the plan.
     *
     * @param leaf the segment
     * @param docs the fetch's hits, in doc id order
     * @param from the index in {@code docs} of the segment's first hit
     */
    void onSegment(LeafReaderContext leaf, FetchPhase.DocIdToIndex[] docs, int from) throws IOException {
        closeCurrent();
        if (enabled == false) {
            return;
        }
        BatchedDocumentValuesProvider provider = findProvider(leaf.reader());
        if (provider == null) {
            return;
        }
        // Only a segment that can batch is timed, so profiles of other indices are unchanged.
        if (timer != null) {
            timer.start();
        }
        try {
            plan = provider.plan(new BatchRequest(leafDocs(leaf, docs, from), context::isCancelled));
            if (plan != null) {
                batch = provider.open(plan);
            }
        } finally {
            if (timer != null) {
                timer.stop();
            }
        }
    }

    /** The leaf-relative doc ids of the segment's hits, ascending and without repeats. */
    static int[] leafDocs(LeafReaderContext leaf, FetchPhase.DocIdToIndex[] docs, int from) {
        int end = leaf.docBase + leaf.reader().maxDoc();
        int to = from;
        while (to < docs.length && docs[to].docId < end) {
            to++;
        }
        int[] leafDocs = new int[to - from];
        int count = 0;
        for (int i = from; i < to; i++) {
            int doc = docs[i].docId - leaf.docBase;
            // The same document can be among the hits twice; a batch reads it once.
            if (count == 0 || leafDocs[count - 1] != doc) {
                leafDocs[count++] = doc;
            }
        }
        return count == leafDocs.length ? leafDocs : Arrays.copyOf(leafDocs, count);
    }

    /** Walks through filter wrappers to the reader that can batch, if any. */
    @Nullable
    static BatchedDocumentValuesProvider findProvider(LeafReader reader) {
        LeafReader current = reader;
        while (current instanceof BatchedDocumentValuesProvider == false && current instanceof FilterLeafReader filter) {
            current = filter.getDelegate();
        }
        return current instanceof BatchedDocumentValuesProvider provider ? provider : null;
    }

    private void closeCurrent() {
        BatchedDocumentValues openBatch = batch;
        BatchPlan openPlan = plan;
        batch = null;
        plan = null;
        try {
            Releasables.close(openBatch, openPlan);
        } catch (RuntimeException e) {
            // The hits were already read; failing to free a batch must not fail the fetch.
            logger.warn("failed to close a batched fetch read", e);
        }
    }

    /** Closes the open batch, if any. Safe to call more than once. */
    @Override
    public void close() {
        closeCurrent();
    }
}
