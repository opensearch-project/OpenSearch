/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.FieldInfos;
import org.opensearch.common.annotation.PublicApi;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Tracks the set of Lucene fields known to the shard's index, refreshed on each NRT reader
 * refresh cycle. Allows {@link DocumentParser} to enforce the dynamic-properties Lucene
 * field-count limit without threading a per-parse snapshot through the call-stack.
 *
 * <p>An instance is attached to {@link MapperService}. After each successful internal refresh,
 * an {@link org.apache.lucene.search.ReferenceManager.RefreshListener} registered by
 * {@code IndexShard} acquires an internal searcher, merges all segment-level {@link FieldInfos}
 * via {@link FieldInfos#getMergedFieldInfos}, and calls {@link #setFieldInfos(FieldInfos)} to
 * update this tracker.
 *
 * <p>Because the snapshot is taken at refresh time, fields added since the last refresh are not
 * yet visible. This makes the limit a <em>soft</em> limit — a small overshoot between refreshes
 * is acceptable.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public class LuceneFieldTracker {

    /**
     * Merged field infos from the most recent internal refresh.
     * Replaced atomically on each refresh; starts as {@link FieldInfos#EMPTY}.
     */
    private final AtomicReference<FieldInfos> fieldInfosRef = new AtomicReference<>(FieldInfos.EMPTY);

    /**
     * Updates the tracked field infos with the merged snapshot from the latest refresh.
     * Called by the {@code IndexShard} refresh listener after each successful internal refresh.
     */
    public void setFieldInfos(FieldInfos fieldInfos) {
        this.fieldInfosRef.set(fieldInfos);
    }

    /**
     * Returns the merged {@link FieldInfos} captured at the last refresh.
     * Returns {@link FieldInfos#EMPTY} if no refresh has occurred yet.
     */
    public FieldInfos getFieldInfos() {
        return fieldInfosRef.get();
    }
}
