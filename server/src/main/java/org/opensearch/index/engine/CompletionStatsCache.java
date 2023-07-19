/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.suggest.document.CompletionTerms;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.FieldMemoryStats;
import org.opensearch.common.Nullable;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.util.CollectionUtils;
import org.opensearch.search.suggest.completion.CompletionStats;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Cache to store engine completion stats
 *
 * @opensearch.internal
 */
class CompletionStatsCache implements ReferenceManager.RefreshListener {

    private final Supplier<Engine.Searcher> searcherSupplier;

    /**
     * Contains a future (i.e. non-null) if another thread is already computing stats, in which case wait for this computation to
     * complete. Contains null otherwise, in which case compute the stats ourselves and save them here for other threads to use.
     * Futures are eventually completed with stats that include all fields, requiring further filtering (see
     * {@link CompletionStatsCache#filterCompletionStatsByFieldName}).
     */
    @Nullable
    private PlainActionFuture<CompletionStats> completionStatsFuture;

    /**
     * Protects accesses to {@code completionStatsFuture} since we can't use {@link java.util.concurrent.atomic.AtomicReference} in JDK8.
     */
    private final Object completionStatsFutureMutex = new Object();

    CompletionStatsCache(Supplier<Engine.Searcher> searcherSupplier) {
        this.searcherSupplier = searcherSupplier;
    }

    CompletionStats get(String... fieldNamePatterns) {
        final PlainActionFuture<CompletionStats> newFuture = new PlainActionFuture<>();

        // final PlainActionFuture<CompletionStats> oldFuture = completionStatsFutureRef.compareAndExchange(null, newFuture);
        // except JDK8 doesn't have compareAndExchange so we emulate it:
        final PlainActionFuture<CompletionStats> oldFuture;
        synchronized (completionStatsFutureMutex) {
            if (completionStatsFuture == null) {
                completionStatsFuture = newFuture;
                oldFuture = null;
            } else {
                oldFuture = completionStatsFuture;
            }
        }

        if (oldFuture != null) {
            // we lost the race, someone else is already computing stats, so we wait for that to finish
            return filterCompletionStatsByFieldName(fieldNamePatterns, oldFuture.actionGet());
        }

        // we won the race, nobody else is already computing stats, so it's up to us
        ActionListener.completeWith(newFuture, () -> {
            long sizeInBytes = 0;
            final Map<String, Long> completionFields = new HashMap<>();

            try (Engine.Searcher currentSearcher = searcherSupplier.get()) {
                for (LeafReaderContext atomicReaderContext : currentSearcher.getIndexReader().leaves()) {
                    LeafReader atomicReader = atomicReaderContext.reader();
                    for (FieldInfo info : atomicReader.getFieldInfos()) {
                        Terms terms = atomicReader.terms(info.name);
                        if (terms instanceof CompletionTerms) {
                            // TODO: currently we load up the suggester for reporting its size
                            final long fstSize = ((CompletionTerms) terms).suggester().ramBytesUsed();
                            completionFields.merge(info.name, fstSize, Long::sum);
                            sizeInBytes += fstSize;
                        }
                    }
                }
            }

            return new CompletionStats(sizeInBytes, new FieldMemoryStats(completionFields));
        });

        boolean success = false;
        final CompletionStats completionStats;
        try {
            completionStats = newFuture.actionGet();
            success = true;
        } finally {
            if (success == false) {
                // invalidate the cache (if not already invalidated) so that future calls will retry

                // completionStatsFutureRef.compareAndSet(newFuture, null); except we're not using AtomicReference in JDK8
                synchronized (completionStatsFutureMutex) {
                    if (completionStatsFuture == newFuture) {
                        completionStatsFuture = null;
                    }
                }
            }
        }

        return filterCompletionStatsByFieldName(fieldNamePatterns, completionStats);
    }

    private static CompletionStats filterCompletionStatsByFieldName(String[] fieldNamePatterns, CompletionStats fullCompletionStats) {
        final FieldMemoryStats fieldMemoryStats;
        if (CollectionUtils.isEmpty(fieldNamePatterns) == false) {
            final Map<String, Long> completionFields = new HashMap<>(fieldNamePatterns.length);
            for (var fieldCursor : fullCompletionStats.getFields()) {
                if (Regex.simpleMatch(fieldNamePatterns, fieldCursor.getKey())) {
                    completionFields.merge(fieldCursor.getKey(), fieldCursor.getValue(), Long::sum);
                }
            }
            fieldMemoryStats = new FieldMemoryStats(completionFields);
        } else {
            fieldMemoryStats = null;
        }
        return new CompletionStats(fullCompletionStats.getSizeInBytes(), fieldMemoryStats);
    }

    @Override
    public void beforeRefresh() {}

    @Override
    public void afterRefresh(boolean didRefresh) {
        if (didRefresh) {
            // completionStatsFutureRef.set(null); except we're not using AtomicReference in JDK8
            synchronized (completionStatsFutureMutex) {
                completionStatsFuture = null;
            }
        }
    }
}
