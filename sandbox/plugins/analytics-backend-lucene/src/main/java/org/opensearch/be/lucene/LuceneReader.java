/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Map;
import java.util.Objects;

/**
 * Bundles a Lucene {@link DirectoryReader} with the {@code writer_generation → segment name} map,
 * plus one shared {@link IndexSearcher} per reader (see {@link #searcher}). Consumers must use that
 * searcher rather than building their own: distinct {@code IndexSearcher}s over the same reader,
 * both wired to the shared query cache, trip Lucene's "top-reader used to create Weight is not the
 * same as the current reader's top-reader" assertion — fatal across the FFM boundary.
 */
@ExperimentalApi
public final class LuceneReader {

    private final DirectoryReader directoryReader;
    private final Map<Long, String> generationToSegmentName;

    /** Lazily built on first {@link #searcher} call; one shared instance per reader thereafter. */
    private volatile IndexSearcher searcher;

    public LuceneReader(DirectoryReader directoryReader, Map<Long, String> generationToSegmentName) {
        this.directoryReader = Objects.requireNonNull(directoryReader, "directoryReader must not be null");
        this.generationToSegmentName = Map.copyOf(generationToSegmentName);
    }

    public DirectoryReader directoryReader() {
        return directoryReader;
    }

    public Map<Long, String> generationToSegmentName() {
        return generationToSegmentName;
    }

    /**
     * Returns the shared searcher over this reader, building it on first use with the given cache
     * and policy. Later calls return the same instance and ignore their arguments (every caller on
     * a shard passes the same per-shard cache/policy).
     */
    public IndexSearcher searcher(QueryCache queryCache, QueryCachingPolicy queryCachingPolicy) {
        IndexSearcher local = searcher;
        if (local != null) {
            return local;
        }
        synchronized (this) {
            if (searcher == null) {
                IndexSearcher built = new IndexSearcher(directoryReader);
                if (queryCache != null) {
                    built.setQueryCache(queryCache);
                }
                if (queryCachingPolicy != null) {
                    built.setQueryCachingPolicy(queryCachingPolicy);
                }
                searcher = built;
            }
            return searcher;
        }
    }
}
