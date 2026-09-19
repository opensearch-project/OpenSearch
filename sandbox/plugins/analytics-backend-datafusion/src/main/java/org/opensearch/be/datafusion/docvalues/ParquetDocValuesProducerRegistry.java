/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.lucene.index.IndexReader;
import org.opensearch.common.CheckedSupplier;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Node-level cache of one segment-lifetime {@link ParquetDocValuesProducer} per segment core.
 *
 * <p>Keyed on the leaf's core cache key, so every request over the same segment core shares a single
 * producer rather than rebuilding it - file resolve, metadata read, format-version gate - per
 * request. The producer is created eagerly when a leaf is first wrapped and closed by the core's
 * closed-listener, so the producer and its file-level resources die with the segment core, not with
 * any one request.
 */
final class ParquetDocValuesProducerRegistry {

    private static final Map<IndexReader.CacheKey, ParquetDocValuesProducer> PRODUCERS = new ConcurrentHashMap<>();

    private ParquetDocValuesProducerRegistry() {}

    /**
     * The producer for the segment core identified by {@code coreHelper}, creating it on first use
     * via {@code factory} and registering a closed-listener that closes it when Lucene drops the
     * core.
     */
    static ParquetDocValuesProducer getOrCreate(
        IndexReader.CacheHelper coreHelper,
        CheckedSupplier<ParquetDocValuesProducer, IOException> factory
    ) throws IOException {
        IndexReader.CacheKey key = coreHelper.getKey();
        ParquetDocValuesProducer existing = PRODUCERS.get(key);
        if (existing != null) {
            return existing;
        }
        synchronized (PRODUCERS) {
            existing = PRODUCERS.get(key);
            if (existing != null) {
                return existing;
            }
            ParquetDocValuesProducer created = factory.get();
            PRODUCERS.put(key, created);
            // Registered once, in the create branch only, so a core carries exactly one listener no
            // matter how many requests wrap its leaf.
            coreHelper.addClosedListener(ParquetDocValuesProducerRegistry::onCoreClosed);
            return created;
        }
    }

    /** Closes and drops the producer bound to a segment core when Lucene drops the core. */
    private static void onCoreClosed(IndexReader.CacheKey key) throws IOException {
        ParquetDocValuesProducer removed = PRODUCERS.remove(key);
        if (removed != null) {
            removed.close();
        }
    }

    /** Live producer count (tests). */
    static int size() {
        return PRODUCERS.size();
    }
}
