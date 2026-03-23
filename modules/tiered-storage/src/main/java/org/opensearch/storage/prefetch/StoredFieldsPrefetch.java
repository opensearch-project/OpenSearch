/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.prefetch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.util.BitSet;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Search operation listener that prefetches stored fields for tiered storage indices.
 */
public class StoredFieldsPrefetch implements SearchOperationListener {

    private static final Logger log = LogManager.getLogger(StoredFieldsPrefetch.class);
    private final Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier;

    /**
     * Creates a new StoredFieldsPrefetch instance.
     * @param tieredStoragePrefetchSettingsSupplier supplier for prefetch settings
     */
    public StoredFieldsPrefetch(Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier) {
        this.tieredStoragePrefetchSettingsSupplier = tieredStoragePrefetchSettingsSupplier;
    }

    @Override
    public void onPreFetchPhase(SearchContext searchContext) {
        // Based on cluster settings
        if (checkIfStoredFieldsPrefetchEnabled()) {
            executePrefetch(searchContext);
            // TieredStorageQueryMetricService.getInstance().recordStoredFieldsPrefetch(true);
        }
        // else {
        // TieredStorageQueryMetricService.getInstance().recordStoredFieldsPrefetch(false);
        // }
    }

    private void executePrefetch(SearchContext context) {
        int currentReaderIndex = -1;
        LeafReaderContext currentReaderContext = null;
        StoredFields currentReader = null;
        log.debug("Stored Field Execute prefetch was triggered: {}", context.docIdsToLoadSize());
        for (int index = 0; index < context.docIdsToLoadSize(); index++) {
            int docId = context.docIdsToLoad()[context.docIdsToLoadFrom() + index];
            try {
                int readerIndex = ReaderUtil.subIndex(docId, context.searcher().getIndexReader().leaves());
                if (currentReaderIndex != readerIndex) {
                    currentReaderContext = context.searcher().getIndexReader().leaves().get(readerIndex);
                    currentReaderIndex = readerIndex;

                    // Unwrap the reader here
                    LeafReader innerLeafReader = currentReaderContext.reader();
                    while (innerLeafReader instanceof FilterLeafReader) {
                        innerLeafReader = ((FilterLeafReader) innerLeafReader).getDelegate();
                    }
                    // never be the case, just sanity check
                    if (!(innerLeafReader instanceof SegmentReader)) {
                        // disable prefetch on stored fields for this segment
                        log.warn("Unexpected reader type [{}], skipping stored fields prefetch", innerLeafReader.getClass().getName());
                        currentReader = null;
                        continue;
                    }
                    currentReader = innerLeafReader.storedFields();
                }
                assert currentReaderContext != null;
                if (currentReader == null) {
                    continue;
                }
                log.debug(
                    "Prefetching stored fields for index shard: {}, docId: {}, readerIndex: {}",
                    context.indexShard().shardId(),
                    docId,
                    readerIndex
                );

                // nested docs logic
                final int subDocId = docId - currentReaderContext.docBase;
                final int rootDocId = findRootDocumentIfNested(context, currentReaderContext, subDocId);
                if (rootDocId != -1) {
                    currentReader.prefetch(rootDocId);
                }
                currentReader.prefetch(subDocId);
            } catch (Exception e) {
                log.warn("Failed to prefetch stored fields for docId: " + docId, e);
            }
        }
    }

    private int findRootDocumentIfNested(SearchContext context, LeafReaderContext subReaderContext, int subDocId) throws IOException {
        if (context.mapperService().hasNested()) {
            BitSet bits = context.bitsetFilterCache().getBitSetProducer(Queries.newNonNestedFilter()).getBitSet(subReaderContext);
            if (bits != null && !bits.get(subDocId)) {
                return bits.nextSetBit(subDocId);
            }
        }
        return -1;
    }

    private boolean checkIfStoredFieldsPrefetchEnabled() {
        TieredStoragePrefetchSettings settings = tieredStoragePrefetchSettingsSupplier.get();
        return settings != null && settings.isStoredFieldsPrefetchEnabled();
    }
}
