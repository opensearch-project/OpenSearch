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
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.storage.slowlogs.TieredStorageQueryMetricService;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Search operation listener that prefetches stored fields before the fetch phase.
 * This improves search performance on warm data by pre-loading stored field blocks
 * from remote storage before they are needed.
 *
 * @opensearch.experimental
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
            TieredStorageQueryMetricService.getInstance().recordStoredFieldsPrefetch(true);
        }
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
                        // disable prefetch on stored fields for this case
                        return;
                    }
                    currentReader = innerLeafReader.storedFields();
                }
                assert currentReaderContext != null;
                log.debug(
                    "Prefetching stored fields for index shard: "
                        + context.indexShard().shardId()
                        + ", docId: "
                        + docId
                        + " readerIndex: "
                        + readerIndex
                );

                // nested docs logic
                final int subDocId = docId - currentReaderContext.docBase;
                final int rootDocId = findRootDocumentIfNested(context, currentReaderContext, subDocId);
                if (rootDocId != -1) {
                    currentReader.prefetch(rootDocId);
                }
                currentReader.prefetch(subDocId);
            } catch (Exception e) {
                throw ExceptionsHelper.convertToOpenSearchException(e);
            }
        }
    }

    private int findRootDocumentIfNested(SearchContext context, LeafReaderContext subReaderContext, int subDocId) throws IOException {
        if (context.mapperService().hasNested()) {
            BitSet bits = context.bitsetFilterCache().getBitSetProducer(Queries.newNonNestedFilter()).getBitSet(subReaderContext);
            if (!bits.get(subDocId)) {
                return bits.nextSetBit(subDocId);
            }
        }
        return -1;
    }

    private boolean checkIfStoredFieldsPrefetchEnabled() {
        return tieredStoragePrefetchSettingsSupplier.get().isStoredFieldsPrefetchEnabled();
    }
}
