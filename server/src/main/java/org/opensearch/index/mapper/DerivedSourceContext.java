/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;
import org.opensearch.common.lucene.search.Queries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Request-local state for reconstructing derived source across root and nested Lucene documents.
 *
 * @opensearch.internal
 */
class DerivedSourceContext {
    private final LeafReader leafReader;
    private final LeafReaderContext leafReaderContext;
    private final Function<Query, BitSetProducer> bitSetProducer;
    private final boolean hasNested;
    private int currentDocId;
    private int currentPreviousParentDocId;

    DerivedSourceContext(
        LeafReaderContext leafReaderContext,
        Function<Query, BitSetProducer> bitSetProducer,
        boolean hasNested,
        int rootDocId
    ) throws IOException {
        this.leafReader = leafReaderContext.reader();
        this.leafReaderContext = leafReaderContext;
        this.bitSetProducer = bitSetProducer;
        this.hasNested = hasNested;
        initRootDoc(rootDocId);
    }

    LeafReader leafReader() {
        return leafReader;
    }

    int currentDocId() {
        return currentDocId;
    }

    int currentPreviousParentDocId() {
        return currentPreviousParentDocId;
    }

    void switchDoc(int docId, int previousParentDocId) {
        currentDocId = docId;
        currentPreviousParentDocId = previousParentDocId;
    }

    List<Integer> nestedDocs(ObjectMapper nestedMapper) throws IOException {
        if (hasNested == false) {
            return Collections.emptyList();
        }
        BitSet childDocs = bitSetForNested(nestedMapper);
        if (childDocs == null) {
            return Collections.emptyList();
        }

        List<Integer> nestedDocs = new ArrayList<>();
        for (int childDoc = childDocs.nextSetBit(currentPreviousParentDocId + 1); childDoc != DocIdSetIterator.NO_MORE_DOCS
            && childDoc < currentDocId; childDoc = childDocs.nextSetBit(childDoc + 1)) {
            nestedDocs.add(childDoc);
        }
        return nestedDocs;
    }

    private void initRootDoc(int rootDocId) throws IOException {
        currentDocId = rootDocId;
        if (hasNested == false) {
            currentPreviousParentDocId = -1;
            return;
        }
        BitSet rootDocs = bitSetForRoot();
        if (rootDocs == null || rootDocs.get(rootDocId) == false) {
            currentPreviousParentDocId = -1;
            return;
        }
        currentPreviousParentDocId = rootDocId == 0 ? -1 : rootDocs.prevSetBit(rootDocId - 1);
    }

    private BitSet bitSetForRoot() throws IOException {
        return bitSetForQuery(Queries.newNonNestedFilter());
    }

    private BitSet bitSetForNested(ObjectMapper nestedMapper) throws IOException {
        return bitSetForQuery(nestedMapper.nestedTypeFilter());
    }

    private BitSet bitSetForQuery(Query query) throws IOException {
        BitSetProducer producer = bitSetProducer.apply(query);
        return producer == null ? null : producer.getBitSet(leafReaderContext);
    }
}
