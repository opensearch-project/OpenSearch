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

package org.opensearch.percolator;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.FetchSubPhaseProcessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.opensearch.percolator.PercolatorHighlightSubFetchPhase.locatePercolatorQuery;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Adds a special field to a percolator query hit to indicate which documents matched with the percolator query.
 * This is useful when multiple documents are being percolated in a single request.
 */
final class PercolatorMatchedSlotSubFetchPhase implements FetchSubPhase {

    static final String FIELD_NAME_PREFIX = "_percolator_document_slot";

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) throws IOException {

        List<PercolateContext> percolateContexts = new ArrayList<>();
        List<PercolateQuery> percolateQueries = locatePercolatorQuery(fetchContext.query());
        boolean singlePercolateQuery = percolateQueries.size() == 1;
        for (PercolateQuery pq : percolateQueries) {
            percolateContexts.add(new PercolateContext(pq, singlePercolateQuery));
        }
        if (percolateContexts.isEmpty()) {
            return null;
        }

        return new FetchSubPhaseProcessor() {

            LeafReaderContext ctx;

            @Override
            public void setNextReader(LeafReaderContext readerContext) {
                this.ctx = readerContext;
            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                for (PercolateContext pc : percolateContexts) {
                    String fieldName = pc.fieldName();
                    Query query = pc.percolateQuery.getQueryStore().getQueries(ctx).apply(hitContext.docId());
                    if (query == null) {
                        // This is not a document with a percolator field.
                        continue;
                    }
                    query = pc.filterNestedDocs(query);
                    IndexSearcher percolatorIndexSearcher = pc.percolateQuery.getPercolatorIndexSearcher();
                    int memoryIndexMaxDoc = percolatorIndexSearcher.getIndexReader().maxDoc();
                    TopDocs topDocs = percolatorIndexSearcher.search(query, memoryIndexMaxDoc, new Sort(SortField.FIELD_DOC));
                    if (topDocs.totalHits.value() == 0) {
                        // This hit didn't match with a percolate query,
                        // likely to happen when percolating multiple documents
                        continue;
                    }

                    IntStream slots = convertTopDocsToSlots(topDocs, pc.rootDocsBySlot);
                    // _percolator_document_slot fields are document fields and should be under "fields" section in a hit
                    hitContext.hit().setDocumentField(fieldName, new DocumentField(fieldName, slots.boxed().collect(Collectors.toList())));
                }
            }
        };
    }

    static class PercolateContext {
        final PercolateQuery percolateQuery;
        final boolean singlePercolateQuery;
        final int[] rootDocsBySlot;

        PercolateContext(PercolateQuery pq, boolean singlePercolateQuery) throws IOException {
            this.percolateQuery = pq;
            this.singlePercolateQuery = singlePercolateQuery;
            IndexSearcher percolatorIndexSearcher = percolateQuery.getPercolatorIndexSearcher();
            Query nonNestedFilter = percolatorIndexSearcher.rewrite(Queries.newNonNestedFilter());
            Weight weight = percolatorIndexSearcher.createWeight(nonNestedFilter, ScoreMode.COMPLETE_NO_SCORES, 1f);
            Scorer s = weight.scorer(percolatorIndexSearcher.getIndexReader().leaves().get(0));
            int memoryIndexMaxDoc = percolatorIndexSearcher.getIndexReader().maxDoc();
            BitSet rootDocs = BitSet.of(s.iterator(), memoryIndexMaxDoc);
            boolean hasNestedDocs = rootDocs.cardinality() != percolatorIndexSearcher.getIndexReader().numDocs();
            if (hasNestedDocs) {
                this.rootDocsBySlot = buildRootDocsSlots(rootDocs);
            } else {
                this.rootDocsBySlot = null;
            }
        }

        String fieldName() {
            return singlePercolateQuery ? FIELD_NAME_PREFIX : FIELD_NAME_PREFIX + "_" + percolateQuery.getName();
        }

        Query filterNestedDocs(Query in) {
            if (rootDocsBySlot != null) {
                // Ensures that we filter out nested documents
                return new BooleanQuery.Builder().add(in, BooleanClause.Occur.MUST)
                    .add(Queries.newNonNestedFilter(), BooleanClause.Occur.FILTER)
                    .build();
            }
            return in;
        }
    }

    static IntStream convertTopDocsToSlots(TopDocs topDocs, int[] rootDocsBySlot) {
        IntStream stream = Arrays.stream(topDocs.scoreDocs).mapToInt(scoreDoc -> scoreDoc.doc);
        if (rootDocsBySlot != null) {
            stream = stream.map(docId -> Arrays.binarySearch(rootDocsBySlot, docId));
        }
        return stream;
    }

    static int[] buildRootDocsSlots(BitSet rootDocs) {
        int slot = 0;
        int[] rootDocsBySlot = new int[rootDocs.cardinality()];
        BitSetIterator iterator = new BitSetIterator(rootDocs, 0);
        for (int rootDocId = iterator.nextDoc(); rootDocId != NO_MORE_DOCS; rootDocId = iterator.nextDoc()) {
            rootDocsBySlot[slot++] = rootDocId;
        }
        return rootDocsBySlot;
    }
}
