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

package org.opensearch.search.aggregations.bucket.nested;

import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.core.ParseField;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.bucket.BucketsAggregator;
import org.opensearch.search.aggregations.bucket.SingleBucketAggregator;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.index.mapper.ObjectMapper.Nested.isParent;

/**
 * Aggregate all docs that match a nested path
 *
 * @opensearch.internal
 */
public class NestedAggregator extends BucketsAggregator implements SingleBucketAggregator {

    static final ParseField PATH_FIELD = new ParseField("path");

    private final BitSetProducer parentFilter;
    private final Query childFilter;
    private final boolean collectsFromSingleBucket;

    private BufferingNestedLeafBucketCollector bufferingNestedLeafBucketCollector;

    NestedAggregator(
        String name,
        AggregatorFactories factories,
        ObjectMapper parentObjectMapper,
        ObjectMapper childObjectMapper,
        SearchContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, cardinality, metadata);

        Query parentFilter = isParent(parentObjectMapper, childObjectMapper, context.mapperService())
            ? parentObjectMapper.nestedTypeFilter()
            : Queries.newNonNestedFilter();
        this.parentFilter = context.bitsetFilterCache().getBitSetProducer(parentFilter);
        this.childFilter = childObjectMapper.nestedTypeFilter();
        this.collectsFromSingleBucket = cardinality.map(estimate -> estimate < 2);
    }

    @Override
    public LeafBucketCollector getLeafCollector(final LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(ctx);
        IndexSearcher searcher = new IndexSearcher(topLevelContext);
        searcher.setQueryCache(null);
        Weight weight = searcher.createWeight(searcher.rewrite(childFilter), ScoreMode.COMPLETE_NO_SCORES, 1f);
        Scorer childDocsScorer = weight.scorer(ctx);

        final BitSet parentDocs = parentFilter.getBitSet(ctx);
        final DocIdSetIterator childDocs = childDocsScorer != null ? childDocsScorer.iterator() : null;
        if (collectsFromSingleBucket) {
            return new LeafBucketCollectorBase(sub, null) {
                @Override
                public void collect(int parentAggDoc, long bucket) throws IOException {
                    // parentAggDoc can be 0 when aggregation:
                    if (parentDocs == null || childDocs == null) {
                        return;
                    }

                    Tuple<Integer, Integer> res = getParentAndChildId(parentDocs, childDocs, parentAggDoc);
                    int currentParentDoc = res.v1();
                    int childDocId = res.v2();

                    for (; childDocId < currentParentDoc; childDocId = childDocs.nextDoc()) {
                        collectBucket(sub, childDocId, bucket);
                    }
                }
            };
        } else {
            return bufferingNestedLeafBucketCollector = new BufferingNestedLeafBucketCollector(sub, parentDocs, childDocs);
        }
    }

    /**
     * In one case, it's talking about the parent doc (from the Lucene block-join standpoint),
     * while in the other case, it's talking about a child doc ID (from the block-join standpoint)
     * from the parent aggregation, where we're trying to aggregate over a sibling of that child.
     * So, we need to map from that document to its parent, then join to the appropriate sibling.
     *
     * @param parentAggDoc the parent aggregation's current doc
     *                    (which may or may not be a block-level parent doc)
     * @return a tuple consisting of the current block-level parent doc (the parent of the
     *         parameter doc), and the next matching child doc (hopefully under this parent)
     *         for the aggregation (according to the child doc iterator).
     */
    static Tuple<Integer, Integer> getParentAndChildId(BitSet parentDocs, DocIdSetIterator childDocs, int parentAggDoc) throws IOException {
        int currentParentAggDoc;
        int prevParentDoc = parentDocs.prevSetBit(parentAggDoc);
        if (prevParentDoc == -1) {
            currentParentAggDoc = parentDocs.nextSetBit(0);
        } else if (prevParentDoc == parentAggDoc) {
            // parentAggDoc is the parent of that child, and is belongs to parentDocs
            currentParentAggDoc = parentAggDoc;
            if (currentParentAggDoc == 0) {
                prevParentDoc = -1;
            } else {
                prevParentDoc = parentDocs.prevSetBit(currentParentAggDoc - 1);
            }
        } else {
            // parentAggDoc is the sibling of that child, and it means the block-join parent
            currentParentAggDoc = parentDocs.nextSetBit(prevParentDoc + 1);
        }

        int childDocId = childDocs.docID();
        if (childDocId <= prevParentDoc) {
            childDocId = childDocs.advance(prevParentDoc + 1);
        }
        return Tuple.tuple(currentParentAggDoc, childDocId);
    }

    @Override
    protected void preGetSubLeafCollectors(LeafReaderContext ctx) throws IOException {
        super.preGetSubLeafCollectors(ctx);
        processBufferedDocs();
    }

    @Override
    protected void doPostCollection() throws IOException {
        processBufferedDocs();
    }

    private void processBufferedDocs() throws IOException {
        if (bufferingNestedLeafBucketCollector != null) {
            bufferingNestedLeafBucketCollector.processBufferedChildBuckets();
        }
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForSingleBucket(
            owningBucketOrds,
            (owningBucketOrd, subAggregationResults) -> new InternalNested(
                name,
                bucketDocCount(owningBucketOrd),
                subAggregationResults,
                metadata()
            )
        );
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalNested(name, 0, buildEmptySubAggregations(), metadata());
    }

    class BufferingNestedLeafBucketCollector extends LeafBucketCollectorBase {

        final BitSet parentDocs;
        final LeafBucketCollector sub;
        final DocIdSetIterator childDocs;
        final List<Long> bucketBuffer = new ArrayList<>();

        Scorable scorer;
        int currentParentDoc = -1;
        final CachedScorable cachedScorer = new CachedScorable();

        BufferingNestedLeafBucketCollector(LeafBucketCollector sub, BitSet parentDocs, DocIdSetIterator childDocs) {
            super(sub, null);
            this.sub = sub;
            this.parentDocs = parentDocs;
            this.childDocs = childDocs;
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            this.scorer = scorer;
            super.setScorer(cachedScorer);
        }

        @Override
        public void collect(int parentDoc, long bucket) throws IOException {
            // parentAggDoc can be 0 when aggregation:
            if (parentDocs == null || childDocs == null) {
                return;
            }

            if (currentParentDoc != parentDoc) {
                processBufferedChildBuckets();
                if (scoreMode().needsScores()) {
                    // cache the score of the current parent
                    cachedScorer.score = scorer.score();
                }
                currentParentDoc = parentDoc;

            }
            bucketBuffer.add(bucket);
        }

        void processBufferedChildBuckets() throws IOException {
            if (bucketBuffer.isEmpty()) {
                return;
            }

            Tuple<Integer, Integer> res = getParentAndChildId(parentDocs, childDocs, currentParentDoc);
            int currentParentDoc = res.v1();
            int childDocId = res.v2();

            for (; childDocId < currentParentDoc; childDocId = childDocs.nextDoc()) {
                cachedScorer.doc = childDocId;
                for (var bucket : bucketBuffer) {
                    collectBucket(sub, childDocId, bucket);
                }
            }
            bucketBuffer.clear();
        }
    }

    /**
     * A cached scorable doc
     *
     * @opensearch.internal
     */
    private static class CachedScorable extends Scorable {
        int doc;
        float score;

        @Override
        public final float score() {
            return score;
        }

        @Override
        public int docID() {
            return doc;
        }

    }

}
