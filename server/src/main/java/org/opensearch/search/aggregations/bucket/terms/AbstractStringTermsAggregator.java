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

package org.opensearch.search.aggregations.bucket.terms;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.BytesRef;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.ShardResultConvertor;
import org.opensearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregator;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static org.opensearch.search.aggregations.InternalOrder.isKeyOrder;

/**
 * Base Aggregator to collect all docs that contain significant terms
 *
 * @opensearch.internal
 */
abstract class AbstractStringTermsAggregator extends TermsAggregator implements ShardResultConvertor {

    protected final boolean showTermDocCountError;

    AbstractStringTermsAggregator(
        String name,
        AggregatorFactories factories,
        SearchContext context,
        Aggregator parent,
        BucketOrder order,
        DocValueFormat format,
        BucketCountThresholds bucketCountThresholds,
        SubAggCollectionMode subAggCollectMode,
        boolean showTermDocCountError,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, bucketCountThresholds, order, format, subAggCollectMode, metadata);
        this.showTermDocCountError = showTermDocCountError;
    }

    protected StringTerms buildEmptyTermsAggregation() {
        return new StringTerms(
            name,
            order,
            order,
            metadata(),
            format,
            bucketCountThresholds.getShardSize(),
            showTermDocCountError,
            0,
            emptyList(),
            0,
            bucketCountThresholds
        );
    }

    protected SignificantStringTerms buildEmptySignificantTermsAggregation(long subsetSize, SignificanceHeuristic significanceHeuristic) {
        // We need to account for the significance of a miss in our global stats - provide corpus size as context
        ContextIndexSearcher searcher = context.searcher();
        IndexReader topReader = searcher.getIndexReader();
        int supersetSize = topReader.numDocs();
        return new SignificantStringTerms(
            name,
            metadata(),
            format,
            subsetSize,
            supersetSize,
            significanceHeuristic,
            emptyList(),
            bucketCountThresholds
        );
    }

    @Override
    public InternalAggregation convertRow(Map<String, Object[]> shardResult, int row, SearchContext searchContext) {
        String termKey = (String) searchContext.convertToComparable(shardResult.get(name)[row]);
        long docCount = 1;

        List<InternalAggregation> subAggs = new ArrayList<>();
        for (Aggregator aggregator : subAggregators) {
            if (aggregator instanceof ShardResultConvertor convertor) {
                InternalAggregation subAgg = convertor.convertRow(shardResult, row, searchContext);
                if (aggregator instanceof ValueCountAggregator) {
                    docCount = ((InternalValueCount) subAgg).getValue();
                }
                subAggs.add(subAgg);
            }
        }

        BucketOrder reduceOrder = order;
        if (isKeyOrder(order) == false) {
            reduceOrder = InternalOrder.key(true);
        }
        StringTerms.Bucket bucket = new StringTerms.Bucket(
            new BytesRef(termKey),
            docCount,
            InternalAggregations.from(subAggs),
            showTermDocCountError,
            0,
            format
        );
        return new StringTerms(
            name,
            reduceOrder,
            order,
            null,
            format,
            bucketCountThresholds.getShardSize(),
            showTermDocCountError,
            0,
            List.of(bucket),
            0,
            bucketCountThresholds
        );
    }

}
