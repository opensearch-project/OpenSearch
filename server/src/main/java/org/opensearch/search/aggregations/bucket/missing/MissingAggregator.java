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

package org.opensearch.search.aggregations.bucket.missing;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Weight;
import org.opensearch.index.fielddata.DocValueBits;
import org.opensearch.index.mapper.DocCountFieldMapper;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.bucket.BucketsAggregator;
import org.opensearch.search.aggregations.bucket.SingleBucketAggregator;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.lucene.index.SortedSetDocValues.NO_MORE_DOCS;

/**
 * Aggregate all docs that are missing a value.
 *
 * @opensearch.internal
 */
public class MissingAggregator extends BucketsAggregator implements SingleBucketAggregator {

    private Weight weight;
    private final ValuesSource valuesSource;
    protected final String fieldName;
    private final ValuesSourceConfig valuesSourceConfig;

    public MissingAggregator(
        String name,
        AggregatorFactories factories,
        ValuesSourceConfig valuesSourceConfig,
        SearchContext aggregationContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, aggregationContext, parent, cardinality, metadata);
        // TODO: Stop using nulls here
        this.valuesSource = valuesSourceConfig.hasValues() ? valuesSourceConfig.getValuesSource() : null;
        if (this.valuesSource != null) {
            this.fieldName = valuesSource.getIndexFieldName();
        } else {
            this.fieldName = null;
        }
        this.valuesSourceConfig = valuesSourceConfig;
    }

    public void setWeight(Weight weight) {
        this.weight = weight;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        final DocValueBits docsWithValue;
        if (valuesSource != null) {
            docsWithValue = valuesSource.docsWithValue(ctx);
        } else {
            docsWithValue = new DocValueBits() {
                @Override
                public boolean advanceExact(int doc) throws IOException {
                    return false;
                }
            };
        }
        return new LeafBucketCollectorBase(sub, docsWithValue) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (docsWithValue.advanceExact(doc) == false) {
                    collectBucket(sub, doc, bucket);
                }
            }
        };
    }

    @Override
    protected boolean tryPrecomputeAggregationForLeaf(LeafReaderContext ctx) throws IOException {
        if (subAggregators.length > 0) {
            // The optimization does not work when there are subaggregations or if there is a filter.
            // The query has to be a match all, otherwise
            //
            return false;
        }

        if (valuesSourceConfig.missing() != null) {
            // we do not collect any documents through the missing aggregation when the missing parameter
            // is up.
            return true;
        }

        if (fieldName == null) {
            // The optimization does not work when there are subaggregations or if there is a filter.
            // The query has to be a match all, otherwise
            //
            return false;
        }

        // The optimization could only be used if there are no deleted documents and the top-level
        // query matches all documents in the segment.
        if (weight == null) {
            return false;
        } else {
            if (weight.count(ctx) == 0) {
                return true;
            } else if (weight.count(ctx) != ctx.reader().maxDoc()) {
                return false;
            }
        }

        Set<String> indexedFields = new HashSet<>(FieldInfos.getIndexedFields(ctx.reader()));

        // This will only work if the field name is indexed because otherwise, the reader would not
        // have kept track of the doc count of the fieldname. There is a case where a field might be nonexistent
        // but still can be calculated.
        if (indexedFields.contains(fieldName) == false && ctx.reader().getFieldInfos().fieldInfo(fieldName) != null) {
            return false;
        }

        NumericDocValues docCountValues = DocValues.getNumeric(ctx.reader(), DocCountFieldMapper.NAME);
        if (docCountValues.nextDoc() != NO_MORE_DOCS) {
            // This segment has at least one document with the _doc_count field.
            return false;
        }

        long docCountWithFieldName = ctx.reader().getDocCount(fieldName);
        int totalDocCount = ctx.reader().maxDoc();

        // The missing aggregation bucket will count the number of documents where the field name is
        // either null or not present in that document. We are subtracting the documents where the field
        // value is valid.
        incrementBucketDocCount(0, totalDocCount - docCountWithFieldName);

        return true;
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForSingleBucket(
            owningBucketOrds,
            (owningBucketOrd, subAggregationResults) -> new InternalMissing(
                name,
                bucketDocCount(owningBucketOrd),
                subAggregationResults,
                metadata()
            )
        );
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalMissing(name, 0, buildEmptySubAggregations(), metadata());
    }

}
