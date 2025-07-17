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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BytesRefHash;
import org.opensearch.common.util.SetBackedScalingCuckooFilter;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

/**
 * An aggregator that finds "rare" string values (e.g. terms agg that orders ascending)
 *
 * @opensearch.internal
 */
public class StringRareTermsAggregator extends AbstractRareTermsAggregator {
    private final ValuesSource.Bytes valuesSource;
    private final IncludeExclude.StringFilter filter;
    private final BytesKeyedBucketOrds bucketOrds;

    StringRareTermsAggregator(
        String name,
        AggregatorFactories factories,
        ValuesSource.Bytes valuesSource,
        DocValueFormat format,
        IncludeExclude.StringFilter filter,
        SearchContext context,
        Aggregator parent,
        Map<String, Object> metadata,
        long maxDocCount,
        double precision,
        CardinalityUpperBound cardinality
    ) throws IOException {
        super(name, factories, context, parent, metadata, maxDocCount, precision, format);
        this.valuesSource = valuesSource;
        this.filter = filter;
        this.bucketOrds = BytesKeyedBucketOrds.build(context.bigArrays(), cardinality);
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        final SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            final BytesRefBuilder previous = new BytesRefBuilder();

            @Override
            public void collect(int docId, long owningBucketOrd) throws IOException {
                if (false == values.advanceExact(docId)) {
                    return;
                }
                int valuesCount = values.docValueCount();
                previous.clear();

                // SortedBinaryDocValues don't guarantee uniqueness so we
                // need to take care of dups
                for (int i = 0; i < valuesCount; ++i) {
                    BytesRef bytes = values.nextValue();
                    if (filter != null && false == filter.accept(bytes)) {
                        continue;
                    }
                    if (i > 0 && previous.get().equals(bytes)) {
                        continue;
                    }
                    previous.copyBytes(bytes);
                    long bucketOrdinal = bucketOrds.add(owningBucketOrd, bytes);
                    if (bucketOrdinal < 0) { // already seen
                        bucketOrdinal = -1 - bucketOrdinal;
                        collectExistingBucket(sub, docId, bucketOrdinal);
                    } else {
                        collectBucket(sub, docId, bucketOrdinal);
                    }
                }
            }
        };
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        /*
         * Collect the list of buckets, populate the filter with terms
         * that are too frequent, and figure out how to merge sub-buckets.
         */
        StringRareTerms.Bucket[][] rarestPerOrd = new StringRareTerms.Bucket[owningBucketOrds.length][];
        SetBackedScalingCuckooFilter[] filters = new SetBackedScalingCuckooFilter[owningBucketOrds.length];
        long keepCount = 0;
        long[] mergeMap = new long[(int) bucketOrds.size()];
        Arrays.fill(mergeMap, -1);
        long offset = 0;
        for (int owningOrdIdx = 0; owningOrdIdx < owningBucketOrds.length; owningOrdIdx++) {
            try (BytesRefHash bucketsInThisOwningBucketToCollect = new BytesRefHash(context.bigArrays())) {
                checkCancelled();
                filters[owningOrdIdx] = newFilter();
                List<StringRareTerms.Bucket> builtBuckets = new ArrayList<>();
                BytesKeyedBucketOrds.BucketOrdsEnum collectedBuckets = bucketOrds.ordsEnum(owningBucketOrds[owningOrdIdx]);
                BytesRef scratch = new BytesRef();
                while (collectedBuckets.next()) {
                    collectedBuckets.readValue(scratch);
                    long docCount = bucketDocCount(collectedBuckets.ord());
                    // if the key is below threshold, reinsert into the new ords
                    if (docCount <= maxDocCount) {
                        StringRareTerms.Bucket bucket = new StringRareTerms.Bucket(BytesRef.deepCopyOf(scratch), docCount, null, format);
                        bucket.bucketOrd = offset + bucketsInThisOwningBucketToCollect.add(scratch);
                        mergeMap[(int) collectedBuckets.ord()] = bucket.bucketOrd;
                        builtBuckets.add(bucket);
                        keepCount++;
                    } else {
                        filters[owningOrdIdx].add(scratch);
                    }
                }
                rarestPerOrd[owningOrdIdx] = builtBuckets.toArray(new StringRareTerms.Bucket[0]);
                offset += bucketsInThisOwningBucketToCollect.size();
            }
        }

        /*
         * Only merge/delete the ordinals if we have actually deleted one,
         * to save on some redundant work.
         */
        if (keepCount != mergeMap.length) {
            mergeBuckets(mergeMap, offset);
            if (deferringCollector != null) {
                deferringCollector.mergeBuckets(mergeMap);
            }
        }

        /*
         * Now build the results!
         */
        buildSubAggsForAllBuckets(rarestPerOrd, b -> b.bucketOrd, (b, aggs) -> b.aggregations = aggs);
        InternalAggregation[] result = new InternalAggregation[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            Arrays.sort(rarestPerOrd[ordIdx], ORDER.comparator());
            result[ordIdx] = new StringRareTerms(
                name,
                ORDER,
                metadata(),
                format,
                Arrays.asList(rarestPerOrd[ordIdx]),
                maxDocCount,
                filters[ordIdx]
            );
        }
        return result;
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new StringRareTerms(name, LongRareTermsAggregator.ORDER, metadata(), format, emptyList(), 0, newFilter());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }
}
