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

import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.ParsedMultiBucketAggregation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DoubleTermsTests extends InternalTermsTestCase {

    @Override
    protected InternalTerms<?, ?> createTestInstance(
        String name,
        Map<String, Object> metadata,
        InternalAggregations aggregations,
        boolean showTermDocCountError,
        long docCountError
    ) {
        BucketOrder order = BucketOrder.count(false);
        long minDocCount = 1;
        int requiredSize = 3;
        int shardSize = requiredSize + 2;
        TermsAggregator.BucketCountThresholds bucketCountThresholds = new TermsAggregator.BucketCountThresholds(
            minDocCount,
            0,
            requiredSize,
            shardSize
        );
        DocValueFormat format = randomNumericDocValueFormat();
        long otherDocCount = 0;
        List<DoubleTerms.Bucket> buckets = new ArrayList<>();
        final int numBuckets = randomNumberOfBuckets();
        Set<Double> terms = new HashSet<>();
        for (int i = 0; i < numBuckets; ++i) {
            double term = randomValueOtherThanMany(d -> terms.add(d) == false, random()::nextDouble);
            int docCount = randomIntBetween(1, 100);
            buckets.add(new DoubleTerms.Bucket(term, docCount, aggregations, showTermDocCountError, docCountError, format));
        }
        BucketOrder reduceOrder = rarely() ? order : BucketOrder.key(true);
        Collections.sort(buckets, reduceOrder.comparator());
        return new DoubleTerms(
            name,
            reduceOrder,
            order,
            metadata,
            format,
            shardSize,
            showTermDocCountError,
            otherDocCount,
            buckets,
            docCountError,
            bucketCountThresholds
        );
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedDoubleTerms.class;
    }

    @Override
    protected InternalTerms<?, ?> mutateInstance(InternalTerms<?, ?> instance) {
        if (instance instanceof DoubleTerms) {
            DoubleTerms doubleTerms = (DoubleTerms) instance;
            String name = doubleTerms.getName();
            BucketOrder order = doubleTerms.order;
            int requiredSize = doubleTerms.requiredSize;
            long minDocCount = doubleTerms.minDocCount;
            DocValueFormat format = doubleTerms.format;
            int shardSize = doubleTerms.getShardSize();
            boolean showTermDocCountError = doubleTerms.showTermDocCountError;
            long otherDocCount = doubleTerms.getSumOfOtherDocCounts();
            List<DoubleTerms.Bucket> buckets = doubleTerms.getBuckets();
            long docCountError = doubleTerms.getDocCountError();
            Map<String, Object> metadata = doubleTerms.getMetadata();
            switch (between(0, 8)) {
                case 0:
                    name += randomAlphaOfLength(5);
                    break;
                case 1:
                    requiredSize += between(1, 100);
                    break;
                case 2:
                    minDocCount += between(1, 100);
                    break;
                case 3:
                    shardSize += between(1, 100);
                    break;
                case 4:
                    showTermDocCountError = showTermDocCountError == false;
                    break;
                case 5:
                    otherDocCount += between(1, 100);
                    break;
                case 6:
                    docCountError += between(1, 100);
                    break;
                case 7:
                    buckets = new ArrayList<>(buckets);
                    buckets.add(
                        new DoubleTerms.Bucket(
                            randomDouble(),
                            randomNonNegativeLong(),
                            InternalAggregations.EMPTY,
                            showTermDocCountError,
                            docCountError,
                            format
                        )
                    );
                    break;
                case 8:
                    if (metadata == null) {
                        metadata = new HashMap<>(1);
                    } else {
                        metadata = new HashMap<>(instance.getMetadata());
                    }
                    metadata.put(randomAlphaOfLength(15), randomInt());
                    break;
                default:
                    throw new AssertionError("Illegal randomisation branch");
            }
            Collections.sort(buckets, doubleTerms.reduceOrder.comparator());
            return new DoubleTerms(
                name,
                doubleTerms.reduceOrder,
                order,
                metadata,
                format,
                shardSize,
                showTermDocCountError,
                otherDocCount,
                buckets,
                docCountError,
                new TermsAggregator.BucketCountThresholds(minDocCount, 0, requiredSize, shardSize)
            );
        } else {
            String name = instance.getName();
            BucketOrder order = instance.order;
            int requiredSize = instance.requiredSize;
            long minDocCount = instance.minDocCount;
            Map<String, Object> metadata = instance.getMetadata();
            switch (between(0, 3)) {
                case 0:
                    name += randomAlphaOfLength(5);
                    break;
                case 1:
                    requiredSize += between(1, 100);
                    break;
                case 2:
                    minDocCount += between(1, 100);
                    break;
                case 3:
                    if (metadata == null) {
                        metadata = new HashMap<>(1);
                    } else {
                        metadata = new HashMap<>(instance.getMetadata());
                    }
                    metadata.put(randomAlphaOfLength(15), randomInt());
                    break;
                default:
                    throw new AssertionError("Illegal randomisation branch");
            }
            return new UnmappedTerms(name, order, new TermsAggregator.BucketCountThresholds(minDocCount, 0, requiredSize, 0), metadata);
        }
    }

}
