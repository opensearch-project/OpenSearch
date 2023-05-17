/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.ParsedMultiBucketAggregation;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UnsignedLongTermsTests extends InternalTermsTestCase {

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
        DocValueFormat format = randomNumericDocValueFormat();
        long otherDocCount = 0;
        List<UnsignedLongTerms.Bucket> buckets = new ArrayList<>();
        final int numBuckets = randomNumberOfBuckets();
        Set<BigInteger> terms = new HashSet<>();
        for (int i = 0; i < numBuckets; ++i) {
            BigInteger term = randomValueOtherThanMany(l -> terms.add(l) == false, InternalTermsTestCase::randomUnsignedLong);
            int docCount = randomIntBetween(1, 100);
            buckets.add(new UnsignedLongTerms.Bucket(term, docCount, aggregations, showTermDocCountError, docCountError, format));
        }
        BucketOrder reduceOrder = rarely() ? order : BucketOrder.key(true);
        Collections.sort(buckets, reduceOrder.comparator());
        return new UnsignedLongTerms(
            name,
            reduceOrder,
            order,
            requiredSize,
            minDocCount,
            metadata,
            format,
            shardSize,
            showTermDocCountError,
            otherDocCount,
            buckets,
            docCountError
        );
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedUnsignedLongTerms.class;
    }

    @Override
    protected InternalTerms<?, ?> mutateInstance(InternalTerms<?, ?> instance) {
        if (instance instanceof UnsignedLongTerms) {
            UnsignedLongTerms longTerms = (UnsignedLongTerms) instance;
            String name = longTerms.getName();
            BucketOrder order = longTerms.order;
            int requiredSize = longTerms.requiredSize;
            long minDocCount = longTerms.minDocCount;
            DocValueFormat format = longTerms.format;
            int shardSize = longTerms.getShardSize();
            boolean showTermDocCountError = longTerms.showTermDocCountError;
            long otherDocCount = longTerms.getSumOfOtherDocCounts();
            List<UnsignedLongTerms.Bucket> buckets = longTerms.getBuckets();
            long docCountError = longTerms.getDocCountError();
            Map<String, Object> metadata = longTerms.getMetadata();
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
                        new UnsignedLongTerms.Bucket(
                            randomUnsignedLong(),
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
            Collections.sort(buckets, longTerms.reduceOrder.comparator());
            return new UnsignedLongTerms(
                name,
                longTerms.reduceOrder,
                order,
                requiredSize,
                minDocCount,
                metadata,
                format,
                shardSize,
                showTermDocCountError,
                otherDocCount,
                buckets,
                docCountError
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
            return new UnmappedTerms(name, order, requiredSize, minDocCount, metadata);
        }
    }
}
