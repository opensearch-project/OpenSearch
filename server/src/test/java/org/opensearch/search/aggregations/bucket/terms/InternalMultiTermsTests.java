/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.ParsedMultiBucketAggregation;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValuesSourceType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

public class InternalMultiTermsTests extends InternalTermsTestCase {

    /**
     * terms count and type should consistent across entire test.
     */
    private final List<ValuesSourceType> types = getSupportedValuesSourceTypes();

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
        long otherDocCount = 0;
        TermsAggregator.BucketCountThresholds bucketCountThresholds = new TermsAggregator.BucketCountThresholds(
            minDocCount,
            0,
            requiredSize,
            shardSize
        );

        final int numBuckets = randomNumberOfBuckets();

        List<InternalMultiTerms.Bucket> buckets = new ArrayList<>();
        List<DocValueFormat> formats = types.stream().map(type -> type.getFormatter(null, null)).collect(Collectors.toList());

        for (int i = 0; i < numBuckets; i++) {
            buckets.add(
                new InternalMultiTerms.Bucket(
                    types.stream().map(this::value).collect(Collectors.toList()),
                    minDocCount,
                    aggregations,
                    showTermDocCountError,
                    docCountError,
                    formats
                )
            );
        }
        BucketOrder reduceOrder = rarely() ? order : BucketOrder.key(true);
        // mimic per-shard bucket sort operation, which is required by bucket reduce phase.
        Collections.sort(buckets, reduceOrder.comparator());
        return new InternalMultiTerms(
            name,
            reduceOrder,
            order,
            metadata,
            shardSize,
            showTermDocCountError,
            otherDocCount,
            docCountError,
            formats,
            buckets,
            bucketCountThresholds
        );
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedMultiTerms.class;
    }

    private static List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return Collections.unmodifiableList(
            asList(
                CoreValuesSourceType.NUMERIC,
                CoreValuesSourceType.BYTES,
                CoreValuesSourceType.IP,
                CoreValuesSourceType.DATE,
                CoreValuesSourceType.BOOLEAN
            )
        );
    }

    private Object value(ValuesSourceType type) {
        if (CoreValuesSourceType.NUMERIC.equals(type)) {
            return randomInt();
        } else if (CoreValuesSourceType.DATE.equals(type)) {
            return randomNonNegativeLong();
        } else if (CoreValuesSourceType.BOOLEAN.equals(type)) {
            return randomBoolean();
        } else if (CoreValuesSourceType.BYTES.equals(type)) {
            return new BytesRef(randomAlphaOfLength(10));
        } else if (CoreValuesSourceType.IP.equals(type)) {
            return new BytesRef(InetAddressPoint.encode(randomIp(randomBoolean())));
        }
        throw new IllegalArgumentException("unexpected type [" + type.typeName() + "]");
    }
}
