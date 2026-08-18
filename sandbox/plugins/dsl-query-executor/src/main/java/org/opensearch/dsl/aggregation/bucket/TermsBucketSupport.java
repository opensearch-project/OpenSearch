/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.apache.lucene.util.BytesRef;
import org.opensearch.common.network.NetworkAddress;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/** Shared bucket post-processing for the terms family (terms, multi_terms). */
final class TermsBucketSupport {

    private TermsBucketSupport() {}

    /** The visible buckets after truncation, and the truncated tail's summed doc count. */
    record Truncated<B extends MultiBucketsAggregation.Bucket>(List<B> buckets, long otherDocCount) {
    }

    /** A Calcite column value paired with the format that renders it. */
    record TypedTerm(Object value, DocValueFormat format) {
    }

    /** Sorts {@code buckets} by the requested order and truncates to {@code size}. */
    static <B extends MultiBucketsAggregation.Bucket> Truncated<B> sortAndTruncate(List<B> buckets, BucketOrder order, int size) {
        buckets.sort(order.comparator());
        long otherDocCount = 0;
        List<B> visible = buckets;
        if (buckets.size() > size) {
            for (int i = size; i < buckets.size(); i++) {
                otherDocCount += buckets.get(i).getDocCount();
            }
            visible = new ArrayList<>(buckets.subList(0, size));
        }
        return new Truncated<>(visible, otherDocCount);
    }

    /** Renders a binary key as an IP address string, or Base64 if the bytes are not a valid address. */
    static String keyString(Object key) {
        if (key instanceof byte[] bytes) {
            try {
                return NetworkAddress.format(InetAddress.getByAddress(bytes));
            } catch (UnknownHostException e) {
                // Not a 4/16-byte address; fall back to a printable, deterministic form.
                return Base64.getEncoder().encodeToString(bytes);
            }
        }
        return key.toString();
    }

    /**
     * Classifies a column value into a raw typed value and format. Values stay raw (not display strings)
     * because {@code _key} ordering compares them via {@link Comparable}.
     */
    static TypedTerm typedTerm(Object key) {
        if (key instanceof Boolean bool) {
            return new TypedTerm(bool ? 1L : 0L, DocValueFormat.BOOLEAN);
        }
        if (key instanceof Double || key instanceof Float) {
            return new TypedTerm(((Number) key).doubleValue(), DocValueFormat.RAW);
        }
        if (key instanceof Number number) {
            return new TypedTerm(number.longValue(), DocValueFormat.RAW);
        }
        return new TypedTerm(new BytesRef(keyString(key)), DocValueFormat.RAW);
    }
}
