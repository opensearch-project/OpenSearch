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
import org.opensearch.dsl.aggregation.AggregationTranslator;
import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Builds a {@link StringTerms} response. Handles keyword, ip, text, and any field type whose
 * bucket keys are naturally string-representable; also the fallback for unmapped types.
 *
 * <p>Keys render through the resolved {@link DocValueFormat}: for ip fields the mapping's
 * format produces the address string; for keywords it is the identity. Binary keys arriving
 * without a mapping-resolved format fall back to address-string rendering.
 */
public final class StringTermsStrategy implements TermsResponseStrategy {

    /** Singleton instance. */
    public static final StringTermsStrategy INSTANCE = new StringTermsStrategy();

    private StringTermsStrategy() {}

    @Override
    public InternalAggregation build(TermsAggregationBuilder agg, List<BucketEntry> entries, long otherDocCount, DocValueFormat format) {
        List<StringTerms.Bucket> termBuckets = new ArrayList<>(entries.size());
        for (BucketEntry entry : entries) {
            BytesRef term = new BytesRef(formatKey(entry.keys().get(0), format));
            termBuckets.add(new StringTerms.Bucket(term, entry.docCount(), entry.subAggs(), false, 0, format));
        }
        BucketOrder order = agg.order();
        return new StringTerms(
            agg.getName(),
            order,
            order,
            AggregationTranslator.userMetadata(agg),
            format,
            agg.shardSize(),
            false,
            otherDocCount,
            termBuckets,
            0,
            TermsBucketTranslator.thresholds(agg)
        );
    }

    /**
     * Renders a key through the resolved {@link DocValueFormat}. Binary keys (ip columns)
     * format through the mapping when one was resolved; with the RAW fallback they render as
     * the address string directly, or Base64 when not a 4/16-byte address.
     */
    private static String formatKey(Object key, DocValueFormat format) {
        if (key instanceof BytesRef ref) {
            return format == DocValueFormat.RAW ? binaryKeyString(ref.bytes) : format.format(ref).toString();
        }
        if (key instanceof byte[] bytes) {
            return format == DocValueFormat.RAW ? binaryKeyString(bytes) : format.format(new BytesRef(bytes)).toString();
        }
        return key.toString();
    }

    /** Binary keys are ip columns: render the address string like classic ip terms. */
    private static String binaryKeyString(byte[] bytes) {
        try {
            return NetworkAddress.format(InetAddress.getByAddress(bytes));
        } catch (UnknownHostException e) {
            // Not a 4/16-byte address; fall back to a printable, deterministic form.
            return Base64.getEncoder().encodeToString(bytes);
        }
    }
}
