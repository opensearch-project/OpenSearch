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
 * bucket keys are naturally string-representable; also the default for unregistered type names.
 *
 * <p>Bucket terms follow the classic StringTerms contract: binary keys (ip columns) are stored
 * as their encoded bytes and the mapping's {@link DocValueFormat} renders them at serialization;
 * string keys are stored as their UTF-8 bytes. A binary key without a mapping-resolved format
 * (RAW) renders as a deterministic Base64 string.
 */
public final class StringTermsStrategy implements TermsResponseStrategy {

    /** Singleton instance. */
    public static final StringTermsStrategy INSTANCE = new StringTermsStrategy();

    private StringTermsStrategy() {}

    @Override
    public InternalAggregation build(TermsAggregationBuilder agg, List<BucketEntry> entries, long otherDocCount, DocValueFormat format) {
        List<StringTerms.Bucket> termBuckets = new ArrayList<>(entries.size());
        for (BucketEntry entry : entries) {
            BytesRef term = termBytes(entry.keys().get(0), format);
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
     * Converts a key into the bucket's term bytes. Binary keys (ip columns) keep their encoded
     * bytes so the mapping-resolved {@link DocValueFormat} renders them at serialization — the
     * classic StringTerms contract; under RAW they are pre-rendered (address string or Base64)
     * since RAW would otherwise print raw bytes. String keys become their UTF-8 bytes.
     */
    private static BytesRef termBytes(Object key, DocValueFormat format) {
        if (key instanceof BytesRef ref) {
            return format == DocValueFormat.RAW ? new BytesRef(binaryKeyString(ref.bytes)) : ref;
        }
        if (key instanceof byte[] bytes) {
            return format == DocValueFormat.RAW ? new BytesRef(binaryKeyString(bytes)) : new BytesRef(bytes);
        }
        return new BytesRef(key.toString());
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
